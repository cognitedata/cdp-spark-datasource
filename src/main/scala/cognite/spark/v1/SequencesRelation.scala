package cognite.spark.v1

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import cognite.spark.compiletime.macros.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.{SetValue, Setter, WithExternalId, WithId}
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.SequencesResource
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant
import cognite.spark.v1.CdpConnector.ioRuntime

import scala.annotation.unused

class SequencesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1InsertableRelation[SequenceReadSchema, Long](config, "sequences")
    with WritableRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, SequenceReadSchema]] =
    // TODO: filters
    client.sequences
      .listPartitions(config.partitions)
      .map(_.map(_.into[SequenceReadSchema].withFieldComputed(_.columns, _.columns.toList).transform))

  /*
     Sequence API does not support more than 200 columns per sequence,
     and the maximum total columns per request is 10000
     so we need to split them to ensure that each chunk can not have more than 10000 columns in total
   */
  private def splitSequences[T](sequences: Seq[T], getSizeOfColumns: T => Int): Seq[Vector[T]] = {
    val (_, groupedSequences) = sequences
      .foldLeft((0, List(Vector.empty[T]))) {
        case ((_, Nil), sequence) =>
          (getSizeOfColumns(sequence), List(Vector(sequence)))
        case ((currentGroupColumnsCount, groups @ currentGroup :: otherGroups), sequence) =>
          val columnsInSequence = getSizeOfColumns(sequence)
          if (currentGroupColumnsCount + columnsInSequence > Constants.DefaultSequencesTotalColumnsLimit) {
            // create a new group
            (columnsInSequence, Vector(sequence) :: groups)
          } else {
            // add to current group
            (currentGroupColumnsCount + columnsInSequence, (sequence +: currentGroup) :: otherGroups)
          }
      }
    groupedSequences
  }

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val sequences = rows.map { row =>
      val s = fromRow[SequenceInsertSchema](row)
      SequenceCreate(
        s.name,
        s.description,
        s.assetId,
        s.externalId,
        s.metadata,
        NonEmptyList
          .fromList(s.columns.toList)
          .getOrElse(
            throw new CdfSparkIllegalArgumentException(
              s"columns must not be empty (on row ${SparkSchemaHelperRuntime.rowIdentifier(row)})")
          ),
        s.dataSetId
      )
    }

    val groupedSequences: Seq[Vector[SequenceCreate]] =
      splitSequences[SequenceCreate](sequences, (s: SequenceCreate) => s.columns.size)
    groupedSequences.toList.traverse_ { sequencesToCreate =>
      client.sequences
        .create(sequencesToCreate)
        .flatMap(_ => incMetrics(itemsCreated, sequencesToCreate.size))
    }
  }

  private def isUpdateEmpty(u: SequenceUpdate): Boolean = u == SequenceUpdate()

  private def getExistingSequenceColumnsByIds(
      sequenceUpdates: Seq[SequenceUpsertSchema]): Map[Option[Long], Seq[(String, String)]] = {
    val ids = sequenceUpdates.filter(_.id.nonEmpty).flatMap(_.id.toList)
    if (ids.nonEmpty) {
      client.sequences
        .retrieveByIds(ids, ignoreUnknownIds = true)
        .unsafeRunSync()
        .map(s => Some(s.id) -> s.columns.toList.map(c => (c.externalId.getOrElse(""), c.valueType)))
        .toMap
    } else {
      Map()
    }
  }

  private def getExistingSequenceColumnsByExternalIds(
      sequenceUpdates: Seq[SequenceUpsertSchema]): Map[Option[String], Seq[(String, String)]] = {
    val externalIds = sequenceUpdates
      .filter(s => s.id.isEmpty && s.externalId.toOption.nonEmpty)
      .flatMap(_.externalId.toOption.toList)
    if (externalIds.nonEmpty) {
      client.sequences
        .retrieveByExternalIds(externalIds, ignoreUnknownIds = true)
        .unsafeRunSync()
        .collect {
          case s if s.externalId.nonEmpty =>
            s.externalId -> s.columns.toList.map(c => (c.externalId.getOrElse(""), c.valueType))
        }
        .toMap
    } else {
      Map()
    }
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val sequenceUpdates = rows.map(r => fromRow[SequenceUpsertSchema](r))
    implicit val toUpdate = transformerUpsertToUpdate(sequenceUpdates)

    updateByIdOrExternalId[SequenceUpsertSchema, SequenceUpdate, SequencesResource[IO], Sequence](
      sequenceUpdates,
      client.sequences,
      isUpdateEmpty
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => fromRow[DeleteItem](r))
    val ids = deletes.map(_.id)
    client.sequences
      .deleteByIds(ids) // can't ignore unknown ids :(
      .flatTap(_ => incMetrics(itemsDeleted, ids.length))
  }

  private def transformerUpsertToCreate() =
    Transformer
      .define[SequenceUpsertSchema, SequenceCreate]
      .withFieldComputed(
        _.columns,
        x => x.getSequenceColumnCreate
      )
      .buildTransformer

  private def transformerUpsertToUpdate(sequences: Seq[SequenceUpsertSchema]) = {
    val existingSeqs: Map[Option[Long], Seq[(String, String)]] = getExistingSequenceColumnsByIds(
      sequences)
    val existingSeqsWithExtId: Map[Option[String], Seq[(String, String)]] =
      getExistingSequenceColumnsByExternalIds(sequences)
    Transformer
      .define[SequenceUpsertSchema, SequenceUpdate]
      .withFieldComputed(
        _.columns,
        x =>
          x.getSequenceColumnsUpdate(
            existingSeqs
              .getOrElse(x.id, existingSeqsWithExtId.getOrElse(x.externalId.toOption, Seq()))
              .toSet)
      )
      .buildTransformer
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val sequences = rows.map(fromRow[SequenceUpsertSchema](_))

    implicit val toCreate = transformerUpsertToCreate()

    implicit val toUpdate = transformerUpsertToUpdate(sequences)

    val groupedSequences: Seq[Vector[SequenceUpsertSchema]] = splitSequences[SequenceUpsertSchema](
      sequences,
      (s: SequenceUpsertSchema) => s.columns.map(_.size).getOrElse(0))

    // scalastyle:off no.whitespace.after.left.bracket
    groupedSequences.toList.traverse_ { sequencesToCreate =>
      genericUpsert[
        Sequence,
        SequenceUpsertSchema,
        SequenceCreate,
        SequenceUpdate,
        SequencesResource[IO]](sequencesToCreate, isUpdateEmpty, client.sequences)
    }
    // scalastyle:on no.whitespace.after.left.bracket
  }

  // scalastyle:off method.length
  override def getFromRowsAndCreate(rows: Seq[Row], @unused doUpsert: Boolean = true): IO[Unit] = {
    val sequences =
      rows
        .map(fromRow[SequenceUpsertSchema](_))

    implicit val toCreate = transformerUpsertToCreate()

    implicit val toUpdate = transformerUpsertToUpdate(sequences)

    // scalastyle:off no.whitespace.after.left.bracket
    createOrUpdateByExternalId[
      Sequence,
      SequenceUpdate,
      SequenceCreate,
      SequenceUpsertSchema,
      OptionalField,
      SequencesResource[IO]](Set.empty, sequences, client.sequences, doUpsert = true)
  }
  // scalastyle:off method.length

  override def schema: StructType = structType[SequenceReadSchema]()

  override def toRow(a: SequenceReadSchema): Row = asRow(a)

  override def uniqueId(a: SequenceReadSchema): Long = a.id
}

object SequenceRelation extends UpsertSchema {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  val upsertSchema: StructType = structType[SequenceUpsertSchema]()
  val insertSchema: StructType = structType[SequenceInsertSchema]()
  val readSchema: StructType = structType[SequenceReadSchema]()
}

final case class SequenceColumnUpsertSchema(
    externalId: String,
    name: OptionalField[String] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    valueType: Option[String] = None,
    metadata: Option[Map[String, String]] = None
) {
  def toColumnCreate: SequenceColumnCreate = SequenceColumnCreate(
    name = name.toOption,
    externalId = externalId,
    description = description.toOption,
    valueType = valueType.getOrElse("STRING"),
    metadata = metadata
  )
  def toColumnUpdate(implicit tr: Transformer[OptionalField[String], Option[Setter[String]]])
    : SequenceColumnModifyUpdate =
    SequenceColumnModifyUpdate(
      externalId = externalId,
      update = SequenceColumnModify(
        description = description.transformInto[Option[Setter[String]]],
        name = name.transformInto[Option[Setter[String]]],
        metadata = metadata.map(SetValue(_))
      )
    )
}

final case class SequenceUpsertSchema(
    id: Option[Long] = None,
    externalId: OptionalField[String] = FieldNotSpecified,
    name: OptionalField[String] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    assetId: OptionalField[Long] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    columns: Option[Seq[SequenceColumnUpsertSchema]] = None,
    dataSetId: OptionalField[Long] = FieldNotSpecified
) extends WithNullableExtenalId
    with WithId[Option[Long]] {

  def getSequenceColumnCreate: NonEmptyList[SequenceColumnCreate] = {
    val cols = cats.data.NonEmptyList
      .fromFoldable(
        columns
          .getOrElse(throw new CdfSparkIllegalArgumentException(
            s"columns is required when inserting sequences (on row $this)"))
          .toVector)
      .getOrElse(throw new CdfSparkIllegalArgumentException(s"columns must not be empty (on row $this)"))
    cols.map(_.toColumnCreate)
  }

  def getSequenceColumnsUpdate(existingColumns: Set[(String, String)])(
      implicit tr: Transformer[OptionalField[String], Option[Setter[String]]])
    : Option[SequenceColumnsUpdate] =
    // Helper for updates and upserts
    // SequenceColumnUpsertSchema type is used for column updates in SequenceColumnUpsertSchema,
    // This helper converts SequenceColumnUpsertSchema to SDK Column updates by detecting what to add, remove or modify
    columns match {
      // Not to break update backward compatibility,
      // upserting empty column list causes 422 (Deleting all columns of the sequence) so we skip columns in the update
      case None => None
      case Some(Seq()) => None
      // When value provided, we upsert columns in sequence update ->
      // we implement columns.set here ourselves using add+remove+modify.
      case Some(colUpsert) =>
        val requestedCols = colUpsert.map(col => col.externalId -> col).toMap
        val existingColumnExternalIds = existingColumns.map(_._1)
        val (modifyMap, addMap) =
          requestedCols.partition(item => existingColumnExternalIds contains item._1)

        // Column.modify do not allow valueType updates,
        // If user tries to change the valueType, we abort.
        existingColumns.foreach {
          case (externalId: String, valueType: String) =>
            val newValueType = modifyMap.get(externalId).flatMap(_.valueType)
            if (newValueType.nonEmpty && !newValueType.contains(valueType)) {
              throw new CdfSparkIllegalArgumentException(
                s"Column valueType cannot be modified: the previous value is $valueType and the user" +
                  s" attempted to update it with ${newValueType.getOrElse("null")}")
            }
        }

        val removeData =
          Option(existingColumnExternalIds.diff(requestedCols.keySet).toList.map(CogniteExternalId(_)))
            .filter(_.nonEmpty)

        val addData = Option(addMap.values.toList.map(_.toColumnCreate)).filter(_.nonEmpty)

        val modifyData = Option(modifyMap.map {
          case (_: String, createVal: SequenceColumnUpsertSchema) => createVal.toColumnUpdate
        }.toList).filter(_.nonEmpty)

        (addData, removeData, modifyData) match {
          case (None, None, None) => None
          case (add, remove, modify) =>
            Some(SequenceColumnsUpdate(add = add, remove = remove, modify = modify))
        }
    }
}

final case class SequenceInsertSchema(
    externalId: Option[String] = None,
    name: Option[String] = None,
    description: Option[String] = None,
    assetId: Option[Long] = None,
    metadata: Option[Map[String, String]] = None,
    columns: Seq[SequenceColumnCreate],
    dataSetId: Option[Long] = None
)

final case class SequenceReadSchema(
    id: Long = 0,
    name: Option[String] = None,
    description: Option[String] = None,
    assetId: Option[Long] = None,
    externalId: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    columns: Seq[SequenceColumn],
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    dataSetId: Option[Long] = None
) extends WithExternalId
    with WithId[Long]
