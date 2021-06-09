package cognite.spark.v1

import java.time.Instant
import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.{WithExternalId, WithId}
import com.cognite.sdk.scala.v1.resources.SequencesResource
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class SequencesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[SequenceReadSchema, Long](config, "sequences")
    with InsertableRelation
    with WritableRelation {
  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, SequenceReadSchema]] =
    // TODO: filters
    client.sequences
      .listPartitions(numPartitions)
      .map(_.map(_.into[SequenceReadSchema].withFieldComputed(_.columns, _.columns.toList).transform))

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

    // Sequence API does not support more than 200 columns per sequence,
    // and the maximum total columns per request is 10000
    // so grouped by 50 ensure that each chunk can not have more than 10000 columns in total,
    // otherwise we already reject because of the first condition
    val (_, groupedSequences) = sequences
      .foldLeft((0, List(Vector.empty[SequenceCreate]))) {
        case ((currentGroupColumnsCount, groups @ currentGroup :: otherGroups), sequence) =>
          val columnsInSequence = sequence.columns.size
          if (currentGroupColumnsCount + columnsInSequence > Constants.DefaultSequencesTotalColumnsLimit) {
            // create a new group
            (columnsInSequence, Vector(sequence) :: groups)
          } else {
            // add to current group
            (currentGroupColumnsCount + columnsInSequence, (sequence +: currentGroup) :: otherGroups)
          }
      }
    groupedSequences.map { sequencesToCreate =>
      client.sequences
        .create(sequencesToCreate)
        .flatMap(_ => incMetrics(itemsCreated, sequencesToCreate.size))
    }.sequence_

  }

  private def isUpdateEmpty(u: SequenceUpdate): Boolean = u == SequenceUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    val sequenceUpdates = rows.map(r => fromRow[SequenceUpdateSchema](r))
    updateByIdOrExternalId[SequenceUpdateSchema, SequenceUpdate, SequencesResource[IO], Sequence](
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

  override def upsert(rows: Seq[Row]): IO[Unit] =
    // Sequences fail with 400 error code when id already exists:
    // /api/v1/projects/jetfiretest2/sequences failed with status 400: The following external id(s) are already in the database:
    throw new CdfSparkException("Upsert not supported for sequences.")

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val sequences =
      rows
        .map(fromRow[SequenceUpdateSchema](_))

    implicit val toCreate =
      Transformer
        .define[SequenceUpdateSchema, SequenceCreate]
        .withFieldComputed(
          _.columns,
          x =>
            cats.data.NonEmptyList
              .fromFoldable(
                x.columns
                  .getOrElse(throw new CdfSparkIllegalArgumentException(
                    s"columns is required when inserting sequences (on row $x)"))
                  .toVector
              )
              .getOrElse(
                throw new CdfSparkIllegalArgumentException(s"columns must not be empty (on row $x)"))
        )
        .buildTransformer

    // scalastyle:off no.whitespace.after.left.bracket
    createOrUpdateByExternalId[
      Sequence,
      SequenceUpdate,
      SequenceCreate,
      SequenceUpdateSchema,
      OptionalField,
      SequencesResource[IO]](Set.empty, sequences, client.sequences, doUpsert = true)
  }

  override def schema: StructType = structType[SequenceReadSchema]

  override def toRow(a: SequenceReadSchema): Row = asRow(a)

  override def uniqueId(a: SequenceReadSchema): Long = a.id
}

object SequenceRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[SequenceUpdateSchema]
  val insertSchema: StructType = structType[SequenceInsertSchema]
  val readSchema: StructType = structType[SequenceReadSchema]
}

final case class SequenceUpdateSchema(
    id: Option[Long] = None,
    externalId: OptionalField[String] = FieldNotSpecified,
    name: OptionalField[String] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    assetId: OptionalField[Long] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    columns: Option[Seq[SequenceColumnCreate]] = None,
    dataSetId: OptionalField[Long] = FieldNotSpecified
) extends WithNullableExtenalId
    with WithId[Option[Long]]

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
