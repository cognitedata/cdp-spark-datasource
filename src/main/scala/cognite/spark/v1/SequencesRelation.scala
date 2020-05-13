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
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class SequencesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[SequenceReadSchema, Long](config, "sequences")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, SequenceReadSchema]] =
    // TODO: filters
    Seq(
      client.sequences
        .list()
        .map(_.into[SequenceReadSchema].withFieldComputed(_.columns, _.columns.toList).transform)
    )

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
            throw new IllegalArgumentException(
              s"columns must not be empty (on row ${SparkSchemaHelperRuntime.rowIdentifier(row)})")
          ),
        s.dataSetId
      )
    }
    client.sequences
      .create(sequences)
      .flatTap(_ => incMetrics(itemsCreated, sequences.size)) *> IO.unit
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
    throw new RuntimeException("Upsert not supported for sequences.")

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val sequences = rows.map(fromRow[SequenceCreate](_))

    createOrUpdateByExternalId[Sequence, SequenceUpdate, SequenceCreate, SequencesResource[IO]](
      Set.empty,
      sequences,
      client.sequences,
      doUpsert = true)
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
    externalId: Option[String] = None,
    name: Option[String] = None,
    description: Option[String] = None,
    assetId: Option[Long] = None,
    metadata: Option[Map[String, String]] = None,
    columns: Option[Seq[SequenceColumnCreate]] = None,
    dataSetId: Option[Long] = None
) extends WithExternalId
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
