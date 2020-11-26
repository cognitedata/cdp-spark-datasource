package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.v1._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class RelationshipsRelation(config: RelationConfig)(val sqlContext: SQLContext)
  extends SdkV1Relation[Relationship, String](config, "relationships")
    with InsertableRelation
    with WritableRelation {

  override def schema: StructType = structType[Relationship]

  override def toRow(a: Relationship): Row = asRow(a)

  override def uniqueId(a: Relationship): String = a.externalId

  override def getStreams(filters: Array[Filter])(
    client: GenericClient[IO],
    limit: Option[Int],
    numPartitions: Int): Seq[fs2.Stream[IO, Relationship]] =
    Seq(client.relationships.filter(RelationshipsFilter()))

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val relationships = rows.map(fromRow[RelationshipCreate](_))
    client.relationships
      .create(relationships)
      .flatTap(_ => incMetrics(itemsCreated, relationships.length)) *> IO.unit
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val relationshipIds = rows.map(fromRow[RelationshipsDeleteSchema](_)).map(_.externalId)
    client.relationships
      .deleteByExternalIds(relationshipIds)
      .flatTap(_ => incMetrics(itemsDeleted, relationshipIds.length))
  }

  override def upsert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Upsert is not supported for relationships")

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for relationships")
}

object RelationshipsRelation {
  var insertSchema: StructType = structType[RelationshipsInsertSchema]
  var readSchema: StructType = structType[RelationshipsReadSchema]
  var deleteSchema: StructType = structType[RelationshipsDeleteSchema]
}

final case class RelationshipsDeleteSchema(
    externalId: String
  )

final case class RelationshipsInsertSchema(
    externalId: Option[String] = None,
    sourceExternalId: String,
    sourceType: String,
    targetExternalId: String,
    targetType: String,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    confidence: Float = 0,
    labels: Option[Seq[CogniteExternalId]] = None,
    dataSetId: Option[Long] = None
 )

final case class RelationshipsReadSchema(
   externalId: Option[String] = None,
   sourceExternalId: String,
   sourceType: String,
   targetExternalId: String,
   targetType: String,
   startTime: Option[Instant] = None,
   endTime: Option[Instant] = None,
   confidence: Float = 0,
   labels: Option[Seq[CogniteExternalId]] = None,
   createdTime: Instant = Instant.ofEpochMilli(0),
   lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
   dataSetId: Option[Long] = None
 )
