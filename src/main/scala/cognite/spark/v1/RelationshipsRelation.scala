package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.{Create, DeleteByIdsWithIgnoreUnknownIds, UpdateByExternalId, UpdateById, WithExternalId, WithId, WithSetExternalId}
import com.cognite.sdk.scala.v1.resources.Relationship
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class RelationshipsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Relationship, Long](config, "relationships")
    with InsertableRelation {
  import CdpConnector._
  override def getStreams(filters: Array[Filter])(
    client: GenericClient[IO],
    limit: Option[Int],
    numPartitions: Int): Seq[Stream[IO, Relationship]] = {
    val fieldNames =
      Array(
        "sourceExternalId",
        "sourceType",
        "targetExternalId",
        "targetType",
        "minStartTime",
        "maxStartTime",
        "minEndTime",
        "maxEndTime",
        "confidence",
        "minCreatedTime",
        "maxCreatedTime",
        "minLastUpdatedTime",
        "maxLastUpdatedTime",
        "dataSetId",
        "labels"
      )
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val shouldGetAllRows = shouldGetAll(pushdownFilterExpression, fieldNames)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val relationshipsFilterSeq = if (filtersAsMaps.isEmpty || shouldGetAllRows) {
      Seq(RelationshipsFilter())
    } else {
      filtersAsMaps.distinct.map(relationshipsFilterFromMap)
    }

    val streamsPerFilter = relationshipsFilterSeq
      .map { f =>
        client.relationships.filterPartitions(f, numPartitions, limit)
      }

    // Merge streams related to each partition to make sure duplicate values are read into
    // the same RDD partition
    streamsPerFilter.transpose
      .map(s => s.reduce(_.merge(_)))
  }
  def relationshipsFilterFromMap(m: Map[String, String]): RelationshipsFilter =
    RelationshipsFilter(
      sourceExternalId = m.get("sourceExternalId"),
      sourceType = m.get("sourceType"),
      targetExternalId = m.get("targetExternalId"),
      targetType = m.get("targetType"),
      startTime = timeRangeFromMinAndMax(m.get("minStartTime"), m.get("maxStartTime")),
      endTime = timeRangeFromMinAndMax(m.get("minEndTime"), m.get("maxEndTime")),
      createdTime = timeRangeFromMinAndMax(m.get("minCreatedTime"), m.get("maxCreatedTime")),
      lastUpdatedTime = timeRangeFromMinAndMax(m.get("minLastUpdatedTime"), m.get("maxLastUpdatedTime")),
      dataSetIds = m.get("dataSetId").map(assetIdsFromWrappedArray(_).map(CogniteInternalId)), // ???????
      labels = m.get("labels") // ???????
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val relationships = rows.map(fromRow[RelationshipCreate](_))
    client.relationships
      .create(relationships)
      .flatTap(_ => incMetrics(itemsCreated, relationships.size)) *> IO.unit
  }
  override def schema: StructType = structType[Relationship]

  override def toRow(a: Relationship): Row = asRow(a)

  override def uniqueId(a: Relationship): Long = a.id
}
object RelationshipsRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[RelationshipsUpsertSchema]
  val insertSchema: StructType = structType[RelationshipsInsertSchema]
  val readSchema: StructType = structType[RelationshipsReadSchema]
}

final case class RelationshipsUpsertSchema(
   id: Long = 0,
   externalId: Option[String] = None,
   sourceExternalId: String,
   sourceType: String,
   targetExternalId: String,
   targetType: String,
   startTime: Option[Instant] = None,
   endTime: Option[Instant] = None,
   confidence: Float = 0,
   labels: Option[Seq[Map[String, String]]] = None,
   dataSetId: Option[Long] = None
 ) extends WithExternalId
  with WithId[Option[Long]]

final case class RelationshipsInsertSchema(
    externalId: Option[String] = None,
    sourceExternalId: String,
    sourceType: String,
    targetExternalId: String,
    targetType: String,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    confidence: Float = 0,
    labels: Option[Seq[Map[String, String]]] = None,
    dataSetId: Option[Long] = None
 )

final case class RelationshipsReadSchema(
   id: Long = 0,
   externalId: Option[String] = None,
   sourceExternalId: String,
   sourceType: String,
   targetExternalId: String,
   targetType: String,
   startTime: Option[Instant] = None,
   endTime: Option[Instant] = None,
   confidence: Float = 0,
   labels: Option[Seq[Map[String, String]]] = None,
   createdTime: Instant = Instant.ofEpochMilli(0),
   lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
   dataSetId: Option[Long] = None
 )
