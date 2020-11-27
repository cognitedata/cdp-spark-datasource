package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.{
  confidenceRangeFromMinAndMax,
  getLabelsFilter,
  idsFromWrappedArray,
  pushdownToParameters,
  shouldGetAll,
  stringSeqFromWrappedArray,
  timeRangeFromMinAndMax,
  toPushdownFilterExpression
}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class RelationshipsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Relationship, String](config, "relationships")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def schema: StructType = structType[Relationship]

  override def toRow(a: Relationship): Row = asRow(a)

  override def uniqueId(a: Relationship): String = a.externalId

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, Relationship]] = {
    val fieldNames =
      Array(
        "sourceExternalId",
        "sourceType",
        "subtype",
        "targetExternalId",
        "targetType",
        "minStartTime",
        "maxStartTime",
        "minEndTime",
        "maxEndTime",
        "minActiveAtTime",
        "maxActiveAtTime",
        "minConfidence",
        "maxConfidence",
        "dataSetId",
        "labels",
        "minCreatedTime",
        "maxCreatedTime",
        "minLastUpdatedTime",
        "maxLastUpdatedTime"
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
      sourceExternalIds = m.get("sourceExternalIds").map(stringSeqFromWrappedArray),
      sourceTypes = m.get("sourceTypes").map(stringSeqFromWrappedArray),
      targetExternalIds = m.get("targetExternalIds").map(stringSeqFromWrappedArray),
      targetTypes = m.get("targetTypes").map(stringSeqFromWrappedArray),
      dataSetIds = m.get("dataSetId").map(idsFromWrappedArray(_).map(CogniteInternalId)),
      startTime = timeRangeFromMinAndMax(m.get("minStartTime"), m.get("maxStartTime")),
      endTime = timeRangeFromMinAndMax(m.get("minEndTime"), m.get("maxEndTime")),
      activeAtTime = timeRangeFromMinAndMax(m.get("minActiveAtTime"), m.get("maxActiveAtTime")),
      labels = m.get("labels").map(stringSeqFromWrappedArray),
      confidence = confidenceRangeFromMinAndMax(m.get("minConfidence"), m.get("maxConfidence")),
      createdTime = timeRangeFromMinAndMax(m.get("minCreatedTime"), m.get("maxCreatedTime")),
      lastUpdatedTime = timeRangeFromMinAndMax(m.get("minLastUpdatedTime"), m.get("maxLastUpdatedTime"))
    )

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
    labels: Option[Seq[String]] = None,
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
    labels: Option[Seq[String]] = None,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    dataSetId: Option[Long] = None
)
