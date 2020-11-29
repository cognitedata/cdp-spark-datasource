package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.{
  confidenceRangeFromLimitStrings,
  getExternalIdSeq,
  idsFromWrappedArray,
  pushdownToParameters,
  shouldGetAll,
  stringToContainsAny,
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
    extends SdkV1Relation[RelationshipsReadSchema, String](config, "relationships")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def schema: StructType = structType[RelationshipsReadSchema]

  override def toRow(a: RelationshipsReadSchema): Row = asRow(a)

  override def uniqueId(a: RelationshipsReadSchema): String = a.externalId

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, RelationshipsReadSchema]] = {
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

    if (filtersAsMaps.isEmpty || shouldGetAllRows) {
      Seq(client.relationships.filter(RelationshipsFilter()).map(relationshipToRelationshipReadSchema))
    } else {
      val relationshipsFilterSeq = filtersAsMaps.distinct.map(relationshipsFilterFromMap)
      relationshipsFilterSeq
        .map { f =>
          client.relationships.filter(f, limit).map(relationshipToRelationshipReadSchema)
        }
    }

  }

  def relationshipsFilterFromMap(m: Map[String, String]): RelationshipsFilter =
    RelationshipsFilter(
      sourceExternalIds = getExternalIdSeq(m.get("sourceExternalId")),
      sourceTypes = getExternalIdSeq(m.get("sourceType")),
      targetExternalIds = getExternalIdSeq(m.get("targetExternalId")),
      targetTypes = getExternalIdSeq(m.get("targetType")),
      dataSetIds = m.get("dataSetId").map(idsFromWrappedArray(_).map(CogniteInternalId)),
      startTime = timeRangeFromMinAndMax(m.get("minStartTime"), m.get("maxStartTime")),
      endTime = timeRangeFromMinAndMax(m.get("minEndTime"), m.get("maxEndTime")),
      labels = m.get("labels").map(stringToContainsAny).getOrElse(None),
      confidence = confidenceRangeFromLimitStrings(m.get("minConfidence"), m.get("maxConfidence")),
      lastUpdatedTime = timeRangeFromMinAndMax(m.get("minLastUpdatedTime"), m.get("maxLastUpdatedTime")),
      createdTime = timeRangeFromMinAndMax(m.get("minCreatedTime"), m.get("maxCreatedTime"))
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val relationships =
      rows.map(fromRow[RelationshipsInsertSchema](_)).map(relationshipInsertSchemaToRelationshipCreate)
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

  def relationshipToRelationshipReadSchema(relationship: Relationship): RelationshipsReadSchema =
    RelationshipsReadSchema(
      externalId = relationship.externalId,
      sourceExternalId = relationship.sourceExternalId,
      sourceType = relationship.sourceType,
      targetExternalId = relationship.targetExternalId,
      targetType = relationship.targetType,
      startTime = relationship.startTime,
      endTime = relationship.endTime,
      confidence = relationship.confidence,
      labels = Option(relationship.labels.getOrElse(Seq()).map(_.externalId)),
      createdTime = relationship.createdTime,
      lastUpdatedTime = relationship.lastUpdatedTime,
      dataSetId = relationship.dataSetId
    )

  def relationshipInsertSchemaToRelationshipCreate(
      relationship: RelationshipsInsertSchema): RelationshipCreate =
    RelationshipCreate(
      externalId = relationship.externalId,
      sourceExternalId = relationship.sourceExternalId,
      sourceType = relationship.sourceType,
      targetExternalId = relationship.targetExternalId,
      targetType = relationship.targetType,
      startTime = relationship.startTime,
      endTime = relationship.endTime,
      confidence = relationship.confidence,
      labels = Option(relationship.labels.getOrElse(Seq()).map(CogniteExternalId)),
      dataSetId = relationship.dataSetId
    )

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
    externalId: String,
    sourceExternalId: String,
    sourceType: String,
    targetExternalId: String,
    targetType: String,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    confidence: Option[Double] = None,
    labels: Option[Seq[String]] = None,
    dataSetId: Option[Long] = None
)

final case class RelationshipsReadSchema(
    externalId: String,
    sourceExternalId: String,
    sourceType: String,
    targetExternalId: String,
    targetType: String,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    confidence: Option[Double] = None,
    labels: Option[Seq[String]] = None,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    dataSetId: Option[Long] = None
)
