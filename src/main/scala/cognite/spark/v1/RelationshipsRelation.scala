package cognite.spark.v1

import cats.Id
import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.compiletime.macros.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.WithRequiredExternalId
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.Relationships
import fs2.Stream
import io.scalaland.chimney.Transformer
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class RelationshipsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[RelationshipsReadSchema, String](config, "relationships")
    with WritableRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  override def schema: StructType = structType[RelationshipsReadSchema]()

  override def toRow(a: RelationshipsReadSchema): Row = asRow(a)

  override def uniqueId(a: RelationshipsReadSchema): String = a.externalId

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, RelationshipsReadSchema]] = {
    val (ids, filters) =
      pushdownToFilters(
        sparkFilters,
        f => relationshipsFilterFromMap(f.fieldValues),
        RelationshipsFilter())

    // TODO: support parallel retrival using partitions
    Seq(
      executeFilterOnePartition(client.relationships, filters, ids, config.limitPerPartition)
        .map(relationshipToRelationshipReadSchema))
  }

  def relationshipsFilterFromMap(m: Map[String, String]): RelationshipsFilter =
    RelationshipsFilter(
      sourceExternalIds = m.get("sourceExternalId").map(Seq(_)),
      sourceTypes = m.get("sourceType").map(Seq(_)),
      targetExternalIds = m.get("targetExternalId").map(Seq(_)),
      targetTypes = m.get("targetType").map(Seq(_)),
      dataSetIds = m.get("dataSetId").map(idsFromStringifiedArray(_).map(CogniteInternalId(_))),
      startTime = timeRange(m, "startTime"),
      endTime = timeRange(m, "endTime"),
      labels = m.get("labels").flatMap(externalIdsToContainsAny),
      confidence = confidenceRangeFromLimitStrings(
        minConfidence = m.get("minConfidence"),
        maxConfidence = m.get("maxConfidence")),
      lastUpdatedTime = timeRange(m, "lastUpdatedTime"),
      createdTime = timeRange(m, "createdTime")
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

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val relationships = rows.map(fromRow[RelationshipsUpsertSchema](_))
    createOrUpdateByExternalId[
      Relationship,
      RelationshipUpdate,
      RelationshipCreate,
      RelationshipsUpsertSchema,
      Id,
      Relationships[IO]](Set.empty, relationships, client.relationships, doUpsert = true)
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val relationshipsUpdates = rows.map(fromRow[RelationshipsUpsertSchema](_))
    createOrUpdateByExternalId[
      Relationship,
      RelationshipUpdate,
      RelationshipCreate,
      RelationshipsUpsertSchema,
      Id,
      Relationships[IO]](
      relationshipsUpdates.map(_.externalId).toSet,
      relationshipsUpdates,
      client.relationships,
      doUpsert = false)
  }

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
      labels = cogniteExternalIdSeqToStringSeq(relationship.labels),
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
      labels = stringSeqToCogniteExternalIdSeq(relationship.labels),
      dataSetId = relationship.dataSetId
    )

  def confidenceRangeFromLimitStrings(
      minConfidence: Option[String],
      maxConfidence: Option[String]): Option[ConfidenceRange] =
    (minConfidence, maxConfidence) match {
      case (None, None) => None
      case (_, None) => Some(ConfidenceRange(min = Some(minConfidence.get.toDouble), max = None))
      case _ => Some(ConfidenceRange(min = None, max = Some(maxConfidence.get.toDouble)))
    }
}

object RelationshipsRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  val insertSchema: StructType = structType[RelationshipsInsertSchema]()
  val readSchema: StructType = structType[RelationshipsReadSchema]()
  val deleteSchema: StructType = structType[RelationshipsDeleteSchema]()
  val upsertSchema: StructType = structType[RelationshipsUpsertSchema]()
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

final case class RelationshipsUpsertSchema(
    externalId: String,
    sourceExternalId: Option[String] = None,
    sourceType: Option[String] = None,
    targetExternalId: Option[String] = None,
    targetType: Option[String] = None,
    startTime: OptionalField[Instant] = FieldNotSpecified,
    endTime: OptionalField[Instant] = FieldNotSpecified,
    confidence: OptionalField[Double] = FieldNotSpecified,
    labels: Option[Seq[String]] = None,
    dataSetId: OptionalField[Long] = FieldNotSpecified
) extends WithRequiredExternalId

object RelationshipsUpsertSchema {
  implicit val toCreate: Transformer[RelationshipsUpsertSchema, RelationshipCreate] =
    Transformer
      .define[RelationshipsUpsertSchema, RelationshipCreate]
      .withFieldComputed(
        _.sourceExternalId,
        _.sourceExternalId.getOrElse(throw new CdfSparkIllegalArgumentException(
          "The sourceExternalId field must be set when creating relationships."))
      )
      .withFieldComputed(
        _.sourceType,
        _.sourceType.getOrElse(throw new CdfSparkIllegalArgumentException(
          "The sourceType field must be set when creating relationships.")))
      .withFieldComputed(
        _.targetExternalId,
        _.targetExternalId.getOrElse(throw new CdfSparkIllegalArgumentException(
          "The targetExternalId field must be set when creating relationships."))
      )
      .withFieldComputed(
        _.targetType,
        _.targetType.getOrElse(throw new CdfSparkIllegalArgumentException(
          "The targetType field must be set when creating relationships.")))
      .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
      .buildTransformer
}
