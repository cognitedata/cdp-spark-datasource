package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import cognite.spark.v1.AssetsRelation.name
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.Assets
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant
import scala.annotation.unused

class AssetsRelation(config: RelationConfig, subtreeIds: Option[List[CogniteId]] = None)(
    val sqlContext: SQLContext)
    extends SdkV1InsertableRelation[AssetsReadSchema, Long](config, name)
    with WritableRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, AssetsReadSchema]] = {
    val (ids, filters) =
      pushdownToFilters(
        sparkFilters,
        f => assetsFilterFromMap(f.fieldValues),
        AssetsFilter(assetSubtreeIds = subtreeIds))
    executeFilter(client.assets, filters, ids, config.partitions, config.limitPerPartition)
      .map(
        _.map(
          _.into[AssetsReadSchema]
            .withFieldComputed(_.labels, asset => cogniteExternalIdSeqToStringSeq(asset.labels))
            .withFieldComputed(
              _.aggregates,
              asset =>
                asset.aggregates.map { aggregates =>
                  AssetsAggregatesSchema(
                    aggregates.get("childCount"),
                    None, // No support for path yet.
                    aggregates.get("depth")
                  )
              }
            )
            .transform))
  }

  private def assetsFilterFromMap(m: Map[String, String]): AssetsFilter =
    AssetsFilter(
      name = m.get("name"),
      source = m.get("source"),
      dataSetIds = m.get("dataSetId").map(idsFromStringifiedArray(_).map(CogniteInternalId(_))),
      labels = m.get("labels").flatMap(externalIdsToContainsAny),
      lastUpdatedTime = timeRange(m, "lastUpdatedTime"),
      createdTime = timeRange(m, "createdTime"),
      externalIdPrefix = m.get("externalIdPrefix"),
      assetSubtreeIds = subtreeIds
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val assetsInsertions = rows.map(fromRow[AssetsInsertSchema](_))
    val assets = assetsInsertions.map(
      _.into[AssetCreate]
        .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
        .transform)
    client.assets
      .create(assets)
      .flatTap(_ => incMetrics(itemsCreated, assets.size)) *> IO.unit
  }

  private def isUpdateEmpty(u: AssetUpdate): Boolean = u == AssetUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    val assetUpdates = rows.map(r => fromRow[AssetsUpsertSchema](r))

    updateByIdOrExternalId[AssetsUpsertSchema, AssetUpdate, Assets[IO], Asset](
      assetUpdates,
      client.assets,
      isUpdateEmpty
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(fromRow[DeleteItemByCogniteId](_))
    deleteWithIgnoreUnknownIds(client.assets, deletes.map(_.toCogniteId), config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val assets = rows.map(fromRow[AssetsUpsertSchema](_))
    genericUpsert[Asset, AssetsUpsertSchema, AssetCreate, AssetUpdate, Assets[IO]](
      assets,
      isUpdateEmpty,
      client.assets,
      mustBeUpdate = r => r.name.isEmpty && r.getExternalId.nonEmpty
    )
  }

  override def getFromRowsAndCreate(rows: Seq[Row], @unused doUpsert: Boolean = true): IO[Unit] = {
    val assetsUpserts = rows.map(fromRow[AssetsUpsertSchema](_))
    val assets = assetsUpserts.map(_.transformInto[AssetCreate])
    createOrUpdateByExternalId[Asset, AssetUpdate, AssetCreate, AssetCreate, Option, Assets[IO]](
      Set.empty,
      assets,
      client.assets,
      doUpsert = true)
  }

  override def schema: StructType = structType[AssetsReadSchema]()

  override def toRow(a: AssetsReadSchema): Row = asRow(a)

  override def uniqueId(a: AssetsReadSchema): Long = a.id
}

object AssetsRelation
    extends UpsertSchema
    with ReadSchema
    with NamedRelation
    with AbortSchema
    with DeleteWithIdSchema {
  override val name = "assets"
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  val upsertSchema: StructType = structType[AssetsUpsertSchema]()
  val abortSchema: StructType = structType[AssetsInsertSchema]()
  val readSchema: StructType = structType[AssetsReadSchema]()

}

final case class AssetsUpsertSchema(
    id: Option[Long] = None,
    name: Option[String] = None,
    description: OptionalField[String] = FieldNotSpecified,
    source: OptionalField[String] = FieldNotSpecified,
    externalId: OptionalField[String] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    // parentId and parentExternalId are not nullable (for now)
    // since only one of them may be non-null, we'd have to combine the information from both fields in a non-trivial way
    parentId: Option[Long] = None,
    parentExternalId: Option[String] = None,
    dataSetId: OptionalField[Long] = FieldNotSpecified,
    labels: Option[Seq[String]] = None
) extends WithNullableExtenalId
    with WithId[Option[Long]]

object AssetsUpsertSchema {
  implicit val toCreate: Transformer[AssetsUpsertSchema, AssetCreate] =
    Transformer
      .define[AssetsUpsertSchema, AssetCreate]
      .withFieldComputed(
        _.name,
        _.name.getOrElse(throw new CdfSparkIllegalArgumentException(
          "The name field must be set when creating assets.")))
      .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
      .buildTransformer

}

final case class AssetsInsertSchema(
    name: String,
    parentId: Option[Long] = None,
    description: Option[String] = None,
    source: Option[String] = None,
    externalId: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    parentExternalId: Option[String] = None,
    dataSetId: Option[Long] = None,
    labels: Option[Seq[String]] = None
)

final case class AssetsReadSchema(
    externalId: Option[String] = None,
    name: String,
    parentId: Option[Long] = None,
    parentExternalId: Option[String] = None,
    description: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    source: Option[String] = None,
    id: Long = 0,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    rootId: Option[Long] = Some(0),
    aggregates: Option[AssetsAggregatesSchema] = None,
    dataSetId: Option[Long] = None,
    labels: Option[Seq[String]] = None
)

final case class AssetsAggregatesSchema(
    // TODO: add actual support for these aggregated properties
    childCount: Option[Long] = None,
    path: Option[Array[String]] = None,
    depth: Option[Long] = None
)
