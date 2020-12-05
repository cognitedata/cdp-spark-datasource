package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common.{WithExternalId, WithId}
import com.cognite.sdk.scala.v1.resources.Assets
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class AssetsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[AssetsReadSchema, Long](config, "assets")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, AssetsReadSchema]] = {
    val fieldNames = Array("name", "source", "dataSetId", "labels")
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val getAll = shouldGetAll(pushdownFilterExpression, fieldNames)
    val params = pushdownToParameters(pushdownFilterExpression)

    val pushdownFilters = if (params.isEmpty || getAll) {
      Seq(AssetsFilter())
    } else {
      params.map(assetsFilterFromMap)
    }

    val streamsPerFilter = pushdownFilters
      .map { f =>
        client.assets.filterPartitions(f, numPartitions, limit)
      }

    // Merge streams related to each partition to make sure duplicate values are read into
    // the same RDD partition
    streamsPerFilter.transpose
      .map(s => s.reduce(_.merge(_)).map(toAssetReadSchema))
  }

  private def assetsFilterFromMap(m: Map[String, String]): AssetsFilter =
    AssetsFilter(
      name = m.get("name"),
      source = m.get("source"),
      dataSetIds = m.get("dataSetId").map(idsFromWrappedArray(_).map(CogniteInternalId)),
      labels = m.get("labels").map(externalIdsToContainsAny).getOrElse(None)
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val assetsInsertions = rows.map(fromRow[AssetsInsertSchema](_))
    val assets = assetsInsertions.map(toAssetCreate)
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
    val deletes = rows.map(r => fromRow[DeleteItem](r))
    deleteWithIgnoreUnknownIds(client.assets, deletes, config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    logger.info("We're upserting")
    val assets = rows.map(fromRow[AssetsUpsertSchema](_))
    val (itemsToUpdate, itemsToUpdateOrCreate) =
      assets.partition(r => r.id.exists(_ > 0) || (r.name.isEmpty && r.externalId.nonEmpty))

    logger.info(
      s"itemsToUpdate: ${itemsToUpdate.length}, itemsToUpdateOrCreate: ${itemsToUpdateOrCreate.length}")
    if (itemsToUpdateOrCreate.exists(_.name.isEmpty)) {
      throw new CdfSparkIllegalArgumentException("The name field must be set when creating assets.")
    }

    genericUpsert[Asset, AssetsUpsertSchema, AssetCreate, AssetUpdate, Assets[IO]](
      itemsToUpdate,
      itemsToUpdateOrCreate.map(_.into[AssetCreate].withFieldComputed(_.name, _.name.get).transform),
      isUpdateEmpty,
      client.assets)
  }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val assetsUpserts = rows.map(fromRow[AssetsUpsertSchema](_))
    val assets = assetsUpserts.map(assetUpsertSchemaToAssetCreate)
    createOrUpdateByExternalId[Asset, AssetUpdate, AssetCreate, Assets[IO]](
      Set.empty,
      assets,
      client.assets,
      doUpsert = true)

  }

  override def schema: StructType = structType[AssetsReadSchema]

  override def toRow(a: AssetsReadSchema): Row = asRow(a)

  override def uniqueId(a: AssetsReadSchema): Long = a.id

  def toAssetReadSchema(a: Asset): AssetsReadSchema =
    AssetsReadSchema(
      externalId = a.externalId,
      name = a.name,
      parentId = a.parentId,
      parentExternalId = a.parentExternalId,
      description = a.description,
      metadata = a.metadata,
      source = a.source,
      id = a.id,
      createdTime = a.createdTime,
      lastUpdatedTime = a.lastUpdatedTime,
      rootId = a.rootId,
      aggregates = a.aggregates,
      dataSetId = a.dataSetId,
      labels = cogniteExternalIdSeqToStringSeq(a.labels)
    )

  def toAssetCreate(a: AssetsInsertSchema): AssetCreate =
    AssetCreate(
      name = a.name,
      parentId = a.parentId,
      description = a.description,
      source = a.source,
      externalId = a.externalId,
      metadata = a.metadata,
      parentExternalId = a.parentExternalId,
      dataSetId = a.dataSetId,
      labels = stringSeqToCogniteExternalIdSeq(a.labels)
    )

  def assetUpsertSchemaToAssetCreate(a: AssetsUpsertSchema): AssetCreate =
    AssetCreate(
      name = a.name.get,
      parentId = a.parentId,
      description = a.description,
      source = a.source,
      externalId = a.externalId,
      metadata = a.metadata,
      parentExternalId = a.parentExternalId,
      dataSetId = a.dataSetId
    )
}

object AssetsRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[AssetsUpsertSchema]
  val insertSchema: StructType = structType[AssetsInsertSchema]
  val readSchema: StructType = structType[AssetsReadSchema]
}

final case class AssetsUpsertSchema(
    id: Option[Long] = None,
    name: Option[String] = None,
    description: Option[String] = None,
    source: Option[String] = None,
    externalId: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    parentId: Option[Long] = None,
    parentExternalId: Option[String] = None,
    dataSetId: Option[Long] = None
) extends WithExternalId
    with WithId[Option[Long]]

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
    aggregates: Option[Map[String, Long]] = None,
    dataSetId: Option[Long] = None,
    labels: Option[Seq[String]] = None
)
