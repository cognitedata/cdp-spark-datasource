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
    extends SdkV1Relation[Asset, Long](config, "assets")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, Asset]] = {
    val fieldNames = Array("name", "source", "dataSetId")
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
      .map(s => s.reduce(_.merge(_)))
  }

  private def assetsFilterFromMap(m: Map[String, String]): AssetsFilter =
    AssetsFilter(
      name = m.get("name"),
      source = m.get("source"),
      dataSetIds = m.get("dataSetId").map(assetIdsFromWrappedArray(_).map(CogniteInternalId))
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val assets = fromRowWithFilteredMetadata(rows)
    client.assets
      .create(assets)
      .flatTap(_ => incMetrics(itemsCreated, assets.size)) *> IO.unit
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val assetUpdates = rows.map(r => fromRow[AssetsUpsertSchema](r))
    updateByIdOrExternalId[AssetsUpsertSchema, AssetUpdate, Assets[IO], Asset](
      assetUpdates,
      client.assets
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => fromRow[DeleteItem](r))
    deleteWithIgnoreUnknownIds(client.assets, deletes, config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val assets = rows.map { r =>
      val asset = fromRow[AssetsUpsertSchema](r)
      asset.copy(metadata = filterMetadata(asset.metadata))
    }
    val (itemsToUpdate, itemsToCreate) = assets.partition(r => r.id.exists(_ > 0))

    if (itemsToCreate.exists(_.name.isEmpty)) {
      throw new IllegalArgumentException("The name field must be set when creating assets.")
    }

    genericUpsert[Asset, AssetsUpsertSchema, AssetCreate, AssetUpdate, Assets[IO]](
      itemsToUpdate,
      itemsToCreate.map(_.into[AssetCreate].withFieldComputed(_.name, _.name.get).transform),
      client.assets)
  }

  private def fromRowWithFilteredMetadata(rows: Seq[Row]): Seq[AssetCreate] =
    rows.map { r =>
      val asset = fromRow[AssetCreate](r)
      asset.copy(metadata = filterMetadata(asset.metadata))
    }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val assets = fromRowWithFilteredMetadata(rows)
    createOrUpdateByExternalId[Asset, AssetUpdate, AssetCreate, Assets[IO]](
      Seq.empty,
      assets,
      client.assets,
      doUpsert = true)
  }

  override def schema: StructType = structType[Asset]

  override def toRow(a: Asset): Row = asRow(a)

  override def uniqueId(a: Asset): Long = a.id
}
object AssetsRelation extends UpsertSchema {
  val upsertSchema = structType[AssetsUpsertSchema]
  val insertSchema = structType[AssetsInsertSchema]
  val readSchema = structType[AssetsReadSchema]
}

case class AssetsUpsertSchema(
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

case class AssetsInsertSchema(
    name: String,
    parentId: Option[Long] = None,
    description: Option[String] = None,
    source: Option[String] = None,
    externalId: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    parentExternalId: Option[String] = None,
    dataSetId: Option[Long] = None
)

case class AssetsReadSchema(
    externalId: Option[String] = None,
    name: String,
    parentId: Option[Long] = None,
    description: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    source: Option[String] = None,
    id: Long = 0,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    rootId: Long = 0,
    aggregates: Option[Map[String, Long]] = None,
    dataSetId: Option[Long] = None
)
