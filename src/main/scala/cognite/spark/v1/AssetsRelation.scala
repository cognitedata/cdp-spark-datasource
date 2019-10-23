package cognite.spark.v1

import cats.effect.{ContextShift, IO}
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, AssetUpdate, AssetsFilter, GenericClient}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import cognite.spark.v1.SparkSchemaHelper._
import org.apache.spark.sql.types._
import cats.implicits._
import PushdownUtilities._
import com.cognite.sdk.scala.common.CdpApiException
import fs2.Stream

import scala.concurrent.ExecutionContext

class AssetsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Asset, Long](config, "assets")
    with InsertableRelation {
  @transient implicit lazy val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, Asset]] = {
    val fieldNames = Array("name", "source")
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
    AssetsFilter(name = m.get("name"), source = m.get("source"))

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val assetCreates = rows.map { r =>
      val assetCreate = fromRow[AssetCreate](r)
      assetCreate.copy(metadata = filterMetadata(assetCreate.metadata))
    }
    client.assets.create(assetCreates) *> IO.unit
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val assetUpdates = rows.map(r => fromRow[AssetUpdate](r))
    client.assets.update(assetUpdates) *> IO.unit
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val ids = rows.map(r => fromRow[DeleteItem](r).id)
    client.assets.deleteByIds(ids)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = getFromRowAndCreate(rows)

  def fromRowWithFilteredMetadata(rows: Seq[Row]): Seq[Asset] =
    rows.map { r =>
      val asset = fromRow[Asset](r)
      asset.copy(metadata = filterMetadata(asset.metadata))
    }
  override def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] = {
    val assets = fromRowWithFilteredMetadata(rows)

    client.assets
      .createFromRead(assets)
      .handleErrorWith {
        case e: CdpApiException =>
          if (e.code == 409) {
            val existingExternalIds =
              e.duplicated.get.map(j => j("externalId").get.asString.get)
            resolveConflict(existingExternalIds, assets)
          } else { IO.raiseError(e) }
      } *> IO.unit
  }

  def resolveConflict(existingExternalIds: Seq[String], assets: Seq[Asset]): IO[Unit] = {
    val (assetsToUpdate, assetsToCreate) = assets.partition(
      p => if (p.externalId.isEmpty) { false } else { existingExternalIds.contains(p.externalId.get) }
    )

    val idMap = client.assets
      .retrieveByExternalIds(existingExternalIds)
      .unsafeRunSync()
      .map(a => a.externalId -> a.id)
      .toMap

    val create =
      if (assetsToCreate.isEmpty) IO.unit else client.assets.createFromRead(assetsToCreate)
    val update =
      if (assetsToUpdate.isEmpty) { IO.unit } else {
        client.assets.updateFromRead(assetsToUpdate.map(a => a.copy(id = idMap(a.externalId))))
      }

    (create, update).parMapN((_, _) => ())
  }

  override def schema: StructType = structType[Asset]

  override def toRow(a: Asset): Row = asRow(a)

  override def uniqueId(a: Asset): Long = a.id
}
