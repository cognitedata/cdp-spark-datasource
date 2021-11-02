package cognite.spark.v1

import java.time.Instant
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1.resources.Assets
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.Transformer
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
    val fieldNames =
      Array("name", "source", "dataSetId", "labels", "id", "externalId", "externalIdPrefix")
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val getAll = shouldGetAll(pushdownFilterExpression, fieldNames)
    val params = pushdownToParameters(pushdownFilterExpression)
    val pushdownFilters = if (params.isEmpty || getAll) {
      Seq(AssetsFilter())
    } else {
      params
        .filter { mapFieldNameToValue =>
          val fieldNames = mapFieldNameToValue.keySet
          !fieldNames.contains("id") && !fieldNames.contains("externalId")
        }
        .map(assetsFilterFromMap)
    }

    val ids = params.flatMap(_.get("id")).distinct
    val externalIds = params.flatMap(_.get("externalId")).distinct
    val streamsOfIdsAndExternalIds = if ((ids ++ externalIds).isEmpty) {
      Seq.empty
    } else {
      val assetFilteredById: Stream[IO, Asset] = getFromIdsOrExternalIds(
        ids,
        (ids: Seq[String]) => client.assets.retrieveByIds(ids.map(_.toLong), true))
      val assetFilteredByExternalId: Stream[IO, Asset] = getFromIdsOrExternalIds(
        externalIds,
        (ids: Seq[String]) => client.assets.retrieveByExternalIds(ids, true))
      Seq(assetFilteredById, assetFilteredByExternalId).distinct
    }
    val streamsPerFilter = pushdownFilters
      .map { f =>
        client.assets.filterPartitions(f, numPartitions, limit)
      }

    // Merge streams related to each partition to make sure duplicate values are read into
    // the same RDD partition
    (streamsPerFilter.transpose
      .map(
        s =>
          s.reduce(_.merge(_))
            .filter(a => checkDuplicateOnIdsOrExternalIds(a.id.toString, a.externalId, ids, externalIds))
      ) ++ streamsOfIdsAndExternalIds)
      .map(
        _.map(
          _.into[AssetsReadSchema]
            .withFieldComputed(_.labels, u => cogniteExternalIdSeqToStringSeq(u.labels))
            .transform))
  }

  private def assetsFilterFromMap(m: Map[String, String]): AssetsFilter =
    AssetsFilter(
      name = m.get("name"),
      source = m.get("source"),
      dataSetIds = m.get("dataSetId").map(idsFromWrappedArray(_).map(CogniteInternalId(_))),
      labels = m.get("labels").map(externalIdsToContainsAny).getOrElse(None),
      externalIdPrefix = m.get("externalIdPrefix")
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
    val deletes = rows.map(r => fromRow[DeleteItem](r))
    deleteWithIgnoreUnknownIds(client.assets, deletes, config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val assets = rows.map(fromRow[AssetsUpsertSchema](_))

    genericUpsert[Asset, AssetsUpsertSchema, AssetCreate, AssetUpdate, Assets[IO]](
      assets,
      isUpdateEmpty,
      client.assets,
      mustBeUpdate = r => r.name.isEmpty && r.getExternalId().nonEmpty
    )
  }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val assetsUpserts = rows.map(fromRow[AssetsUpsertSchema](_))
    val assets = assetsUpserts.map(_.transformInto[AssetCreate])
    createOrUpdateByExternalId[Asset, AssetUpdate, AssetCreate, AssetCreate, Option, Assets[IO]](
      Set.empty,
      assets,
      client.assets,
      doUpsert = true)
  }

  override def schema: StructType = structType[AssetsReadSchema]

  override def toRow(a: AssetsReadSchema): Row = asRow(a)

  override def uniqueId(a: AssetsReadSchema): Long = a.id
}

object AssetsRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[AssetsUpsertSchema]
  val insertSchema: StructType = structType[AssetsInsertSchema]
  val readSchema: StructType = structType[AssetsReadSchema]
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
    aggregates: Option[Map[String, Long]] = None,
    dataSetId: Option[Long] = None,
    labels: Option[Seq[String]] = None
)
