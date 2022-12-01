package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.Wells
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class WellsRelation(config: RelationConfig, subtreeIds: Option[List[CogniteId]] = None)(
    sqlContext: SQLContext)
    extends SdkV1Relation[WellsReadSchema, Long](config, "assets")
    with InsertableRelation
    with WritableRelation {
  Array("name", "source", "dataSetId", "labels", "id", "externalId", "externalIdPrefix")
  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, WellsReadSchema]] = {
    (ids, filters) =
      pushdownToFilters(sparkFilters, assetsFilterFromMap, WellsFilter(assetSubtreeIds = subtreeIds))
    executeFilter(client.assets, filters, ids, numPartitions, limit)
      .map(
        _.map(
          _.into[WellsReadSchema]
            .withFieldComputed(_.labels, asset => cogniteExternalIdSeqToStringSeq(asset.labels))
            .withFieldComputed(
              _.aggregates,
              asset =>
                asset.aggregates.map { aggregates =>
                  WellsAggregatesSchema(
                    aggregates.get("childCount"),
                    None, // No support for path yet.
                    aggregates.get("depth")
                  )
              }
            )
            .transform))
  }

  private def assetsFilterFromMap(m: Map[String, String]): WellsFilter =
    WellsFilter(
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
    assetsInsertions = rows.map(fromRow[WellIngestionInsertSchema](_))
    assets = assetsInsertions.map(
      _.into[WellCreate]
        .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
        .transform)
    client.assets
      .create(assets)
      .flatTap(_ => incMetrics(itemsCreated, assets.size)) *> IO.unit
  }

  private def isUpdateEmpty(u: WellUpdate): Boolean = u == WellUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    assetUpdates = rows.map(r => fromRow[WellIngestionInsertSchema](r))

    updateByIdOrExternalId[WellIngestionInsertSchema, WellUpdate, Wells[IO], Well](
      assetUpdates,
      client.assets,
      isUpdateEmpty
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    deletes = rows.map(fromRow[DeleteItemByCogniteId](_))
    deleteWithIgnoreUnknownIds(client.assets, deletes.map(_.toCogniteId), config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    assets = rows.map(fromRow[WellIngestionInsertSchema](_))
    genericUpsert[Well, WellIngestionInsertSchema, WellCreate, WellUpdate, Wells[IO]](
      assets,
      isUpdateEmpty,
      client.assets,
      mustBeUpdate = r => r.name.isEmpty && r.getExternalId.nonEmpty
    )
  }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    assetsUpserts = rows.map(fromRow[WellIngestionInsertSchema](_))
    assets = assetsUpserts.map(_.transformInto[WellCreate])
    createOrUpdateByExternalId[Well, WellUpdate, WellCreate, WellCreate, Option, Wells[IO]](
      Set.empty,
      assets,
      client.assets,
      doUpsert = true)
  }

  override def schema: StructType = structType[WellsReadSchema]()

  override def toRow(a: WellsReadSchema): Row = asRow(a)

  override def uniqueId(a: WellsReadSchema): Long = a.id
}

object WellsRelation extends UpsertSchema {
  upsertSchema: StructType = structType[WellIngestionInsertSchema]()
  insertSchema: StructType = structType[WellIngestionInsertSchema]()
  readSchema: StructType = structType[WellsReadSchema]()
}

final case class WellheadsReadSchema(
    x: Double,
    y: Double,
    /* The coordinate reference systems of the wellhead. */
    crs: String,
)

final case class AssetSourceReadSchema(
    /* Asset external ID. */
    assetExternalId: String,
    /* Name of the source this asset external ID belongs to. */
    sourceName: String,
)

object DistanceUnitEnum extends Enumeration {
  type DistanceUnit = Value

  val meter = Value("meter")

  val foot = Value("foot")

  val inch = Value("inch")

  val yard = Value("yard")
}

final case class Distance(
    /* Amount of a given unit. */
    value: Double,
    unit: DistanceUnitEnum.DistanceUnit,
)

final case class Datum (
    /* Amount of a given unit. */
    value: Double,
    unit: DistanceUnitEnum.DistanceUnit,
    /* The name of the reference point. Eg. \"KB\" for kelly bushing. */
    reference: String,
)

final case class WellboreReadSchema (
    /* Unique identifier used to match wellbores from different sources. The matchingId must be unique within a source. */
    matchingId: String,
    /* Name of the wellbore. */
    name: String,
    /* Matching id of the associated well. */
    wellMatchingId: String,
    /* List of sources that are associated to this wellbore. */
    sources: Seq[AssetSourceReadSchema],
    /* Description of the wellbore. */
    description: Option[String] = None,
    /* Parent wellbore if it exists. */
    parentWellboreMatchingId: Option[String] = None,
    /* Also called UBI. */
    uniqueWellboreIdentifier: Option[String] = None,
    datum: Option[Datum] = None,
    /* Total days of drilling for this wellbore */
    totalDrillingDays: Option[Double] = None,
)

final case class WellsReadSchema(
    /* Unique identifier used to match wells from different sources. */
    matchingId: String,
    /* Name of the well. */
    name: String,
    wellhead: WellheadsReadSchema,
    /* List of sources that are associated to this well. */
    sources: Seq[AssetSourceReadSchema],
    /* Description of the well. */
    description: Option[String] = None,
    /* Also called UWI. */
    uniqueWellIdentifier: Option[String] = None,
    /* Country of the well. */
    country: Option[String] = None,
    /* The quadrant of the well. This is the first part of the unique well identifier used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in quadrant `15`. */
    quadrant: Option[String] = None,
    /* Region of the well. */
    region: Option[String] = None,
    /* The block of the well. This is the second part of the unique well identifer used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in block `15/9`. */
    block: Option[String] = None,
    /* Field of the well. */
    field: Option[String] = None,
    /* Operator that owns the well. */
    operator: Option[String] = None,
    /* The date a new well was spudded or the date of first actual penetration of the earth with a drilling bit. */
    spudDate: Option[java.time.LocalDate] = None,
    /* Exploration or development. */
    wellType: Option[String] = None,
    /* Well licence. */
    license: Option[String] = None,
    /* Water depth of the well. Vertical distance from the mean sea level (MSL) to the sea bed. */
    waterDepth: Option[Distance] = None,
    /* List of wellbores that are associated to this well. */
    wellbores: Option[Seq[WellboreReadSchema]] = None,
)

final case class WellIngestionInsertSchema (
    /* Name of the well. */
    name: String,
    /* Connection between this well and the well asset with a given source. */
    source: AssetSourceReadSchema,
    /* Unique identifier used to match wells from different sources. The matchingId must be unique within a source. */
    matchingId: Option[String] = None,
    /* Description of the well. */
    description: Option[String] = None,
    /* Also called UWI. */
    uniqueWellIdentifier: Option[String] = None,
    /* Country of the well. */
    country: Option[String] = None,
    /* The quadrant of the well. This is the first part of the unique well identifier used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in quadrant `15`. */
    quadrant: Option[String] = None,
    /* Region of the well. */
    region: Option[String] = None,
    /* The date a new well was spudded or the date of first actual penetration of the earth with a drilling bit. */
    spudDate: Option[java.time.LocalDate] = None,
    /* The block of the well. This is the second part of the unique well identifer used on the norwegian continental shelf. The wellbore `15/9-19-RS` in the VOLVE fild is in block `15/9`. */
    block: Option[String] = None,
    /* Field of the well. */
    field: Option[String] = None,
    /* Operator that owns the well. */
    operator: Option[String] = None,
    /* Exploration or development. */
    wellType: Option[String] = None,
    /* Well licence. */
    license: Option[String] = None,
    /* Water depth of the well. Vertical distance from the mean sea level (MSL) to the sea bed. */
    waterDepth: Option[Distance] = None,
    wellhead: Option[WellheadsReadSchema] = None,
)

final case class WellsAggregatesSchema(
    // TODO: add actual support for these aggregated properties
    childCount: Option[Long] = None,
    path: Option[Array[String]] = None,
    depth: Option[Long] = None
)
