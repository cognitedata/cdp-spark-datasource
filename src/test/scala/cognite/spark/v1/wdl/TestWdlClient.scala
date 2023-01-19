package cognite.spark.v1.wdl

import io.circe.generic.auto._
import org.apache.spark.sql.types.StructType

class TestWdlClient(val client: WdlClient) {

  def getSchema(name: String): StructType = client.getSchema(name)

  def ingestSources(sources: Seq[Source]): Seq[Source] = {
    val created = client.post[SourceItems, SourceItems]("sources", SourceItems(sources))
    created.items
  }

  def getSources: Seq[Source] =
    client.get[SourceItems]("sources").items

  def deleteSources(sourceNames: Seq[String]): Unit = {
    val items = sourceNames.map(name => Source(name))
    val deleteSources = DeleteSources(items = items)

    val _ = client.post[DeleteSources, EmptyObj]("sources/delete", deleteSources)
  }

  def setMergeRules(sources: Seq[String]): Unit = {
    // Discard two values. Can't set both to _, so I'm doing this gymnastics.
    val _1 = client.post[WellMergeRules, EmptyObj]("wells/mergerules", WellMergeRules(sources))
    val _2 = client.post[WellboreMergeRules, EmptyObj]("wellbores/mergerules", WellboreMergeRules(sources))
    val _ = (_1, _2)
  }

  def deleteAll(): Unit = {
    val wells = getWells
    val wellSources = wells.flatMap(_.sources)
    if (wellSources.nonEmpty) {
      deleteWells(wellSources, recursive = true)
    }
    val sources = getSources
    if (sources.nonEmpty) {
      deleteSources(sources.map(_.name))
    }
  }

  def ingestWells(wells: Seq[WellIngestion]): Seq[Well] = {
    val body = WellIngestionItems(wells)
    client.post[WellIngestionItems, WellItems]("wells", body).items
  }

  def ingestWellbores(wells: Seq[WellboreIngestion]): Seq[Wellbore] = {
    val body = WellboreIngestionItems(wells)
    client.post[WellboreIngestionItems, WellboreItems]("wellbores", body).items
  }

  case class MiniSetup(
      source: Source,
      well: Well,
      wellIngesiton: WellIngestion,
      wellbores: Seq[Wellbore],
      wellboreIngestions: Seq[WellboreIngestion],
  )

  def miniSetup(): MiniSetup = {
    val source = ingestSources(Seq(Source("A"))).head
    setMergeRules(Seq("A"))
    val wellIngestion = WellIngestion(
      matchingId = Some("w1"),
      source = AssetSource(assetExternalId = "A:w1", sourceName = "A"),
      name = "w1",
      wellhead = Some(Wellhead(x = 0.0, y = 60.0, crs = "EPSG:4326"))
    )
    val well = ingestWells(Seq(wellIngestion)).head
    val wellboreIngestions = Seq(
      WellboreIngestion(
        matchingId = Some("wb1"),
        name = "wb1",
        source = AssetSource(assetExternalId = "A:wb1", sourceName = "A"),
        datum = Some(Datum(value = 50.0, unit = "meter", reference = "KB")),
        wellAssetExternalId = "A:w1",
      ),
      WellboreIngestion(
        matchingId = Some("wb2"),
        name = "wb1",
        source = AssetSource(assetExternalId = "A:wb2", sourceName = "A"),
        datum = Some(Datum(value = 50.0, unit = "meter", reference = "KB")),
        wellAssetExternalId = "A:w1",
      ),
    )
    val wellbores = ingestWellbores(wellboreIngestions)
    MiniSetup(
      source = source,
      well = well,
      wellIngesiton = wellIngestion,
      wellbores = wellbores,
      wellboreIngestions = wellboreIngestions
    )
  }

  def getWells: Seq[Well] =
    client.post[EmptyObj, WellItems]("wells/list", EmptyObj()).items

  def deleteWells(wells: Seq[AssetSource], recursive: Boolean = false): Unit = {
    val body = DeleteWells(wells, recursive = recursive)
    val _ = client.post[DeleteWells, EmptyObj]("wells/delete", body)
  }

  def initTestWells(): Seq[Well] = {
    val items = Seq(
      WellIngestion(
        name = "34/10-8",
        uniqueWellIdentifier = Some("34/10-8"),
        waterDepth = Some(Distance(value = 100.0, unit = "meter")),
        wellhead = Some(
          Wellhead(
            x = 457008.04,
            y = 6781760.88,
            crs = "EPSG:23031"
          )),
        source = AssetSource(assetExternalId = "asset:34/10-8", sourceName = "EDM"),
        description = Some("this is a test well ingestion"),
        country = Some("Norway"),
        quadrant = Some("8"),
        spudDate = Some(s"2017-05-17"),
        block = Some("34/10"),
        field = Some("field"),
        operator = Some("Op1"),
        wellType = Some("production"),
        license = Some("MIT"),
        region = Some("MyRegion"),
      )
    )

    val wellIngestionItems = WellIngestionItems(items = items)
    client.post[WellIngestionItems, WellItems]("wells", wellIngestionItems).items
  }
}
