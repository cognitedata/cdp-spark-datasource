package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfSparkException, CdpConnector, RelationConfig}
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax.EncoderOps
import sttp.client3.UriContext

class TestWdlClient(config: RelationConfig) extends WdlClient(config) {
  import CdpConnector.ioRuntime

  def post[Input, Output](url: String, body: Input)(
    implicit encoder: Encoder[Input],
    decoder: Decoder[Output],
  ): Output = {
    val bodyAsJson = body.asJson.noSpaces
    val urlParts = url.split("/")
    val fullUrl = uri"$baseUrl/$urlParts"
    val response = sttpRequest
      .contentType("application/json")
      .header("accept", "application/json")
      .body(bodyAsJson)
      .post(fullUrl)
      .send(sttpBackend)
      .map(_.body)
      .unsafeRunSync()

    response match {
      case Left(e) => throw new CdfSparkException(s"Query to $fullUrl failed: " + e)
      case Right(s) =>
        decode[Output](s) match {
          case Left(e) => throw new CdfSparkException("Failed to decode: " + e)
          case Right(decoded) => decoded
        }
    }
  }

  def get[Output](url: String)(
    implicit decoder: Decoder[Output],
  ): Output = {
    val urlParts = url.split("/")
    val fullUrl = uri"$baseUrl/$urlParts"
    val response = sttpRequest
      .contentType("application/json")
      .header("accept", "application/json")
      .get(fullUrl)
      .send(sttpBackend)
      .map(_.body)
      .unsafeRunSync()

    response match {
      case Left(e) => throw new CdfSparkException(s"Query to $fullUrl failed: " + e)
      case Right(s) => {
        decode[Output](s) match {
          case Left(e) => throw new CdfSparkException("Failed to decode: " + e)
          case Right(decoded) => decoded
        }
      }
    }
  }

  def ingestSources(sources: Seq[Source]): Seq[Source] = {
    val created = post[SourceItems, SourceItems]("sources", SourceItems(sources))
    created.items
  }

  def getSources: Seq[Source] =
    get[SourceItems]("sources").items

  def deleteSources(sourceNames: Seq[String]): Unit = {
    val items = sourceNames.map(name => Source(name))
    val deleteSources = DeleteSources(items = items)

    val _ = post[DeleteSources, EmptyObj]("sources/delete", deleteSources)
  }

  def setMergeRules(sources: Seq[String]): Unit = {
    // Discard two values. Can't set both to _, so I'm doing this gymnastics.
    val _1 = post[WellMergeRules, EmptyObj]("wells/mergerules", WellMergeRules(sources))
    val _2 = post[WellboreMergeRules, EmptyObj]("wellbores/mergerules", WellboreMergeRules(sources))
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
    post[WellIngestionItems, WellItems]("wells", body).items
  }

  def ingestWellbores(wells: Seq[WellboreIngestion]): Seq[Wellbore] = {
    val body = WellboreIngestionItems(wells)
    post[WellboreIngestionItems, WellboreItems]("wellbores", body).items
  }

  case class MiniSetup(
      source: Source,
      well: Well,
      wellIngesiton: WellIngestion,
      wellbore: Wellbore,
      wellboreIngestion: WellboreIngestion
  )

  def miniSetup(): MiniSetup = {
    val source = ingestSources(Seq(Source("A"))).head
    setMergeRules(Seq("A"))
    val wellIngestion = WellIngestion(
      matchingId = Some("w1"),
      source = AssetSource("A:w1", "A"),
      name = "w1",
      wellhead = Some(Wellhead(0.0, 60.0, "EPSG:4326"))
    )
    val well = ingestWells(Seq(wellIngestion)).head
    val wellboreIngestion = WellboreIngestion(
      matchingId = Some("wb1"),
      name = "wb1",
      source = AssetSource("A:wb1", "A"),
      datum = Some(Datum(50.0, "meter", "KB")),
      wellAssetExternalId = "A:w1",
    )
    val wellbore = ingestWellbores(Seq(wellboreIngestion)).head
    MiniSetup(
      source = source,
      well = well,
      wellIngesiton = wellIngestion,
      wellbore = wellbore,
      wellboreIngestion = wellboreIngestion
    )
  }

  def getWells: Seq[Well] =
    post[EmptyObj, WellItems]("wells/list", EmptyObj()).items

  def deleteWells(wells: Seq[AssetSource], recursive: Boolean = false): Unit = {
    val body = DeleteWells(wells, recursive = recursive)
    val _ = post[DeleteWells, EmptyObj]("wells/delete", body)
  }

  def initTestWells(): Seq[Well] = {
    val items = Seq(
      WellIngestion(
        name = "34/10-8",
        uniqueWellIdentifier = Some("34/10-8"),
        waterDepth = Some(Distance(100.0, "meter")),
        wellhead = Some(
          Wellhead(
            x = 457008.04,
            y = 6781760.88,
            crs = "EPSG:23031"
          )),
        source = AssetSource("asset:34/10-8", "EDM"),
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
    post[WellIngestionItems, WellItems]("wells", wellIngestionItems).items
  }
}
