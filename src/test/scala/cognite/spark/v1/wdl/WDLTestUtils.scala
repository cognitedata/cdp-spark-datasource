package cognite.spark.v1.wdl

import cats.effect.unsafe.implicits.global
import cognite.spark.v1.{CdfSparkAuth, RelationConfig, SparkTest}
import com.cognite.sdk.scala.common.ApiKeyAuth
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax.EncoderOps
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import sttp.client3.UriContext

class WDLTestUtilsTest extends FlatSpec with Matchers with SparkTest with Inspectors {

  private class TestWdlClient(config: RelationConfig) extends WdlClient(config) {
    def getSources(): String = {
      val url = uri"$baseUrl/sources"
      val response = sttpRequest
        .contentType("application/json")
        .header("accept", "application/json")
        .get(url)
        .send(sttpBackend)
        .map(_.body)
        .unsafeRunSync()
        .merge

      response
    }

    def deleteSources(sourceNames: Seq[String]): String = {
      val url = uri"$baseUrl/sources/delete"

      val items = sourceNames.map(name => Source(name))
      val deleteSources = DeleteSources(items = items, recursive = Some(true))
      val stringBody = deleteSources.asJson.noSpaces

      val response = sttpRequest
        .contentType("application/json")
        .header("accept", "application/json")
        .body(stringBody)
        .post(url)
        .send(sttpBackend)
        .map(_.body)
        .unsafeRunSync()
        .merge

      response
    }

    def initTestSources(): String = {
      val url = uri"$baseUrl/sources"

      val items = Map(
        ("EDM", "EDM SOURCE"),
        ("VOLVE", "VOLVE SOURCE"),
        ("test_source", "For testing merging with 3 sources"),
      ).map {
        case (name, description) =>
          Source(name, Some(description))
      }

      val sourceItems = SourceItems(items = items.toSeq)
      val stringBody = sourceItems.asJson.noSpaces

      val response = sttpRequest
        .contentType("application/json")
        .header("accept", "application/json")
        .body(stringBody)
        .post(url)
        .send(sttpBackend)
        .map(_.body)
        .unsafeRunSync()
        .merge

      response
    }

    def getWells(): String = {
      val url = uri"$baseUrl/wells"
      val response = sttpRequest
        .contentType("application/json")
        .header("accept", "application/json")
        .get(url)
        .send(sttpBackend)
        .map(_.body)
        .unsafeRunSync()
        .merge

      response
    }

    def initTestWells(): String = {
      val url = uri"$baseUrl/wells"

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
      val stringBody = wellIngestionItems.asJson.noSpaces

      val response = sttpRequest
        .contentType("application/json")
        .header("accept", "application/json")
        .body(stringBody)
        .post(url)
        .send(sttpBackend)
        .map(_.body)
        .unsafeRunSync()
        .merge

      response
    }
  }

  private val config = getDefaultConfig(CdfSparkAuth.Static(ApiKeyAuth(writeApiKey)))
  private val testWdlClient = new TestWdlClient(config)

  it should "ingest and read WDL sources" in {
    val contentsBeforeJson = testWdlClient.getSources()
    val contentsBefore = decode[SourceItems](contentsBeforeJson)
    println(contentsBefore)

    val inserted = testWdlClient.initTestSources()
    println(inserted)

    val contentsAfterJson = testWdlClient.getSources()
    val contentsAfter = decode[SourceItems](contentsAfterJson)
    println(contentsAfter)

//    val toDelete = contentsAfter
//      .getOrElse(SourceItems(items = Seq.empty[Source]))
//      .items.map(source => source.name)

//    if (toDelete.nonEmpty) {
//      val deletedJson = testWdlClient.deleteSources(toDelete)
//      println(deletedJson)
//      val deleted = decode[SourceItems](deletedJson)
//      println(deleted)
//    }
  }

  it should "ingest and read WDL wells" in {

//    val contentsBeforeJson = testWdlClient.getWells()
//    val contentsBefore = decode[WellItems](contentsBeforeJson)
//    println(contentsBefore)

    val inserted = testWdlClient.initTestWells()
    println(inserted)

//    val contentsAfterJson = testWdlClient.getWells()
//    val contentsAfter = decode[SourceItems](contentsAfterJson)
//    println(contentsAfter)
  }
}
