package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.{CdfSparkException, CdpConnector, RelationConfig}
import com.cognite.sdk.scala.common.{Items, ItemsWithCursor}
import com.cognite.sdk.scala.v1.{AuthSttpBackend, GenericClient}
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, JsonObject}
import org.apache.spark.sql.types.{DataType, StructType}
import sttp.client3.circe._
import sttp.client3.circe.asJson
import sttp.client3.{ResponseException, SttpBackend, UriContext, basicRequest}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import scala.concurrent.duration.DurationInt

class WdlClient(
    val config: RelationConfig,
) {
  import CdpConnector._
  implicit val decoder: Decoder[ItemsWithCursor[JsonObject]] = deriveDecoder
  implicit val encoder: Encoder[Items[JsonObject]] = deriveEncoder

  lazy val client: GenericClient[IO] = clientFromConfig(config)

  protected val baseUrl =
    uri"http://localhost:8080/api/playground/projects/${config.projectName}/wdl"
//  private val baseUrl =uri"${config.baseUrl}/api/playground/projects/${config.projectName}/wdl"

  implicit val sttpBackend: SttpBackend[IO, Any] = {
    val retryingBackend = retryingSttpBackend(
      config.maxRetries,
      config.maxRetryDelaySeconds,
      config.parallelismPerPartition,
    )
    val authProvider = config.auth.provider.unsafeRunSync()
    new AuthSttpBackend[IO, Any](
      retryingBackend,
      authProvider
    )
  }

  protected val sttpRequest = basicRequest
    .followRedirects(false)
    .header("x-cdp-sdk", s"CogniteWellsInSpark:${BuildInfo.BuildInfo.version}")
    .header("x-cdp-app", "cdp-spark-datasource")
    .header("cdf-version", "alpha")
    .readTimeout(3.seconds)

  def getSchema(modelType: String): StructType = {
    val url = uri"$baseUrl/spark/structtypes/$modelType"
    val response = sttpRequest
      .contentType("application/json")
      .header("accept", "application/json")
      .get(url)
      .send(sttpBackend)
      .map(_.body)
      .unsafeRunSync()
      .merge

    DataType.fromJson(response).asInstanceOf[StructType]
  }

  def getItems(modelType: String): ItemsWithCursor[JsonObject] = {
    val urlParts = modelType match {
      case "Well" => Seq("wells", "list")
      case "Npt" => Seq("npt", "list")
      case "Nds" => Seq("npt", "list")
      case "CasingSchematic" => Seq("casings", "list")
      case "Source" => Seq("sources")
      case _ => sys.error(s"Unknown model type: $modelType")
    }
    implicit val decoder: Decoder[ItemsWithCursor[JsonObject]] = deriveDecoder
    val url = uri"$baseUrl/".addPath(urlParts)

    val request = {
      val req = sttpRequest
        .header("accept", "application/json")

      if (modelType == "Source") {
        req
          .get(url)
      } else {
        req
          .contentType("application/json")
          .body("""{"limit": null}""")
          .post(url)
      }
    }

    val response = request
      .response(asJson[ItemsWithCursor[JsonObject]])
      .send(sttpBackend)
      .map(_.body)
      .unsafeRunSync()

    handleResponse(response)
  }

  private def getUrlPart(modelType: String): String =
    modelType match {
      case "Well" => "wells"
      case "WellIngestion" => "wells"
      case "Npt" => "npt"
      case "Nds" => "npt"
      case "CasingSchematic" => "casings"
      case _ => sys.error(s"Unknown model type: $modelType")
    }

  def setItems(modelType: String, items: Items[JsonObject]): ItemsWithCursor[JsonObject] = {
    val url = uri"$baseUrl/${getUrlPart(modelType)}"
    val response = sttpRequest
      .contentType("application/json")
      .header("accept", "application/json")
      .body(items)
      .post(url)
      .response(asJson[ItemsWithCursor[JsonObject]])
      .send(sttpBackend)
      .map(_.body)
      .unsafeRunSync()

    handleResponse(response)
  }

  private def handleResponse[O](
      response: Either[ResponseException[String, io.circe.Error], ItemsWithCursor[O]])
    : ItemsWithCursor[O] =
    response match {
      case Left(e) =>
        throw new CdfSparkException(s"Failed to run WDL query: $e")
      case Right(a) => a
    }
}
