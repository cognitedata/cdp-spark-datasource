package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.udf.CogniteUdfs.backend
import cognite.spark.v1.{CdfSparkException, CdpConnector, RelationConfig}
import com.cognite.sdk.scala.common.{AuthProvider, Items, ItemsWithCursor}
import com.cognite.sdk.scala.v1.AuthSttpBackend
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, JsonObject}
import org.apache.spark.sql.types.{DataType, StructType}
import sttp.client3.circe._
import sttp.client3.{Empty, RequestT, ResponseException, SttpBackend, UriContext, basicRequest}
import io.circe.parser._
import io.circe.syntax.EncoderOps
import org.apache.logging.log4j.LogManager.getLogger
import io.circe.generic.auto._
import scala.concurrent.duration.DurationInt

object WdlClient {
  def fromConfig(config: RelationConfig): WdlClient = {
    import CdpConnector._

    val authProvider = config.auth.provider(implicitly, backend)

    new WdlClient(
      baseUrl = config.baseUrl,
      projectName = config.projectName,
      maxRetries = config.maxRetries,
      maxRetryDelaySeconds = config.maxRetryDelaySeconds,
      parallelismPerPartition = config.parallelismPerPartition,
      authProvider = authProvider.unsafeRunSync()
    )
  }
}

case class LimitRequest(limit: Option[Int])

class WdlClient(
  baseUrl: String,
  val projectName: String,
  maxRetries: Int,
  maxRetryDelaySeconds: Int,
  parallelismPerPartition: Int,
  authProvider: AuthProvider[IO],
) {
  import CdpConnector._


  private val logger = getLogger

  implicit val decoder: Decoder[ItemsWithCursor[JsonObject]] = deriveDecoder
  implicit val encoder: Encoder[Items[JsonObject]] = deriveEncoder

  println(s"base url: $baseUrl") // So that the warning isn't fatal.
  private val basePath = uri"http://localhost:8080/api/playground/projects/${projectName}/wdl"
  // private val basePath =uri"$baseUrl/api/playground/projects/$projectName/wdl"

  implicit val sttpBackend: SttpBackend[IO, Any] = {
    val retryingBackend = retryingSttpBackend(
      maxRetries,
      maxRetryDelaySeconds,
      parallelismPerPartition,
    )
    new AuthSttpBackend[IO, Any](
      retryingBackend,
      authProvider
    )
  }

   private val sttpRequest: RequestT[Empty, Either[String, String], Any] = basicRequest
    .followRedirects(false)
    .header("x-cdp-sdk", s"CogniteWellsInSpark:${BuildInfo.BuildInfo.version}")
    .header("x-cdp-app", "cdp-spark-datasource")
    .header("cdf-version", "alpha")
    .readTimeout(3.seconds)

  def post[Input, Output](url: String, body: Input)(
    implicit encoder: Encoder[Input],
    decoder: Decoder[Output],
  ): Output = {
    logger.info(s"POST $url")
    val bodyAsJson = body.asJson.noSpaces
    val urlParts = url.split("/")
    val fullUrl = uri"$basePath/$urlParts"
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
    logger.info(s"GET $url")
    val urlParts = url.split("/")
    val fullUrl = uri"$basePath/$urlParts"
    val response = sttpRequest
      .contentType("application/json")
      .header("accept", "application/json")
      .get(fullUrl)
      .send(sttpBackend)
      .map(_.body)
      .unsafeRunSync()

    response match {
      case Left(e) => throw new CdfSparkException(s"Query to $fullUrl failed: " + e)
      case Right(s) =>
        decode[Output](s) match {
          case Left(e) => throw new CdfSparkException("Failed to decode", e)
          case Right(decoded) => decoded
        }
    }
  }

  def getSchema(modelType: String): StructType = {
    // circe doesn't understand how to decode a string into a string (decode[String]("{...}"))
    //     val response = get[String](s"spark/structtypes/$modelType") // this should have worked.
    // So, it has to be done like this:
    val url = uri"$basePath/spark/structtypes/$modelType"
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
    val url: String = getReadUrlPart(modelType).mkString("/")
    val input = LimitRequest(limit = None)
    if (modelType == "Source") {
      get[ItemsWithCursor[JsonObject]](url)
    } else {
      post[LimitRequest, ItemsWithCursor[JsonObject]](url, input)
    }
  }

  private def getReadUrlPart(modelType: String): Seq[String] =
    modelType.replace("Ingestion", "") match {
      case "Well" => Seq("wells", "list")
      case "Npt" => Seq("npt", "list")
      case "Nds" => Seq("npt", "list")
      case "CasingSchematic" => Seq("casings", "list")
      case "Source" => Seq("sources")
      case _ => sys.error(s"Unknown model type: $modelType")
    }

  private def getWriteUrlPart(modelType: String): String =
    modelType.replace("Ingestion", "") match {
      case "Well" => "wells"
      case "Wellbore" => "wellbores"
      case "Npt" => "npt"
      case "Nds" => "npt"
      case "CasingSchematic" => "casings"
      case "Source" => "sources"
      case _ => sys.error(s"Unknown model type: $modelType")
    }

  def setItems(modelType: String, items: Items[JsonObject]): ItemsWithCursor[JsonObject] = {
    val url = uri"$basePath/${getWriteUrlPart(modelType)}"
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
