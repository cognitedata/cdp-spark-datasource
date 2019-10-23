package cognite.spark.v1

import java.io.IOException

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.cognite.sdk.scala.common.{Auth, CdpApiException}
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.{Decoder, Printer}

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration._
import scala.util.Random

case class Data[A](data: A)
case class Items[A](items: Seq[A])
case class CdpApiErrorPayload(code: Int, message: String)
case class Error[A](error: A)
case class Login(user: String, loggedIn: Boolean, project: String, projectId: Long)

trait CdpConnector {
  import CdpConnector._
  implicit val customPrinter: Printer = Printer.noSpaces.copy(dropNullValues = true)

  def getProject(auth: Auth, maxRetries: Int, baseUrl: String): String = {
    val loginStatusUrl = uri"$baseUrl/login/status"

    val result = sttp
      .header("Accept", "application/json")
      .auth(auth)
      .get(loginStatusUrl)
      .response(asJson[Data[Login]])
      .parseResponseIf(_ => true)
      .send()
      .flatMap(r => onError(loginStatusUrl)(r))

    retryWithBackoff(result, Constants.DefaultInitialRetryDelay, maxRetries)
      .unsafeRunSync()
      .data
      .project
  }

  def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
    val result = sttp
      .header("Accept", "application/json")
      .header("x-cdp-sdk", Constants.SparkDatasourceVersion)
      .header("x-cdp-app", config.applicationId)
      .auth(config.auth)
      .get(url)
      .response(asJson[A])
      .parseResponseIf(_ => true)
      .send()
      .flatMap(r => onError(url)(r))

    retryWithBackoff(result, Constants.DefaultInitialRetryDelay, config.maxRetries)
  }

  def onError[A, B](url: Uri): PartialFunction[Response[Either[B, A]], IO[A]] = {
    case Response(Right(Right(data)), _, _, _, _) => IO.pure(data)
    case Response(Right(Left(DeserializationError(original, _, _))), statusCode, _, _, _) =>
      parseCdpApiError(original, url, statusCode)
    case Response(Left(bytes), statusCode, _, _, _) =>
      parseCdpApiError(new String(bytes, "utf-8"), url, statusCode)
  }

  def parseCdpApiError(responseBody: String, url: Uri, statusCode: StatusCode): IO[Nothing] =
    decode[CdpApiError](responseBody) match {
      case Right(cdpApiError) =>
        IO.raiseError(
          CdpApiException(url, cdpApiError.error.code, cdpApiError.error.message, None, None, None))
      case Left(error) =>
        IO.raiseError(
          CdpApiException(
            url,
            statusCode,
            s"${error.getMessage} reading '$responseBody'",
            None,
            None,
            None))
    }

  def defaultHandling(url: Uri): PartialFunction[Response[String], IO[Unit]] = {
    case r if r.isSuccess => IO.unit
    case Response(Right(body), statusCode, _, _, _) => parseCdpApiError(body, url, statusCode)
    case r =>
      IO.raiseError(
        CdpApiException(url, r.code, "Failed to read request body as string", None, None, None))
  }

  // scalastyle:off cyclomatic.complexity
  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int): IO[A] = {
    val exponentialDelay = (Constants.DefaultMaxBackoffDelay / 2).min(initialDelay * 2)
    val randomDelayScale = (Constants.DefaultMaxBackoffDelay / 2).min(initialDelay * 2).toMillis
    val nextDelay = Random.nextInt(randomDelayScale.toInt).millis + exponentialDelay
    ioa.handleErrorWith {
      case cdpError: CdpApiException =>
        if (shouldRetry(cdpError.code) && maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, nextDelay, maxRetries - 1)
        } else {
          IO.raiseError(cdpError)
        }
      case exception @ (_: TimeoutException | _: IOException) =>
        if (maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, nextDelay, maxRetries - 1)
        } else {
          IO.raiseError(exception)
        }
      case error => IO.raiseError(error)
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def shouldRetry(status: Int): Boolean = status match {
    // @larscognite: Retry on 429,
    case 429 => true
    // and I would like to say never on other 4xx, but we give 401 when we can't authenticate because
    // we lose connection to db, so 401 can be transient
    case 401 => true
    // 500 is hard to say, but we should avoid having those in the api
    case 500 =>
      true // we get random and transient 500 responses often enough that it's worth retrying them.
    // 502 and 503 are usually transient.
    case 502 => true
    case 503 => true

    // do not retry other responses.
    case _ => false
  }

  // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
  // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
  // Since null keys don't make sense to CDP either, remove them as well.
  // Additionally, values are limited to 512 characters, yet we still have data where values have
  // more characters than that, so truncate them to the valid length if required: it's necessary for
  // copying existing data, and probably for upserts as well.
  def filterMetadata(metadata: Option[Map[String, String]]): Option[Map[String, String]] =
    metadata.map(_.filter { case (k, v) => k != null && v != null }
      .mapValues(_.slice(0, Constants.MetadataValuePostMaxLength)))
}

object CdpConnector {
  @transient implicit lazy val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  @transient implicit val cs: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
  @transient implicit val sttpBackend: SttpBackend[IO, Nothing] =
    AsyncHttpClientCatsBackend[cats.effect.IO]()

  type DataItems[A] = Data[Items[A]]
  type CdpApiError = Error[CdpApiErrorPayload]
}
