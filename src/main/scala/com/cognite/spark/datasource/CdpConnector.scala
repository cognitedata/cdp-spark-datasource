package com.cognite.spark.datasource

import java.io.IOException

import cats.effect.{IO, Timer}
import cats.implicits._
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.{Decoder, Encoder}

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration._
import scala.util.Random

case class Data[A](data: A)
case class ItemsWithCursor[A](items: Seq[A], nextCursor: Option[String] = None)
case class Items[A](items: Seq[A])
case class CdpApiErrorPayload(code: Int, message: String)
case class Error[A](error: A)
case class Login(user: String, loggedIn: Boolean, project: String, projectId: Long)

case class CdpApiException(url: Uri, code: Int, message: String)
  extends Throwable(s"Request to ${url.toString()} failed with status $code: $message") {
}

trait CdpConnector {
  import CdpConnector._

  def getProject(apiKey: String, maxRetries: Int): String = {
    val loginStatusUrl = uri"https://api.cognitedata.com/login/status"
    val jsonResult = getJson[Data[Login]](apiKey, loginStatusUrl, maxRetries)
    jsonResult.unsafeRunSync().data.project
  }

  def baseUrl(project: String, version: String = "0.5"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project"
  }

  def getJson[A : Decoder](apiKey: String, url: Uri, maxRetries: Int): IO[A] = {
    val result = sttp.header("Accept", "application/json")
      .header("api-key", apiKey).get(url).response(asJson[A])
      .parseResponseIf(_ => true)
      .send()
      .flatMap(r => onError(url)(r))

    retryWithBackoff(result, Constants.DefaultInitialRetryDelay, maxRetries)
  }

  def getProtobuf[A](apiKey: String, url: Uri,
                     parseResult: Response[Array[Byte]] => Response[A],
                     maxRetries: Int): IO[A] = {
    val result = sttp.header("Accept", "application/protobuf")
      .header("api-key", apiKey).get(url).response(asByteArray)
      .parseResponseIf(_ => true)
      .send()
      .map(parseResult)
      .flatMap(r => r.body match {
        case Right(body) => IO.pure(body)
        case Left(error) => IO.raiseError(CdpApiException(url, r.code, error))
      })

    retryWithBackoff(result, Constants.DefaultInitialRetryDelay, maxRetries)
  }

  def get[A : Decoder](apiKey: String, url: Uri, batchSize: Int,
                       limit: Option[Int], maxRetries: Int,
                       initialCursor: Option[String] = None): Iterator[A] = {
    getWithCursor(apiKey, url, batchSize, limit, maxRetries, initialCursor)
      .flatMap(_.chunk)
  }

  def onError[A, B](url: Uri): PartialFunction[Response[Either[B, A]], IO[A]] = {
    case Response(Right(Right(data)), _, _, _, _) => IO.pure(data)
    case Response(Right(Left(DeserializationError(original, _, _))), statusCode, _, _, _) => parseCdpApiError(original, url, statusCode)
    case Response(Left(bytes), statusCode, _, _, _) => parseCdpApiError(new String(bytes, "utf-8"), url, statusCode)
  }

  def parseCdpApiError(responseBody: String, url: Uri, statusCode: StatusCode): IO[Nothing] = {
    decode[CdpApiError](responseBody) match {
      case Right(cdpApiError) => IO.raiseError(CdpApiException(url, cdpApiError.error.code, cdpApiError.error.message))
      case Left(error) => IO.raiseError(CdpApiException(url, statusCode, error.getMessage))
    }
  }

  def defaultHandling(url: Uri): PartialFunction[Response[String], IO[Unit]] = {
    case r if r.isSuccess => IO.unit
    case Response(Right(body), statusCode, _, _, _) => parseCdpApiError(body, url, statusCode)
    case r => IO.raiseError(CdpApiException(url, r.code, "Failed to read request body as string"))
  }

  def getWithCursor[A: Decoder](apiKey: String, url: Uri, batchSize: Int,
                                limit: Option[Int], maxRetries: Int,
                                initialCursor: Option[String] = None): Iterator[Chunk[A, String]] = {
    Batch.chunksWithCursor(batchSize, limit, initialCursor) { (chunkSize, cursor: Option[String]) =>
      val urlWithLimit = url.param("limit", chunkSize.toString)
      val getUrl = cursor.fold(urlWithLimit)(urlWithLimit.param("cursor", _))

      val dataWithCursor = getJson[DataItemsWithCursor[A]](apiKey, getUrl, maxRetries).unsafeRunSync().data
      (dataWithCursor.items, dataWithCursor.nextCursor)
    }
  }

  def post[A : Encoder](apiKey: String, url: Uri, items: Seq[A], maxRetries: Int): IO[Unit] = {
    postOr(apiKey, url, items, maxRetries)(Map.empty)
  }

  def postOr[A : Encoder](apiKey: String, url: Uri, items: Seq[A], maxRetries: Int)
                       (onResponse: PartialFunction[Response[String], IO[Unit]]): IO[Unit] = {
    val singlePost = sttp.header("Accept", "application/json")
      .header("api-key", apiKey)
      .parseResponseIf(_ => true)
      .body(Items(items))
      .post(url)
      .send()
      .flatMap(onResponse orElse defaultHandling(url))
    retryWithBackoff(singlePost, Constants.DefaultInitialRetryDelay, maxRetries)
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

  def onError(url: Uri, response: Response[String]): Throwable = {
    decode[CdpApiError](response.unsafeBody)
      .fold(error => CdpApiException(url, response.code.toInt, error.getMessage),
        cdpApiError => CdpApiException(url, cdpApiError.error.code, cdpApiError.error.message))
  }

  private def shouldRetry(status: Int): Boolean = status match {
    // @larscognite: Retry on 429,
    case 429 => true
    // and I would like to say never on other 4xx, but we give 401 when we can't authenticate because
    // we lose connection to db, so 401 can be transient
    case 401 => true
    // 500 is hard to say, but we should avoid having those in the api
    //case 500 => false // let's not retry them for now
    // 502 and 503 are usually transient.
    case 502 => true
    case 503 => true

    // do not retry other responses.
    case _ => false
  }
}

object CdpConnector {
  @transient implicit lazy val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  @transient implicit lazy val sttpBackend: SttpBackend[IO, Nothing] = AsyncHttpClientCatsBackend[IO]()

  type DataItemsWithCursor[A] = Data[ItemsWithCursor[A]]
  type CdpApiError = Error[CdpApiErrorPayload]
}
