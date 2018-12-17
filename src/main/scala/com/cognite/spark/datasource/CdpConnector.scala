package com.cognite.spark.datasource

import java.io.IOException
import java.util.concurrent.Executors

import cats.effect.{IO, Timer}
import io.circe.{Decoder, DecodingFailure, Encoder}
import cats.implicits._
import io.circe.generic.auto._
import io.circe.parser.decode

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.TimeoutException
import scala.concurrent.ExecutionContext

case class Data[A](data: A)
case class ItemsWithCursor[A](items: Seq[A], nextCursor: Option[String] = None)
case class Items[A](items: Seq[A])
case class CdpApiErrorPayload(code: Int, message: String)
case class Error[A](error: A)

import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import com.softwaremill.sttp.asynchttpclient.cats._

case class CdpApiException(url: Uri, code: Int, message: String)
  extends Throwable(s"Request to ${url.toString()} failed with status $code: $message") {
}

object CdpConnector {
  @transient implicit private val timer: Timer[IO] = IO.timer(
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(50)))
  @transient implicit private val sttpBackend: SttpBackend[IO, Nothing] = AsyncHttpClientCatsBackend[IO]()
  type CdpApiError = Error[CdpApiErrorPayload]
  type DataItemsWithCursor[A] = Data[ItemsWithCursor[A]]

  def baseUrl(project: String, version: String = "0.5"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project"
  }

  def get[A : Decoder](apiKey: String, url: Uri, batchSize: Int,
                       limit: Option[Int], maxRetries: Int = 10,
                       initialCursor: Option[String] = None): Iterator[A] = {
    getWithCursor(apiKey, url, batchSize, limit, maxRetries, initialCursor)
      .flatMap(_.chunk)
  }

  def onError[A, B](url: Uri): PartialFunction[Response[Either[B, A]], IO[A]] = {
    case Response(Right(Right(data)), _, _, _, _) => IO.pure(data)
    case Response(Left(bytes), statusCode, _, _, _) => parseCdpApiError(new String(bytes, "utf-8"), url, statusCode)
  }

  def parseCdpApiError(responseBody: String, url: Uri, statusCode: StatusCode): IO[Nothing] = {
    decode[CdpApiError](responseBody) match {
      case Right(cdpApiError) => IO.raiseError(CdpApiException(url, cdpApiError.error.code, cdpApiError.error.message))
      case Left(error) => IO.raiseError(CdpApiException(url, statusCode, error.getMessage))
    }
  }

  def getWithCursor[A : Decoder](apiKey: String, url: Uri, batchSize: Int,
                        limit: Option[Int], maxRetries: Int = 10,
                        initialCursor: Option[String] = None): Iterator[Chunk[A, String]] = {
    Batch.chunksWithCursor(batchSize, limit, initialCursor) { (chunkSize, cursor: Option[String]) =>
      val urlWithLimit = url.param("limit", chunkSize.toString)
      val getUrl = cursor.fold(urlWithLimit)(urlWithLimit.param("cursor", _))

      val result = sttp.header("Accept", "application/json")
        .header("api-key", apiKey).get(getUrl).response(asJson[DataItemsWithCursor[A]])
        .parseResponseIf(_ => true)
        .send()
        .flatMap(r => onError(getUrl)(r))

      val dataWithCursor = retryWithBackoff(result, 30.millis, maxRetries)
        .unsafeRunSync()
        .data
      (dataWithCursor.items, dataWithCursor.nextCursor)
    }
  }

  def post[A : Encoder](apiKey: String, url: Uri, items: Seq[A], maxRetries: Int = 10): IO[Unit] = {
    postOr(apiKey, url, items, maxRetries)(Map.empty)
  }

  def postOr[A : Encoder](apiKey: String, url: Uri, items: Seq[A], maxRetries: Int = 10)
                       (onResponse: PartialFunction[Response[String], IO[Unit]]): IO[Unit] = {
    val defaultHandling: PartialFunction[Response[String], IO[Unit]] = {
      case r if r.isSuccess => IO.unit
      case Response(Right(body), statusCode, _, _, _) => parseCdpApiError(body, url, statusCode)
      case r => IO.raiseError(CdpApiException(url, r.code, "Failed to read request body as string"))
    }
    val singlePost = sttp.header("Accept", "application/json")
      .header("api-key", apiKey)
      .parseResponseIf(_ => true)
      .body(Items(items))
      .post(url)
      .send()
      .flatMap(onResponse orElse defaultHandling)
    retryWithBackoff(singlePost, 30.millis, maxRetries)
  }

  // scalastyle:off cyclomatic.complexity
  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int)
                                 (implicit timer: Timer[IO]): IO[A] = {
    ioa.handleErrorWith {
      case cdpError: CdpApiException =>
        if (shouldRetry(cdpError.code) && maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(cdpError)
        }
      case exception @ (_: TimeoutException | _: IOException) =>
        if (maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
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
