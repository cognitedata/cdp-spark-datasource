package com.cognite.spark.datasource

import java.io.IOException
import java.util.concurrent.Executors

import cats.MonadError
import cats.effect.{Effect, IO, Timer}
import io.circe.{Decoder, Encoder}
import org.http4s.{EntityDecoder, Header, Headers, Method, Request, Response, Uri}
import org.http4s.client.blaze.{BlazeClientConfig, Http1Client}
import org.http4s.client.JavaNetClientBuilder

import cats.implicits._
import io.circe.generic.auto._
import okhttp3.HttpUrl
import org.http4s.Status.Successful
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.util.CaseInsensitiveString

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.TimeoutException
import cats.effect.IO.timer

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
  extends Throwable(s"Request to ${url.renderString} failed with status $code: $message") {
}

object CdpConnector {
  implicit val timer = cats.effect.IO.timer(ExecutionContext.global)
  implicit val sttpBackend = AsyncHttpClientCatsBackend[IO]()
//  val httpClient: Client[IO] = Http1Client(
//    BlazeClientConfig.defaultConfig.copy(responseHeaderTimeout = 60.seconds))
//    .unsafeRunSync()
  val blockingEC = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(5))
  private implicit val contextShift = IO.contextShift(ExecutionContext.global)
  val httpClient: Client[IO] = JavaNetClientBuilder(blockingEC).create
  type CdpApiError = Error[CdpApiErrorPayload]
  type DataItemsWithCursor[A] = Data[ItemsWithCursor[A]]

  //private implicit val timer = Timer.derive
  def baseUrl(project: String, version: String = "0.5"): HttpUrl.Builder = {
    new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegment("api")
      .addPathSegment(version)
      .addPathSegment("projects")
      .addPathSegment(project)
  }

  def get[A : Decoder](apiKey: String, url: HttpUrl, batchSize: Int,
                       limit: Option[Int], maxRetries: Int = 10,
                       initialCursor: Option[String] = None): Iterator[A] = {
    getWithCursor(apiKey, url, batchSize, limit, maxRetries, initialCursor)
      .flatMap(_.chunk)
  }

  def getWithCursor[A : Decoder](apiKey: String, url: HttpUrl, batchSize: Int,
                        limit: Option[Int], maxRetries: Int = 10,
                        initialCursor: Option[String] = None): Iterator[Chunk[A, String]] = {
    Batch.chunksWithCursor(batchSize, limit, initialCursor) { (chunkSize, cursor: Option[String]) =>
      val baseUrl = Uri.unsafeFromString(url.toString).withQueryParam("limit", chunkSize)
      val getUrl = cursor.fold(baseUrl)(cursor => baseUrl.withQueryParam("cursor", cursor))
      //val getUrl = uri"${cursor.fold(baseUrl)(cursor => baseUrl.withQueryParam("cursor", cursor)).toString()}"

      println(s"Getting from ${getUrl.toString()}")
      val request = Request[IO](Method.GET, getUrl,
        headers = Headers(Header.Raw(CaseInsensitiveString("api-key"), apiKey)))
      val result = httpClient.expectOr[DataItemsWithCursor[A]](request)(onError(getUrl, _))

//      val result = sttp.header("api-key", apiKey).get(getUrl).response(asJson[DataItemsWithCursor[A]])
//        .parseResponseIf(_ => true)
//        .send()
//        .map(r => r.unsafeBody match {
//          case Left(error) => throw new RuntimeException(s"boom ${error.message}, ${r.statusText} ${r.code.toString} ${r.toString()}")
//          case Right(items) => items
//        })
      val dataWithCursor = retryWithBackoff(result, 30.millis, maxRetries)
        .unsafeRunSync()
        .data
      //println(s"got data: ${dataWithCursor.toString}")
      (dataWithCursor.items, dataWithCursor.nextCursor)
    }
  }

  def post[A : Encoder](apiKey: String, url: HttpUrl, items: Seq[A], maxRetries: Int = 10): IO[Unit] = {
    postOr(apiKey, url, items, maxRetries)(Map.empty)
  }

  def postOr[A : Encoder](apiKey: String, url: HttpUrl, items: Seq[A], maxRetries: Int = 10)
                       (onResponse: PartialFunction[Response[IO], IO[Unit]]): IO[Unit] = {
    val postUrl = Uri.unsafeFromString(url.toString)
    val request = Request[IO](Method.POST, postUrl,
      headers = Headers(Header.Raw(CaseInsensitiveString("api-key"), apiKey)))
      .withBody(Items(items))
    val defaultHandling: PartialFunction[Response[IO], IO[Unit]] = {
      case Successful(_) => ().asInstanceOf
      case failedResponse => onError(postUrl, failedResponse).flatMap(IO.raiseError)
    }
    val singlePost = httpClient.fetch(request) (onResponse orElse defaultHandling)
    retryWithBackoff(singlePost, 30.millis, maxRetries)
  }

  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int)
                                 (implicit timer: Timer[IO]): IO[A] = {
    ioa.handleErrorWith {
      case cdpError: CdpApiException =>
        if (shouldRetry(cdpError.code) && maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(cdpError)
        }
      case malformedMessageBodyFailure:org.http4s.MalformedMessageBodyFailure =>
        val failureBody = malformedMessageBodyFailure.toHttpResponse[IO](org.http4s.HttpVersion.`HTTP/1.1`)
          .unsafeRunSync()
          .bodyAsText
          .compile.fold(List.empty[String]) { case (acc, str) => str :: acc }
        println(s"Failed to parse ${failureBody}")
        IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
      case _:TimeoutException | _:IOException =>
        IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
      case error =>
        println(s"woops: ${error.getCause.getClass.toString}")
        IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
    }
  }

  def onError[F[_]](url: Uri, response: Response[F])
                             (implicit F: MonadError[F, Throwable], decoder: EntityDecoder[F, CdpApiError]): F[Throwable] = {
    val error = response.as[CdpApiError]
    error.map[Throwable](e => CdpApiException(url, e.error.code, e.error.message))
      .handleError(e => {
        CdpApiException(url, response.status.code, e.getMessage)
      })
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

  // Legacy
  def baseRequest(apiKey: String): okhttp3.Request.Builder = {
    new okhttp3.Request.Builder()
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Accept-Charset", "utf-8")
      .header("api-key", apiKey)
  }
}
