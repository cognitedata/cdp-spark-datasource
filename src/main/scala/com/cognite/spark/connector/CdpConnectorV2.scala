package com.cognite.spark.connector

import cats.MonadError
import cats.effect.{IO, Timer}
import com.cognite.spark.connector.CdpConnector.DataItemsWithCursor
import io.circe.{Decoder, Encoder}
import org.http4s.{EntityDecoder, Header, Headers, Method, Request, Response, Uri}
import org.http4s.client.blaze.Http1Client
import cats.implicits._
import io.circe.generic.auto._
import okhttp3.HttpUrl
import org.http4s.Status.Successful
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.util.CaseInsensitiveString

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

case class CdpApiErrorPayload(code: Int, message: String)
case class Error[A](error: A)

case class CdpApiException(url: Uri, code: Int, message: String)
  extends Throwable(s"Request to ${url.renderString} failed with status $code: $message") {
}

object CdpConnectorV2 {
  val httpClient: Client[IO] = Http1Client[IO]().unsafeRunSync()
  type CdpApiError = Error[CdpApiErrorPayload]

  def get[A : Decoder](apiKey: String, url: HttpUrl, batchSize: Int,
                       limit: Option[Int], maxRetries: Int = 10): Iterator[A] = {
    Batch.withCursor(batchSize, limit) { (chunkSize, cursor: Option[String]) =>
      val baseUrl = Uri.unsafeFromString(url.toString).withQueryParam("limit", chunkSize)
      val getUrl = (baseUrl, cursor) match {
        case (u, Some(cur)) => u.withQueryParam("cursor", cur)
        case (u, None) => u
      }

      val request = Request[IO](Method.GET, getUrl,
        headers = Headers(Header.Raw(CaseInsensitiveString("api-key"), apiKey)))

      val result = httpClient.expectOr[DataItemsWithCursor[A]](request)(onError(getUrl, _))
      val dataWithCursor = retryWithBackoff(result, 100.millis, maxRetries)
        .unsafeRunSync()
        .data
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
      case Successful(_) => IO.unit
      case failedResponse => onError(postUrl, failedResponse).flatMap(IO.raiseError)
    }
    val singlePost = httpClient.fetch(request) (onResponse orElse defaultHandling)
    retryWithBackoff(singlePost, 100.millis, maxRetries)
  }

  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int)
                                 (implicit timer: Timer[IO]): IO[A] = {
    ioa.handleErrorWith {
      case error: CdpApiException =>
        if (shouldRetry(error.code) && maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(error)
        }
      case error => IO.raiseError(error)
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

  private def shouldRetry[F](status: Int): Boolean = status match {
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
