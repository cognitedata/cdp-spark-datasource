package com.cognite.spark.datasource

import java.io.IOException

import cats.MonadError
import cats.effect.{IO, Timer}
import io.circe.{Decoder, Encoder}
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
  @transient implicit val timer = cats.effect.IO.timer(ExecutionContext.global)
  @transient implicit val sttpBackend = AsyncHttpClientCatsBackend[IO]()
  type CdpApiError = Error[CdpApiErrorPayload]
  type DataItemsWithCursor[A] = Data[ItemsWithCursor[A]]

  def baseUrl(project: String, version: String = "0.5"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project"
  }

  def get[A : Decoder](apiKey: String, url: Uri, batchSize: Int,
                       limit: Option[Int], maxRetries: Int = 3,
                       initialCursor: Option[String] = None): Iterator[A] = {
    getWithCursor(apiKey, url, batchSize, limit, maxRetries, initialCursor)
      .flatMap(_.chunk)
  }

  def getWithCursor[A : Decoder](apiKey: String, url: Uri, batchSize: Int,
                        limit: Option[Int], maxRetries: Int = 3,
                        initialCursor: Option[String] = None): Iterator[Chunk[A, String]] = {
    Batch.chunksWithCursor(batchSize, limit, initialCursor) { (chunkSize, cursor: Option[String]) =>
      val urlWithLimit = url.param("limit", chunkSize.toString)
      val getUrl = cursor.fold(urlWithLimit)(urlWithLimit.param("cursor", _))
      //url.queryFragments(QueryFragment)


      //println(s"Getting from ${getUrl.toString()} url ${url.toString()} with batchSize $batchSize chunkSize $chunkSize limit ${limit.map(_.toString).getOrElse("none")} cursor ${cursor.getOrElse("none")}")

      val result = sttp.header("Accept", "application/json")
        .header("api-key", apiKey).get(getUrl).response(asJson[DataItemsWithCursor[A]])
        .parseResponseIf(_ => true)
        .send()
        .map(r => r.unsafeBody match {
          case Left(error) =>
            //println(s"boom ${error.message}, ${r.statusText} ${r.code.toString} ${r.toString()}")
            throw new RuntimeException(s"boom ${error.message}, ${r.statusText} ${r.code.toString} ${r.toString()}")
          case Right(items) => items
        })
      val dataWithCursor = retryWithBackoff(result, 30.millis, maxRetries)
        .unsafeRunSync()
        .data
        //println(s"got data: ${dataWithCursor.toString}")
      (dataWithCursor.items, dataWithCursor.nextCursor)
    }
  }

  def post[A : Encoder](apiKey: String, url: Uri, items: Seq[A], maxRetries: Int = 3): IO[Unit] = {
    postOr(apiKey, url, items, maxRetries)(Map.empty)
  }

  def postOr[A : Encoder](apiKey: String, url: Uri, items: Seq[A], maxRetries: Int = 3)
                       (onResponse: PartialFunction[Response[String], IO[Unit]]): IO[Unit] = {
//    val postUrl = Uri.unsafeFromString(url.toString)
//    val request = Request[IO](Method.POST, postUrl,
//      headers = Headers(Header.Raw(CaseInsensitiveString("api-key"), apiKey)))
//      .withBody(Items(items))
    val defaultHandling: PartialFunction[Response[String], IO[Unit]] = {
      case response if response.isSuccess => IO.unit
      case failedResponse =>
        //println(s"failed response is ${failedResponse.toString()}")
        IO.raiseError(onError(url, failedResponse))
    }
    val singlePost = sttp.header("Accept", "application/json")
      .header("api-key", apiKey)
      .parseResponseIf(_ => true)
      .body(Items(items))
      .post(url)
      .send()
      .flatMap(onResponse orElse defaultHandling)

    //val singlePost = httpClient.fetch(request) (onResponse orElse defaultHandling)
    retryWithBackoff(singlePost, 30.millis, maxRetries)
  }

  // scalastyle:off cyclomatic.complexity
  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int)
                                 (implicit timer: Timer[IO]): IO[A] = {
    ioa.handleErrorWith {
      case cdpError: CdpApiException =>
        if (shouldRetry(cdpError.code) && maxRetries > 0) {
          //println(s"retrying, cdp api exception caught, ${maxRetries} retries left")
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(cdpError)
        }
//      case malformedMessageBodyFailure:org.http4s.MalformedMessageBodyFailure =>
//        val failureBody = malformedMessageBodyFailure.toHttpResponse[IO](org.http4s.HttpVersion.`HTTP/1.1`)
//          .unsafeRunSync()
//          .bodyAsText
//          .compile.fold(List.empty[String]) { case (acc, str) => str :: acc }
//        println(s"Failed to parse ${failureBody}")
//        IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
      case exception:IOException =>
        if (maxRetries > 0) {
          //println(s"retrying, timeout exception or ioexception caught ${maxRetries} retries left")
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(exception)
        }
      case exception:TimeoutException =>
        if (maxRetries > 0) {
          //println(s"retrying, timeout exception or ioexception caught ${maxRetries} retries left")
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(exception)
        }
      case error =>
        if (maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
        } else {
          IO.raiseError(error)
        }
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
