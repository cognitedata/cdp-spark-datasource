package com.cognite.spark.connector

import java.io.IOException

import okhttp3._
import java.util.concurrent.TimeUnit

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.util.Random

case class Data[A](data: A)
case class ItemsWithCursor[A](items: Seq[A], nextCursor: Option[String] = None)
case class Items[A](items: Seq[A])

object CdpConnector {
  type DataItemsWithCursor[A] = Data[ItemsWithCursor[A]]

  def baseUrl(project: String, version: String = "0.5"): HttpUrl.Builder = {
    new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegment("api")
      .addPathSegment(version)
      .addPathSegment("projects")
      .addPathSegment(project)
  }

  def baseRequest(apiKey: String): Request.Builder = {
    new Request.Builder()
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Accept-Charset", "utf-8")
      .header("api-key", apiKey)
  }

  @transient lazy val client: OkHttpClient = new OkHttpClient.Builder()
    .readTimeout(2, TimeUnit.MINUTES)
    .writeTimeout(2, TimeUnit.MINUTES)
    .build()

  // exponential backoff as described here:
  // https://developers.google.com/api-client-library/java/google-http-java-client/backoff
  private val randomizationFactor = 0.5
  private val retryMultiplier = 1.5

  private val random = new Random()
  private def exponentialBackoffSleep(interval: Double) =
    Thread.sleep(1000 * (interval * (1.0 + (random.nextDouble() - 0.5) * 2.0 * randomizationFactor)).toLong)

  def callWithRetries(call: Call, maxRetries: Int): Response = {
    var callAttempt = 0
    var response = call.execute()
    var retryInterval = 0.5
    while (callAttempt < maxRetries && isServerError(response)) {
      exponentialBackoffSleep(retryInterval)
      retryInterval = retryInterval * retryMultiplier
      response = call.execute()
      callAttempt += 1
    }
    response
  }

  def get[A](apiKey: String, url: HttpUrl, batchSize: Int, limit: Option[Int],
             batchCompletedCallback: Option[DataItemsWithCursor[A] => Unit] = None, maxRetries: Int = 5)
            (implicit decoder: Decoder[A]): Iterator[A] = {
    Batch.withCursor(batchSize, limit) { (chunkSize, cursor: Option[String]) =>
      val nextUrl = url.newBuilder().addQueryParameter("limit", chunkSize.toString)
      cursor.foreach(cur => nextUrl.addQueryParameter("cursor", cur))
      val requestBuilder = CdpConnector.baseRequest(apiKey)
      val response = callWithRetries(client.newCall(requestBuilder.url(nextUrl.build()).build()), maxRetries)
      if (!response.isSuccessful) {
        reportResponseFailure(url, s"received ${response.code()} (${response.message()})")
      }
      try {
        val d = response.body().string()
        decode[DataItemsWithCursor[A]](d) match {
          case Right(r) =>
            for (callback <- batchCompletedCallback) {
              callback(r)
            }
            (r.data.items, r.data.nextCursor)
          case Left(e) => throw new RuntimeException("Failed to deserialize", e)
        }
      } finally {
        response.close()
      }
    }
  }

  def reportResponseFailure(url: HttpUrl, reason: String, method: String = "GET") = {
    throw new RuntimeException(s"Non-200 status response to $method $url, $reason.")
  }

  private def isServerError(response: Response): Boolean = 500 until 600 contains response.code()

  def post[A](apiKey: String, url: HttpUrl, items: Seq[A], wantAsync: Boolean = false, maxRetries: Int = 5,
              retryInterval: Double = 0.5, successCallback: Option[Response => Unit] = None,
              failureCallback: Option[Call => Unit] = None)
             (implicit encoder : Encoder[A]): Unit = {
    val dataItems = Items(items)
    val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
    val requestBody = RequestBody.create(jsonMediaType, dataItems.asJson.noSpaces)

    val call = client.newCall(
      CdpConnector.baseRequest(apiKey)
        .url(url)
        .post(requestBody)
        .build()
    )

    if (wantAsync) {
      call.enqueue(new Callback {
        override def onFailure(call: Call, e: IOException): Unit = {
          if (maxRetries > 0) {
            exponentialBackoffSleep(retryInterval)
            post(apiKey, url, items, wantAsync, maxRetries - 1, retryInterval * retryMultiplier, successCallback)
          } else {
            for (callback <- failureCallback) {
              callback(call)
            }
            reportResponseFailure(url, e.getCause.getMessage, "POST")
          }
        }

        override def onResponse(call: Call, response: Response): Unit = {
          try {
            for (callback <- successCallback) {
              callback(response)
            }
          } finally {
            response.close()
          }
        }
      })
    } else {
      val response = callWithRetries(call, maxRetries)
      try {
        if (!response.isSuccessful) {
          reportResponseFailure(url, s"received ${response.code()} (${response.message()})", "POST")
        } else {
          for (callback <- successCallback) {
            callback(response)
          }
        }
      } finally {
        response.close()
      }
    }
  }
}
