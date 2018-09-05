package com.cognite.spark.connector

import okhttp3._
import java.util.concurrent.TimeUnit

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

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

  def get[A](apiKey: String, url: HttpUrl, batchSize: Int, limit: Option[Int],
             batchCompletedCallback: Option[DataItemsWithCursor[A] => Unit] = None)
            (implicit decoder: Decoder[A]): Iterator[A] = {
    Batch.withCursor(batchSize, limit) { (chunkSize, cursor: Option[String]) =>
      val nextUrl = url.newBuilder().addQueryParameter("limit", chunkSize.toString)
      cursor.foreach(cur => nextUrl.addQueryParameter("cursor", cur))
      val requestBuilder = CdpConnector.baseRequest(apiKey)
      val response = client.newCall(requestBuilder.url(nextUrl.build()).build()).execute()
      try {
        if (!response.isSuccessful) {
          throw new RuntimeException("Non-200 status when querying API")
        }

        val d = response.body().string()
        decode[DataItemsWithCursor[A]](d) match {
          case Right(r) => {
            batchCompletedCallback.foreach(callback => callback(r))
            (r.data.items, r.data.nextCursor)
          }
          case Left(e) => throw new RuntimeException("Failed to deserialize", e)
        }
      } finally {
        response.close()
      }
    }
  }

  def post[A](apiKey: String, url: HttpUrl, items: Seq[A])(implicit encoder : Encoder[A]): Unit = {
    val dataItems = Items(items)
    val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
    val requestBody = RequestBody.create(jsonMediaType, dataItems.asJson.noSpaces)

    val response = client.newCall(
      CdpConnector.baseRequest(apiKey)
        .url(url)
        .post(requestBody)
        .build()
    ).execute()

    if (!response.isSuccessful) {
      throw new RuntimeException(s"Non-200 status when posting to $url, received ${response.code()} (${response.message()}).")
    }
  }
}
