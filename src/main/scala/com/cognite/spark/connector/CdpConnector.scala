package com.cognite.spark.connector

import okhttp3._
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Data[A](data: DataItemsWithCursor[A])
case class DataItemsWithCursor[A](items: Seq[A], nextCursor: Option[String] = None)
case class DataItems[A](items: Seq[A])

object CdpConnector {
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

  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }

  def get[A](apiKey: String, url: HttpUrl, batchSize: Int, limit: Option[Int]): Iterator[A] = {
    Batch.withCursor(batchSize, limit) { (chunkSize, cursor: Option[String]) =>
      val nextUrl = url.newBuilder().addQueryParameter("limit", chunkSize.toString)
      cursor.foreach(cur => nextUrl.addQueryParameter("cursor", cur))
      val requestBuilder = CdpConnector.baseRequest(apiKey)
      val response = client.newCall(requestBuilder.url(nextUrl.build()).build()).execute()
      try {
        if (!response.isSuccessful) {
          throw new RuntimeException("Non-200 status when querying API")
        }
        val r = mapper.readValue(response.body().string(), classOf[Data[A]])
        (r.data.items, r.data.nextCursor)
      } finally {
        response.close()
      }
    }
  }

  def post[A](apiKey: String, url: HttpUrl, items: Seq[A]): Unit = {
    val dataItems = DataItems(items)
    val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
    val requestBody = RequestBody.create(jsonMediaType, mapper.writeValueAsString(dataItems))

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