package com.cognite.spark.connector

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import okhttp3.{HttpUrl, OkHttpClient, Request}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

case class RawData(data: RawDataItems)

case class RawDataItems(items: Array[RawDataItemsItem], nextCursor: Option[String] = None)

case class RawDataItemsItem(key: String, columns: Map[String, Any])

class RawTableRelation(apiKey: String,
                       project: String,
                       database: String,
                       table: String,
                       userSchema: Option[StructType],
                       batchSizeOption: Option[Int])(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan {

  val DEFAULT_BATCH_SIZE = 10000

  // TODO: make read/write timeouts configurable
  @transient lazy val client: OkHttpClient = new OkHttpClient.Builder()
    .readTimeout(2, TimeUnit.MINUTES)
    .build()
  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }
  val batchSize = batchSizeOption match {
    case Some(size) => size
    case None => DEFAULT_BATCH_SIZE
  }

  override def schema: StructType = userSchema.getOrElse[StructType] {
    StructType(Seq(
      StructField("key", DataTypes.StringType),
      StructField("columns", DataTypes.StringType)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val responses: ListBuffer[Row] = ListBuffer()
    var doneReading = false
    var cursor: Option[String] = None

    do {
      val urlBuilder = RawTableRelation.baseRawTableURL(project, database, table)
        .addQueryParameter("limit", batchSize.toString)
      if (!cursor.isEmpty) {
        urlBuilder.addQueryParameter("cursor", cursor.get)
      }

      val response = client.newCall(
        RawTableRelation.baseRequest(apiKey)
          .url(urlBuilder.build())
          .build()
      ).execute()
      if (!response.isSuccessful) {
        throw new RuntimeException("Non-200 status when querying API, received " + response.code() + "(" + response.message() + ")")
      }
      val r = mapper.readValue(response.body().string(), classOf[RawData])
      for (item <- r.data.items) {
        responses += Row(item.key, mapper.writeValueAsString(item.columns))
      }

      cursor = r.data.nextCursor
    } while (!cursor.isEmpty)

    sqlContext.sparkContext.parallelize(responses)
  }
}

object RawTableRelation {
  def baseRawTableURL(project: String, database: String, table: String): HttpUrl.Builder = {
    new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegments("api/0.4/projects")
      .addPathSegment(project)
      .addPathSegment("raw")
      .addPathSegment(database)
      .addPathSegment(table)
  }

  def baseRequest(apiKey: String): Request.Builder = {
    new Request.Builder()
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Accept-Charset", "utf-8")
      .header("Api-Key", apiKey)
  }
}
