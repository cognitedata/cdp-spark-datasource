package com.cognite.spark.connector

import okhttp3.{HttpUrl, OkHttpClient, Request, Response}
import org.codehaus.jackson.annotate.JsonIgnoreProperties

import scala.collection.mutable.ListBuffer
// import com.squareup.okhttp.{HttpUrl, OkHttpClient, Request}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Data(data: DataItems)
@JsonIgnoreProperties(ignoreUnknown = true)
case class DataItems(items: Array[Item])
@JsonIgnoreProperties(ignoreUnknown = true)
case class Item(tagId: String, datapoints: Array[DataPoint])
@JsonIgnoreProperties(ignoreUnknown = true)
case class DataPoint(timestamp: Long, value: Double)

/**
  * Inital extremely MVP: The full scan relation does not use pushdown, but instead bases limiting start/stop on
  * options passed to the read-call. This means we won't inherit .where() options etc.
  *
  * Question: Should we support both .where() and start/stop even in the pushdown variant?
  */
class FullScanRelations(apiKey: String,
                        project: String,
                        path: String,
                        suppliedSchema: StructType,
                        batchSize: Int,
                        start: Option[Long],
                        stop: Option[Long])(@transient val sqlContext: SQLContext) extends BaseRelation
  with TableScan {
  @transient lazy val client: OkHttpClient = new OkHttpClient()
  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  override def schema: StructType = {
    if (suppliedSchema != null) {
      suppliedSchema
    } else {
      StructType(Seq(
        StructField("tagId", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("value", DoubleType, nullable = false)))
    }
  }

  override def buildScan(): RDD[Row] = {
    val last = stop.orElse(Some(System.currentTimeMillis()))

    var tag: Item = getTag(start, last)
    val responses: ListBuffer[Item] = ListBuffer(tag)
    var next: Long = tag.datapoints.last.timestamp + 1

    while (batchSize <= tag.datapoints.length && next < last.get) {
      tag = getTag(Some(next), last)
      responses += tag
      next = tag.datapoints.last.timestamp + 1
    }
    sqlContext.sparkContext.parallelize(responses.flatMap(item => item.datapoints).map(d => Row(tag.tagId, d.timestamp, d.value: Double)))
  }

  // Should be rewritten to use async queries
  def getTag(start: Option[Long], stop: Option[Long]): Item = {
    val response = client.newCall(FullScanRelations.baseRequest(apiKey)
      .url(FullScanRelations.baseTimeSeriesURL(project, batchSize, start, stop)
        .addPathSegment(path)
        .build())
      .build()).execute()
    if (!response.isSuccessful) {
      throw new RuntimeException("Non-200 status when querying API, received " + response.code() + "(" + response.message() + ")")
    }
    parseResult(response)
  }

  def parseResult(response: Response): Item = {
    val json = response.body().string()
    // System.err.println(json)
    val js = mapper.readValue(json, classOf[Data])
    js.data.items.head
  }

}

object FullScanRelations {
  def baseTimeSeriesURL(project: String, batchSize: Int = 100, start: Option[Long] = None, stop: Option[Long] = None): HttpUrl.Builder = {
    val builder = new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegments("api/0.4/projects")
      .addPathSegment(project)
      .addPathSegments("timeseries/data")
    builder.addQueryParameter("limit", batchSize.toString)
    start.map(q => builder.addQueryParameter("start", q.toString))
    stop.map(q => builder.addQueryParameter("end", q.toString))
    builder
  }

  def baseRequest(apiKey: String): Request.Builder = {
    new Request.Builder()
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Accept-Charset", "utf-8")
      .header("Api-Key", apiKey)
  }
}
