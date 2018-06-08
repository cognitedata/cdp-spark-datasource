package com.cognite.spark.connector

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer
import scala.util.Try

case class TimeSeriesData(data: TimeSeriesDataItems[TimeSeriesItem])

case class TimeSeriesDataItems[A](items: Seq[A])

case class TimeSeriesItem(tagId: String, datapoints: Array[TimeSeriesDataPoint])

case class TimeSeriesLatestDataPoint(data: TimeSeriesDataItems[TimeSeriesDataPoint])

case class TimeSeriesDataPoint(timestamp: Long, value: Double) extends Serializable

object Limit extends Enumeration with Serializable {
  val Max, Min = Value
}

abstract class Limit extends Ordered[Limit] with Serializable {
  def value: Long

  override def compare(that: Limit): Int = this.value.compareTo(that.value)
}

case class Min(value: Long) extends Limit

case class Max(value: Long) extends Limit

class TimeSeriesRelation(apiKey: String,
                         project: String,
                         path: String,
                         suppliedSchema: StructType,
                         limit: Option[Int],
                         batchSizeOption: Option[Int])(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with PrunedFilteredScan
    with Serializable {
  @transient lazy val batchSize = batchSizeOption.getOrElse(10000)
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
        StructField("tagId", StringType),
        StructField("timestamp", LongType),
        StructField("value", DoubleType)))
    }
  }

  private def isTimestamp(s: String): Boolean = s.compareToIgnoreCase("timestamp") == 0

  def getTimestampLimit(filter: Filter): Seq[Limit] = {
    filter match {
      case LessThan(attribute, value) if isTimestamp(attribute) => Seq(Max(value.toString.toLong))
      case LessThanOrEqual(attribute, value) if isTimestamp(attribute) => Seq(Max(value.toString.toLong))
      case GreaterThan(attribute, value) if isTimestamp(attribute) => Seq(Min(value.toString.toLong))
      case GreaterThanOrEqual(attribute, value) if isTimestamp(attribute) => Seq(Min(value.toString.toLong))
      case And(f1, f2) => getTimestampLimit(f1) ++ getTimestampLimit(f2)
      // case Or(f1, f2) => we might possibly want to do something clever with joining an "or" clause
      //                    with timestamp limits on each side; just ignore them for now
      case _ => Seq.empty
    }
  }

  def getLatestDatapoint(): Option[TimeSeriesDataPoint] = {
    val url = new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegments("api/0.4/projects")
      .addPathSegment(project)
      .addPathSegments("timeseries/latest")
      .addPathSegment(path)
      .build()
    var response: Response = null
    try {
      response = client.newCall(TimeSeriesRelation.baseRequest(apiKey)
        .url(url)
        .build()).execute()
      if (!response.isSuccessful) {
        throw new RuntimeException("Non-200 status when querying API, received " + response.code() + "(" + response.message() + ")")
      }

      val r: TimeSeriesLatestDataPoint = mapper.readValue(response.body().string(), classOf[TimeSeriesLatestDataPoint])
      Option(r.data.items.head)
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val timestampLimits = filters.flatMap(getTimestampLimit)
    val timestampLowerLimit: Option[Long] = Try(timestampLimits.filter(_.isInstanceOf[Min]).min)
      .toOption
      .map(_.value)
    val timestampUpperLimit: Option[Long] = Try(timestampLimits.filter(_.isInstanceOf[Max]).max)
      .toOption
      .map(_.value)

    if (timestampLimits.exists(_.value < 0)) {
      sys.error("timestamp limits must be non-negative")
    }
    val maxTimestamp: Long = timestampUpperLimit match {
      case Some(i) => i
      case None => {
        getLatestDatapoint()
          .getOrElse(sys.error("Failed to get latest datapoint for " + path))
          .timestamp + 1
      }
    }

    val requiredColumnToIndex = Map("tagId" -> 0, "timestamp" -> 1, "value" -> 2)
    val requiredColumnIndexes = requiredColumns.map(requiredColumnToIndex)
    val responses: ListBuffer[Row] = ListBuffer()

    var nRowsRemaining: Option[Int] = limit
    var tag: TimeSeriesItem = TimeSeriesItem("", Array.empty)
    var next = timestampLowerLimit.getOrElse(maxTimestamp - 1000 * 60 * 60 * 24 * 14)
    do {
      val thisBatchSize = scala.math.min(nRowsRemaining.getOrElse(batchSize), batchSize)
      tag = getTag(Some(next), Some(maxTimestamp), thisBatchSize)
      for (datapoint <- tag.datapoints) {
        val columns: ListBuffer[Any] = ListBuffer()
        for (index <- requiredColumnIndexes) {
          index match {
            case 0 => columns += tag.tagId
            case 1 => columns += datapoint.timestamp
            case 2 => columns += datapoint.value
            case _ => sys.error("Invalid required column index " + index.toString)
          }
        }
        responses += Row.fromSeq(columns)
      }
      if (tag.datapoints.nonEmpty) {
        next = tag.datapoints.last.timestamp + 1
      }
      nRowsRemaining = nRowsRemaining.map(_ - tag.datapoints.size)
    } while (tag.datapoints.nonEmpty && (nRowsRemaining.isEmpty || nRowsRemaining.get > 0) && (next < maxTimestamp))
    sqlContext.sparkContext.parallelize(responses)
  }

  // Should be rewritten to use async queries
  def getTag(start: Option[Long], stop: Option[Long], limit: Int): TimeSeriesItem = {
    val url = TimeSeriesRelation.baseTimeSeriesURL(project, start, stop)
      .addPathSegment(path)
      .addQueryParameter("limit", limit.toString)
      .build()
    val response = client.newCall(TimeSeriesRelation.baseRequest(apiKey)
      .url(url)
      .build()).execute()
    if (!response.isSuccessful) {
      throw new RuntimeException("Non-200 status when querying API, received " + response.code() + "(" + response.message() + ")")
    }
    parseResult(response)
  }

  def parseResult(response: Response): TimeSeriesItem = {
    try {
      val json = response.body().string()
      val js = mapper.readValue(json, classOf[TimeSeriesData])
      js.data.items.head
    } finally {
      response.close()
    }
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      rows.grouped(batchSize).foreach(postRows)
    })
  }

  private def postRows(rows: Seq[Row]) = {
    val dataPointsByTagId = rows.groupBy(r => r.getAs[String](0))
      .mapValues(rs => rs.map(r => TimeSeriesDataPoint(r.getLong(1), r.getDouble(2))))
    for ((tagId, dataPoints) <- dataPointsByTagId) {
      postTimeSeries(tagId, TimeSeriesDataItems[TimeSeriesDataPoint](dataPoints))
    }
  }

  private def postTimeSeries(tagId: String, items: TimeSeriesDataItems[TimeSeriesDataPoint]) = {
    val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
    val requestBody = RequestBody.create(jsonMediaType, mapper.writeValueAsString(items))
    println("post to " + TimeSeriesRelation.baseTimeSeriesURL(project)
      .addPathSegment(tagId)
      .build())
    var response: Response = null
    try {
      response = client.newCall(
        TimeSeriesRelation.baseRequest(apiKey)
          .url(TimeSeriesRelation.baseTimeSeriesURL(project)
            .addPathSegment(tagId)
            .build())
          .post(requestBody)
          .build()
      ).execute()
      if (!response.isSuccessful) {
        throw new RuntimeException("Non-200 status when posting to raw API, received " + response.code() + "(" + response.message() + ")")
      }
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }
}

object TimeSeriesRelation {
  def baseTimeSeriesURL(project: String, start: Option[Long] = None, stop: Option[Long] = None): HttpUrl.Builder = {
    val builder = new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegments("api/0.4/projects")
      .addPathSegment(project)
      .addPathSegments("timeseries/data")
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
