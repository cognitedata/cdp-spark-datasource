package com.cognite.spark.datasource

import java.util.concurrent.Executors

import cats.effect.{IO, Timer}
import cats.implicits._
import io.circe.generic.auto._
import com.cognite.data.api.v1.{NumericDatapoint, NumericTimeseriesData, TimeseriesData}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.concurrent.duration._

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
                         suppliedSchema: Option[StructType],
                         limit: Option[Int],
                         batchSizeOption: Option[Int],
                         metricsPrefix: String,
                         collectMetrics: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with PrunedFilteredScan
    with CdpConnector
    with Serializable {

  private val maxRetries = 10
  @transient lazy val batchSize = batchSizeOption.getOrElse(10000)
  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  lazy private val datapointsCreated = UserMetricsSystem.counter(s"${metricsPrefix}datapoints.created")
  lazy private val datapointsRead = UserMetricsSystem.counter(s"${metricsPrefix}datapoints.read")

  override def schema: StructType = {
    suppliedSchema.getOrElse(StructType(Seq(
      StructField("tagId", StringType),
      StructField("timestamp", LongType),
      StructField("value", DoubleType))))
  }

  private def isTimestamp(s: String): Boolean = s.compareToIgnoreCase("timestamp") == 0

  // scalastyle:off cyclomatic.complexity
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
  // scalastyle:on cyclomatic.complexity

  implicit val backend = sttpBackend
  def getLatestDatapoint: Option[TimeSeriesDataPoint] = {
    val url = uri"https://api.cognitedata.com/api/0.5/projects/$project/timeseries/latest/$path"
    get[TimeSeriesDataPoint](apiKey, url, 10, None, maxRetries)
      .toStream
      .headOption
  }

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  private val requiredColumnToIndex = Map("tagId" -> 0, "timestamp" -> 1, "value" -> 2)
  private def toColumns(requiredColumns: Array[String], dataPoint: NumericDatapoint): Seq[Option[Any]] = {
    val requiredColumnIndexes = requiredColumns.map(requiredColumnToIndex)
    for (index <- requiredColumnIndexes)
      yield index match {
        case 0 => Some(path)
        case 1 => Some(dataPoint.getTimestamp)
        case 2 => Some(dataPoint.getValue)
        case _ =>
          sys.error("Invalid required column index " + index.toString)
          None
      }
  }

  private def getRows(minTimestamp: Long, maxTimestamp: Long, requiredColumns: Array[String]) = {
    Batch.withCursor(batchSize, limit) { (thisBatchSize, cursor: Option[Long]) =>
      val tags = getTag(Some(cursor.getOrElse(minTimestamp)), Some(maxTimestamp), thisBatchSize)
      val rows = for (dataPoint <- tags)
        yield Row.fromSeq(toColumns(requiredColumns, dataPoint).flatten)
      if (collectMetrics) {
        datapointsRead.inc(rows.length)
      }
      (rows, tags.lastOption.map(_.getTimestamp + 1))
    }
  }

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
      case None =>
        getLatestDatapoint
          .getOrElse(sys.error("Failed to get latest datapoint for " + path))
          .timestamp + 1
    }
    val finalRows = getRows(timestampLowerLimit.getOrElse(maxTimestamp - 1000 * 60 * 60 * 24 * 14), maxTimestamp, requiredColumns)
      .toList
    sqlContext.sparkContext.parallelize(finalRows)
  }

  def onProtobufError(url: Uri): PartialFunction[Response[Array[Byte]], IO[Array[Byte]]] = {
    case Response(Right(data), _, _, _, _) => IO.pure(data)
    case Response(Left(bytes), statusCode, _, _, _) => parseCdpApiError(new String(bytes, "utf-8"), url, statusCode)
  }

  def getTag(start: Option[Long], end: Option[Long], limit: Int): Seq[NumericDatapoint] = {
    (start, end) match {
      case(Some(startTime), Some(endTime)) if startTime >= endTime =>
          Seq()
      case _ =>
        val url = uri"${baseTimeSeriesURL(project)}/$path?limit=$limit&start=$start&end=$end"
        val get = sttp.header("Accept", "application/protobuf")
          .header("api-key", apiKey)
          .parseResponseIf(_ => true)
          .response(asByteArray)
          .get(url)
          .send()
          .flatMap(r => onProtobufError(url)(r))
          .map(parseResult)
        retryWithBackoff(get, 30.millis, maxRetries).unsafeRunSync()
    }
  }

  def parseResult(body: Array[Byte]): Seq[NumericDatapoint] = {
    val tsData = TimeseriesData.parseFrom(body)
    //TODO: handle string timeseries
    if (tsData.hasNumericData) tsData.getNumericData.getPointsList.asScala else Seq.empty
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      rows.grouped(batchSize).foreach(postRows)
    })
  }

  @transient private implicit val contextShift = IO.contextShift(ExecutionContext.global)
  private def postRows(rows: Seq[Row]): Unit = {
    val tsDataByTagId = rows.groupBy(r => r.getAs[String](0))
      .mapValues(rs =>
        TimeseriesData.newBuilder()
          .setNumericData(
            NumericTimeseriesData.newBuilder()
              .addAllPoints(rs.map(r =>
                NumericDatapoint.newBuilder()
                  .setTimestamp(r.getLong(1))
                  .setValue(r.getDouble(2))
                  .build()).asJava)
              .build())
          .build()).toVector
    tsDataByTagId.parTraverse(t => postTimeSeries(t._1, t._2)).unsafeRunSync()
  }

  private def postTimeSeries(tagId: String, data: TimeseriesData): IO[Unit] = {
    val url = uri"${baseTimeSeriesURL(project)}/$tagId"
    val postDataPoints = sttp.header("Accept", "application/protobuf")
      .header("api-key", apiKey)
      .parseResponseIf(_ => true)
      .contentType("application/protobuf")
      .body(data.toByteArray)
      .response(asByteArray)
      .post(url)
      .send()
      .flatMap(r => onProtobufError(url)(r))
    retryWithBackoff(postDataPoints, 30.millis, maxRetries)
      .map(r => {
        if (collectMetrics) {
          datapointsCreated.inc(data.getNumericData.getPointsCount)
        }
        r
      }).flatMap(_ => IO.unit)
  }

  def baseTimeSeriesURL(project: String): Uri = {
    uri"${baseUrl(project, "0.5")}/timeseries/data"
  }
}
