package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.codahale.metrics.Counter
import io.circe.generic.auto._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._

import scala.concurrent.ExecutionContext
import scala.util.Try
import com.cognite.data.api.v2.DataPoints._
import com.cognite.spark.datasource.Auth._

sealed case class DataPointsDataItems[A](items: Seq[A])

sealed case class DataPointsItem(name: String, datapoints: Array[DataPoint])

sealed case class LatestDataPoint(data: DataPointsDataItems[DataPoint])

sealed case class DataPoint(
    timestamp: Long,
    value: Option[Double],
    average: Option[Double],
    max: Option[Double],
    min: Option[Double],
    count: Option[Double],
    sum: Option[Double],
    stepInterpolation: Option[Double],
    continuousVariance: Option[Double],
    discreteVariance: Option[Double],
    totalVariation: Option[Double])
    extends Serializable

sealed case class DataPointsTimestampItem(name: String, datapoints: Array[DataPointTimestamp])

sealed case class DataPointTimestamp(timestamp: Long)

object Limit extends Enumeration with Serializable {
  val Max, Min = Value
}

abstract class Limit extends Ordered[Limit] with Serializable {
  def value: Long

  override def compare(that: Limit): Int = this.value.compareTo(that.value)
}

sealed case class Min(value: Long) extends Limit

sealed case class Max(value: Long) extends Limit

// Note: using a case class is overkill right now, but will be necessary when/if we want to support
// starts with, ends with, contains, etc.
sealed case class NameFilter(name: String)

abstract class DataPointsRelation(
    config: RelationConfig,
    numPartitions: Int,
    suppliedSchema: Option[StructType])(val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation
    with TableScan
    with PrunedFilteredScan
    with CdpConnector
    with Serializable {
  import CdpConnector._
  @transient lazy private val maxRetries = config.maxRetries

  val datapointsCreated: Counter
  val datapointsRead: Counter

  def getTimestampLimit(filter: Filter): Seq[Limit] =
    filter match {
      case LessThan("timestamp", value) => Seq(Max(value.toString.toLong))
      case LessThanOrEqual("timestamp", value) => Seq(Max(value.toString.toLong))
      case GreaterThan("timestamp", value) => Seq(Min(value.toString.toLong))
      case GreaterThanOrEqual("timestamp", value) => Seq(Min(value.toString.toLong))
      case And(f1, f2) => getTimestampLimit(f1) ++ getTimestampLimit(f2)
      // case Or(f1, f2) => we might possibly want to do something clever with joining an "or" clause
      //                    with timestamp limits on each side; just ignore them for now
      case _ => Seq.empty
    }

  // scalastyle:off cyclomatic.complexity
  def getNameFilters(filter: Filter): Seq[NameFilter] =
    filter match {
      case IsNotNull("name") => Seq()
      case EqualTo("name", value) => Seq(NameFilter(value.toString))
      case EqualNullSafe("name", value) => Seq(NameFilter(value.toString))
      case In("name", values) => values.map(v => NameFilter(v.toString))
      case And(f1, f2) => getNameFilters(f1) ++ getNameFilters(f2)
      case Or(f1, f2) => getNameFilters(f1) ++ getNameFilters(f2)
      case StringStartsWith("name", value) =>
        // TODO: add support for this using the "q" parameter when listing time series
        sys.error(
          s"Filtering using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("name", value) =>
        // TODO: add support for this using the time series search endpoint
        sys.error(
          s"Filtering using 'string ends with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("name", value) =>
        // TODO: add support using the time series search endpoint
        sys.error(
          s"Filtering using 'string contains' not allowed for data points, attempted for ${value.toString}")
      case _ =>
        Seq()
    }
  // scalastyle:on cyclomatic.complexity

  private def getLatestDataPointTimestamp(timeSeriesName: String): Long = {
    val url =
      uri"${config.baseUrl}/api/0.5/projects/${config.project}/timeseries/latest/$timeSeriesName"
    val getLatest = sttp
      .header("Accept", "application/json")
      .auth(config.auth)
      .response(asJson[LatestDataPoint])
      .get(url)
      .send()
    retryWithBackoff(getLatest, Constants.DefaultInitialRetryDelay, maxRetries)
      .unsafeRunSync()
      .unsafeBody
      .toOption
      .flatMap(_.data.items.headOption)
      .map(_.timestamp)
      .getOrElse(System.currentTimeMillis())
  }

  private def getFirstDataPointTimestamp(timeSeriesName: String): Long =
    getJson[DataItemsWithCursor[DataPointsTimestampItem]](
      config.auth,
      uri"${baseDataPointsUrl(config.project)}/$timeSeriesName"
        .param("limit", "1")
        .param("start", "0"),
      config.maxRetries)
      .unsafeRunSync()
      .data
      .items
      .headOption
      .map(_.datapoints.headOption.map(_.timestamp).getOrElse(0L))
      .getOrElse(0L)

  def getTimestampLimits(
      timeSeriesNames: Seq[String],
      hardLimits: (Option[Long], Option[Long])): Map[String, (Long, Long)] =
    timeSeriesNames.map { name =>
      name -> (
        hardLimits._1.getOrElse(getFirstDataPointTimestamp(name)),
        hardLimits._2.getOrElse(getLatestDataPointTimestamp(name))
      )
    }.toMap

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  def filtersToTimestampLimits(filters: Array[Filter]): (Option[Long], Option[Long]) = {
    val timestampLimits = filters.flatMap(getTimestampLimit)

    if (timestampLimits.exists(_.value < 0)) {
      sys.error("timestamp limits must be non-negative")
    }

    Tuple2(
      Try(timestampLimits.filter(_.isInstanceOf[Min]).min).toOption.map(_.value),
      Try(timestampLimits.filter(_.isInstanceOf[Max]).max).toOption.map(_.value))
  }

  def postTimeSeries(data: MultiNamedTimeseriesData): IO[Unit] = {
    val url = uri"${baseDataPointsUrl(config.project)}"
    val postDataPoints = sttp
      .header("Accept", "application/json")
      .auth(config.auth)
      .parseResponseIf(_ => true)
      .contentType("application/protobuf")
      .body(data.toByteArray)
      .post(url)
      .send()
      .flatMap(defaultHandling(url))
    retryWithBackoff(postDataPoints, Constants.DefaultInitialRetryDelay, maxRetries)
      .map(r => {
        if (config.collectMetrics) {
          val numPoints = data.namedTimeseriesData
            .map(
              ts =>
                ts.data.numericData
                  .map(_.points.length)
                  .getOrElse(0) + ts.data.stringData.map(_.points.length).getOrElse(0))
            .sum
          datapointsCreated.inc(numPoints)
        }
        r
      })
  }

  def baseDataPointsUrl(project: String): Uri =
    uri"${baseUrl(project, "0.5", Constants.DefaultBaseUrl)}/timeseries/data"
}
