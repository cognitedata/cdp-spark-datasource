package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import io.circe.generic.auto._
import com.cognite.data.api.v1.{NumericDatapoint, TimeseriesData}
import com.cognite.data.api.v2.{MultiNamedTimeseriesData, NamedTimeseriesData, NumericTimeseriesData, NumericDatapoint => NumericDataPointV2}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.concurrent.duration._

sealed case class DataPointsDataItems[A](items: Seq[A])

sealed case class DataPointsItem(name: String, datapoints: Array[DataPoint])

sealed case class LatestDataPoint(data: DataPointsDataItems[DataPoint])

sealed case class DataPoint(timestamp: Long,
                            value: Option[Double],
                            average: Option[Double],
                            max: Option[Double],
                            min: Option[Double],
                            count: Option[Double],
                            sum: Option[Double],
                            stepInterpolation: Option[Double],
                            continuousVariance: Option[Double],
                            discreteVariance: Option[Double],
                            totalVariation: Option[Double]) extends Serializable

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

// TODO: make case classes / enums for each valid granularity
sealed case class GranularityFilter(amount: Option[Long], unit: String)

// TODO: make case classes / enums for each valid aggregation
sealed case class AggregationFilter(aggregation: String)

class DataPointsRelation(apiKey: String,
                         project: String,
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
  import CdpConnector._

  @transient lazy val batchSize = batchSizeOption.getOrElse(100000)

  @transient lazy val datapointsCreated = UserMetricsSystem.counter(s"${metricsPrefix}datapoints.created")
  @transient lazy val datapointsRead = UserMetricsSystem.counter(s"${metricsPrefix}datapoints.read")

  override def schema: StructType = {
    suppliedSchema.getOrElse(StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("value", DoubleType, nullable = false),
      StructField("aggregation", StringType, nullable = true),
      StructField("granularity", StringType, nullable = true))))
  }

  // scalastyle:off cyclomatic.complexity
  def getTimestampLimit(filter: Filter): Seq[Limit] = {
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
  }

  def toAggregationFilter(aggregation: String): AggregationFilter = {
    // 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step, continuousvariance/cv, discretevariance/dv, totalvariation/tv'
    aggregation match {
      case "average" => AggregationFilter("average")
      case "avg" => AggregationFilter("avg")
      case "max" => AggregationFilter("max")
      case "min" => AggregationFilter("min")
      case "count" => AggregationFilter("count")
      case "sum" => AggregationFilter("sum")
      case "stepinterpolation" => AggregationFilter("stepinterpolation")
      case "step" => AggregationFilter("step")
      case "continuousvariance" => AggregationFilter("continuousvariance")
      case "cv" => AggregationFilter("cv")
      case "discretevariance" => AggregationFilter("discretevariance")
      case "dv" => AggregationFilter("dv")
      case "totalvariation" => AggregationFilter("totalvariation")
      case "tv" => AggregationFilter("tv")
      case _ => sys.error(s"Invalid aggregation $aggregation")
    }
  }

  def getAggregation(filter: Filter): Seq[AggregationFilter] = {
    filter match {
      case IsNotNull("aggregation") => Seq()
      case EqualTo("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case EqualNullSafe("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case In("aggregation", values) =>
        values.map(v => toAggregationFilter(v.toString))
      case And(_, _) => sys.error("AND is not allowed for granularity")
      case Or(f1, f2) => getAggregation(f1) ++ getAggregation(f2)
      case StringStartsWith("name", value) =>
        sys.error(s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("name", value) =>
        sys.error(s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("name", value) =>
        sys.error(s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
    }
  }

  def getNameFilters(filter: Filter): Seq[NameFilter] = {
    filter match {
      case IsNotNull("name") => Seq()
      case EqualTo("name", value) => Seq(NameFilter(value.toString))
      case EqualNullSafe("name", value) => Seq(NameFilter(value.toString))
      case In("name", values) => values.map(v => NameFilter(v.toString))
      case And(f1, f2) => getNameFilters(f1) ++ getNameFilters(f2)
      case Or(f1, f2) => getNameFilters(f1) ++ getNameFilters(f2)
      case StringStartsWith("name", value) =>
        // TODO: add support for this using the "q" parameter when listing time series
        sys.error(s"Filtering using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("name", value) =>
        // TODO: add support for this using the timeseries search endpoint
        sys.error(s"Filtering using 'string ends with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("name", value) =>
        // TODO: add support using the timeseries search endpoint
        sys.error(s"Filtering using 'string contains' not allowed for data points, attempted for ${value.toString}")
      case _ =>
        Seq()
    }
  }

  def toGranularityFilter(granularity: String): GranularityFilter = {
    // day/d, hour/h, minute/m, second/s
    val granularityPattern = raw"(\d+)?(day|d|hour|h|minute|m|second|s)".r
    granularity match {
      case granularityPattern(null, unit) => GranularityFilter(None, unit) // scalastyle:ignore null
      case granularityPattern(amount, unit) => GranularityFilter(Some(amount.toInt), unit)
      case _ => sys.error(s"Invalid granularity $granularity")
    }
  }

  def getGranularity(filter: Filter): Seq[GranularityFilter] = {
    filter match {
      case IsNotNull("granularity") => Seq()
      case EqualTo("granularity", value) => Seq(toGranularityFilter(value.toString))
      case EqualNullSafe("granularity", value) => Seq(toGranularityFilter(value.toString))
      case In("granularity", values) =>
        values.map(v => toGranularityFilter(v.toString))
      case And(_, _) => sys.error("AND is not allowed for granularity")
      case Or(f1, f2) => getGranularity(f1) ++ getGranularity(f2)
      case StringStartsWith("name", value) =>
        sys.error(s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("name", value) =>
        sys.error(s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("name", value) =>
        sys.error(s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
    }
  }

  def getLatestDataPoint(timeSeriesName: String): Option[DataPoint] = {
    val url = uri"https://api.cognitedata.com/api/0.5/projects/$project/timeseries/latest/$timeSeriesName"
    val getLatest = sttp.header("Accept", "application/json")
      .header("api-key", apiKey)
      .response(asJson[LatestDataPoint])
      .get(url)
      .send()
    retryWithBackoff(getLatest, 30.millis, maxRetries)
      .unsafeRunSync()
      .unsafeBody
      .toOption
      .flatMap(_.data.items.headOption)
  }

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  private val requiredColumnToIndex = Map("name" -> 0, "timestamp" -> 1, "value" -> 2, "aggregation" -> 3, "granularity" -> 4)
  private def toColumns(name: String, aggregation: Option[String], granularity: Option[String],
                        requiredColumns: Array[String], dataPoint: NumericDatapoint): Seq[Option[Any]] = {
    val requiredColumnIndexes = requiredColumns.map(requiredColumnToIndex)
    for (index <- requiredColumnIndexes)
      yield index match {
        case 0 => Some(name)
        case 1 => Some(dataPoint.getTimestamp)
        case 2 => Some(dataPoint.getValue)
        case 3 => aggregation
        case 4 => granularity
        case _ =>
          sys.error("Invalid required column index " + index.toString)
          None
      }
  }

  private def toRow(name: String, aggregation: Option[String], granularity: Option[String],
                    requiredColumns: Array[String])(dataPoint: NumericDatapoint): Row = {
    Row.fromSeq(toColumns(name, aggregation, granularity, requiredColumns, dataPoint))
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val timestampLimits = filters.flatMap(getTimestampLimit)
    val timestampLowerLimit: Option[Long] = Try(timestampLimits.filter(_.isInstanceOf[Min]).min)
      .toOption
      .map(_.value)
    val timestampUpperLimit: Option[Long] = Try(timestampLimits.filter(_.isInstanceOf[Max]).max)
      .toOption
      .map(_.value)
    val aggregations = filters.flatMap(getAggregation).map(_.some).distinct
    val granularities = filters.flatMap(getGranularity).map(_.some).distinct

    if (aggregations.nonEmpty && granularities.isEmpty) {
      sys.error(s"Aggregations requested but granularity is not specified")
    }

    if (aggregations.isEmpty && granularities.nonEmpty) {
      sys.error(s"Granularity specified but no aggregation requested")
    }

    val names = filters.flatMap(getNameFilters).map(_.name).distinct

    val rdds1 = for {
      name <- names
      aggregation <- if (aggregations.isEmpty) { Array(None) } else { aggregations }
      granularity <- if (granularities.isEmpty) { Array(None) } else { granularities }
    } yield {
      if (timestampLimits.exists(_.value < 0)) {
        sys.error("timestamp limits must be non-negative")
      }
      val maxTimestamp: Long = timestampUpperLimit match {
        case Some(i) => i
        case None =>
          getLatestDataPoint(name).map(_.timestamp + 1)
            .getOrElse(System.currentTimeMillis())
      }
      DataPointsRdd(sqlContext.sparkContext,
        parseResult,
        toRow(name, aggregation.map(_.aggregation), granularity.map(g => s"${g.amount.getOrElse("")}${g.unit}"), requiredColumns),
        aggregation, granularity,
        timestampLowerLimit.getOrElse(0),
        maxTimestamp,
        uri"${baseDataPointsUrl(project)}/$name",
        apiKey, project, limit, aggregation.map(_ => 10000).getOrElse(batchSize))
    }
    rdds1.foldLeft(sqlContext.sparkContext.emptyRDD[Row])((a, b) => a.union(b))
  }
  // scalastyle:on cyclomatic.complexity

  def parseResult(response: Response[Array[Byte]]): Response[Seq[NumericDatapoint]] = {
    //TODO: handle string timeseries
    val r = Either.catchNonFatal {
      val timeSeriesData = TimeseriesData.parseFrom(response.unsafeBody)
      if (timeSeriesData.hasNumericData) {
        timeSeriesData.getNumericData.getPointsList.asScala
      } else {
        Seq.empty
      }
    }
    val rr = r.left.map(throwable => throwable.getMessage.getBytes)
    Response(rr, response.code, response.statusText, response.headers, response.history)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(batch => {
        val timeSeriesData = MultiNamedTimeseriesData.newBuilder()
        batch.groupBy(r => r.getAs[String](0))
          .foreach { case (name, timeseriesRows) => {
            val d = timeseriesRows.foldLeft(NumericTimeseriesData.newBuilder())((builder, row) =>
              builder.addPoints(NumericDataPointV2.newBuilder()
                .setTimestamp(row.getLong(1))
                .setValue(row.getDouble(2))))

            timeSeriesData.addNamedTimeseriesData(
              NamedTimeseriesData.newBuilder()
                .setName(name)
                .setNumericData(d.build())
                .build()
            )
          }}
        postTimeSeries(timeSeriesData.build())
      }).unsafeRunSync
    })
  }

  private val maxRetries = 10
  private def postTimeSeries(data: MultiNamedTimeseriesData): IO[Unit] = {
    val url = uri"${baseDataPointsUrl(project)}"
    val postDataPoints = sttp.header("Accept", "application/json")
      .header("api-key", apiKey)
      .parseResponseIf(_ => true)
      .contentType("application/protobuf")
      .body(data.toByteArray)
      .post(url)
      .send()
      .flatMap(defaultHandling(url))
    retryWithBackoff(postDataPoints, 30.millis, maxRetries)
      .map(r => {
        if (collectMetrics) {
          val numPoints = data.getNamedTimeseriesDataList.asScala
            .map(_.getNumericData.getPointsCount)
            .sum
          datapointsCreated.inc(numPoints)
        }
        r
      }).flatMap(_ => IO.unit)
  }

  def baseDataPointsUrl(project: String): Uri = {
    uri"${baseUrl(project, "0.5")}/timeseries/data"
  }
}
