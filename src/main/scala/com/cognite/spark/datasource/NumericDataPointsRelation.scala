package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.data.api.v2.DataPoints.{
  MultiNamedTimeseriesData,
  NamedTimeseriesData,
  NumericDatapoint,
  NumericTimeseriesData
}
import com.softwaremill.sttp._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.concurrent.ExecutionContext

// TODO: make case classes / enums for each valid granularity
sealed case class GranularityFilter(amount: Option[Long], unit: String)

// TODO: make case classes / enums for each valid aggregation
sealed case class AggregationFilter(aggregation: String)

class NumericDataPointsRelation(
    config: RelationConfig,
    numPartitions: Int,
    suppliedSchema: Option[StructType])(override val sqlContext: SQLContext)
    extends DataPointsRelation(config, numPartitions, suppliedSchema)(sqlContext) {
  @transient override val datapointsCreated =
    metricsSource.getOrCreateCounter(s"datapoints.created")
  @transient override val datapointsRead = metricsSource.getOrCreateCounter(s"datapoints.read")

  override def schema: StructType =
    suppliedSchema.getOrElse(
      StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("timestamp", LongType, nullable = false),
          StructField("value", DoubleType, nullable = false),
          StructField("aggregation", StringType, nullable = true),
          StructField("granularity", StringType, nullable = true)
        )))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (timestampLowerLimit, timestampUpperLimit) = getTimestampLimits(filters)
    val (aggregations, granularities) = getAggregationSettings(filters)
    val names = filters.flatMap(getNameFilters).map(_.name).distinct

    val rdds = for {
      name <- names
      aggregation <- if (aggregations.isEmpty) { Array(None) } else { aggregations }
      granularity <- if (granularities.isEmpty) { Array(None) } else { granularities }
    } yield {
      val maxTimestamp = timestampUpperLimit match {
        case Some(i) => i
        case None =>
          getLatestDataPoint(name)
            .map(_.timestamp + 1)
            .getOrElse(System.currentTimeMillis())
      }
      val aggregationBatchSize = aggregation
        .map(
          _ =>
            math.min(
              config.batchSize.getOrElse(Constants.DefaultDataPointsAggregationBatchSize),
              Constants.DefaultDataPointsAggregationBatchSize))
        .getOrElse(config.batchSize.getOrElse(Constants.DefaultDataPointsBatchSize))
      new NumericDataPointsRdd(
        sqlContext.sparkContext,
        toRow(
          name,
          aggregation.map(_.aggregation),
          granularity.map(g => s"${g.amount.getOrElse("")}${g.unit}"),
          requiredColumns),
        numPartitions,
        aggregation,
        granularity,
        timestampLowerLimit.getOrElse(0),
        maxTimestamp,
        uri"${baseDataPointsUrl(config.project)}/$name",
        config.copy(batchSize = Some(aggregationBatchSize))
      )
    }
    rdds.foldLeft(sqlContext.sparkContext.emptyRDD[Row])((a, b) => a.union(b))
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches =
        rows.grouped(config.batchSize.getOrElse(Constants.DefaultDataPointsBatchSize)).toVector
      batches
        .parTraverse(batch => {
          val timeSeriesData = MultiNamedTimeseriesData()
          val namedTimeseriesData = batch
            .groupBy(r => r.getAs[String](0))
            .map {
              case (name, timeseriesRows) =>
                val d = timeseriesRows.foldLeft(NumericTimeseriesData())((builder, row) =>
                  builder.addPoints(NumericDatapoint(row.getLong(1), row.getDouble(2))))
                NamedTimeseriesData(name, NamedTimeseriesData.Data.NumericData(d))
            }
          postTimeSeries(timeSeriesData.addAllNamedTimeseriesData(namedTimeseriesData))
        })
        .unsafeRunSync
    })

  private val requiredColumnToIndex =
    Map("name" -> 0, "timestamp" -> 1, "value" -> 2, "aggregation" -> 3, "granularity" -> 4)
  private def toColumns(
      name: String,
      aggregation: Option[String],
      granularity: Option[String],
      requiredColumns: Array[String],
      dataPoint: NumericDatapoint): Seq[Option[Any]] = {
    val requiredColumnIndexes = requiredColumns.map(requiredColumnToIndex)
    for (index <- requiredColumnIndexes)
      yield
        index match {
          case 0 => Some(name)
          case 1 => Some(dataPoint.timestamp)
          case 2 => Some(dataPoint.value)
          case 3 => aggregation
          case 4 => granularity
          case _ =>
            sys.error("Invalid required column index " + index.toString)
            None
        }
  }

  private def toRow(
      name: String,
      aggregation: Option[String],
      granularity: Option[String],
      requiredColumns: Array[String])(dataPoint: NumericDatapoint): Row = {
    if (config.collectMetrics) {
      datapointsRead.inc()
    }
    Row.fromSeq(toColumns(name, aggregation, granularity, requiredColumns, dataPoint))
  }

  private def getAggregationSettings(filters: Array[Filter]) = {
    val aggregations = filters.flatMap(getAggregation).map(_.some).distinct
    val granularities = filters.flatMap(getGranularity).map(_.some).distinct

    if (aggregations.nonEmpty && granularities.isEmpty) {
      sys.error(s"Aggregations requested but granularity is not specified")
    }

    if (aggregations.isEmpty && granularities.nonEmpty) {
      sys.error(s"Granularity specified but no aggregation requested")
    }

    (aggregations, granularities)
  }

  // scalastyle:off cyclomatic.complexity
  def toAggregationFilter(aggregation: String): AggregationFilter =
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

  def getAggregation(filter: Filter): Seq[AggregationFilter] =
    filter match {
      case IsNotNull("aggregation") => Seq()
      case EqualTo("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case EqualNullSafe("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case In("aggregation", values) =>
        values.map(v => toAggregationFilter(v.toString))
      case And(_, _) => sys.error("AND is not allowed for granularity")
      case Or(f1, f2) => getAggregation(f1) ++ getAggregation(f2)
      case StringStartsWith("name", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("name", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("name", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
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

  def getGranularity(filter: Filter): Seq[GranularityFilter] =
    filter match {
      case IsNotNull("granularity") => Seq()
      case EqualTo("granularity", value) => Seq(toGranularityFilter(value.toString))
      case EqualNullSafe("granularity", value) => Seq(toGranularityFilter(value.toString))
      case In("granularity", values) =>
        values.map(v => toGranularityFilter(v.toString))
      case And(_, _) => sys.error("AND is not allowed for granularity")
      case Or(f1, f2) => getGranularity(f1) ++ getGranularity(f2)
      case StringStartsWith("name", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("name", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("name", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
    }
  // scalastyle:on cyclomatic.complexity
}
