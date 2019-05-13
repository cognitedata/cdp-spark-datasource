package com.cognite.spark.datasource

import cats.implicits._
import com.cognite.data.api.v2.DataPoints.{NumericDatapoint, TimeseriesData}
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.sql.Row

class NumericDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    timestampLimits: Map[String, (Long, Long)],
    toRow: (String, NumericDatapoint) => Row,
    numPartitions: Int,
    aggregation: Option[AggregationFilter],
    granularity: Option[GranularityFilter],
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig)
    extends DataPointsRdd(sparkContext, getSinglePartitionBaseUri, config) {
  private val granularityMilliseconds = granularityToMilliseconds(granularity)
  private val unitMilliseconds = granularityToMilliseconds(
    granularity.map(_.copy(amount = Some(1))))

  override def getDataPointRows(name: String, uri: Uri, start: Long): (Seq[Row], Option[Long]) = {
    val dataPoints = aggregation match {
      case Some(aggregationFilter) =>
        val g =
          granularity.getOrElse(sys.error("Aggregation requested, but no granularity specified"))
        val uriWithAggregation = uri
          .param("aggregates", s"${aggregationFilter.aggregation}")
          .param("granularity", s"${g.amount.getOrElse("")}${g.unit}")
        getJson[CdpConnector.DataItemsWithCursor[DataPointsItem]](
          config.auth,
          uriWithAggregation,
          config.maxRetries)
          .unsafeRunSync()
          .data
          .items
          .flatMap(dataPoints =>
            dataPoints.datapoints.map(dataPoint => {
              NumericDatapoint(
                dataPoint.timestamp,
                getAggregationValue(dataPoint, aggregationFilter))
            }))
      case None =>
        getProtobuf[Seq[NumericDatapoint]](config.auth, uri, parseResult, config.maxRetries)
          .unsafeRunSync()
    }
    if (dataPoints.lastOption.fold(true)(_.timestamp < start)) {
      (Seq.empty, None)
    } else {
      (
        dataPoints.map(toRow(name, _)),
        dataPoints.lastOption.map(_.timestamp + granularityMilliseconds))
    }
  }

  def parseResult(response: Response[Array[Byte]]): Response[Seq[NumericDatapoint]] = {
    val r = Either.catchNonFatal {
      val timeSeriesData = TimeseriesData.parseFrom(response.unsafeBody)
      if (timeSeriesData.data.isNumericData) {
        timeSeriesData.getNumericData.points
      } else {
        Seq.empty
      }
    }
    val rr = r.left.map(throwable => throwable.getMessage.getBytes)
    Response(rr, response.code, response.statusText, response.headers, response.history)
  }

  // scalastyle:off cyclomatic.complexity
  private def getAggregationValue(dataPoint: DataPoint, aggregation: AggregationFilter): Double =
    aggregation match {
      // TODO: make this properly typed
      case AggregationFilter("average") | AggregationFilter("avg") =>
        dataPoint.average.get
      case AggregationFilter("max") => dataPoint.max.get
      case AggregationFilter("min") => dataPoint.min.get
      case AggregationFilter("count") => dataPoint.count.get
      case AggregationFilter("sum") => dataPoint.sum.get
      case AggregationFilter("stepinterpolation") | AggregationFilter("step") =>
        dataPoint.stepInterpolation.get
      case AggregationFilter("continuousvariance") | AggregationFilter("cv") =>
        dataPoint.continuousVariance.get
      case AggregationFilter("discretevariance") | AggregationFilter("dv") =>
        dataPoint.discreteVariance.get
      case AggregationFilter("totalvariation") | AggregationFilter("tv") =>
        dataPoint.totalVariation.get
      case _ => sys.error(s"Invalid aggregation $aggregation")
    }
  // scalastyle:on cyclomatic.complexity

  private def granularityToMilliseconds(granularity: Option[GranularityFilter]): Long =
    granularity
      .map(g => NumericDataPointsRdd.granularityUnitToMilliseconds(g.unit) * g.amount.getOrElse(1L))
      .getOrElse(1)

  private def floorToNearest(x: Long, base: Double) =
    (base * math.floor(x.toDouble / base)).toLong

  private def ceilToNearest(x: Long, base: Double) =
    (base * math.ceil(x.toDouble / base)).toLong

  override def getPartitions: Array[Partition] =
    timestampLimits
      .flatMap {
        case (name, (lowerLimit, upperLimit)) =>
          DataPointsRdd
            .intervalPartitions(
              name,
              scala.math.max(
                floorToNearest(lowerLimit, unitMilliseconds) - aggregation.map(_ => 0).getOrElse(1),
                0),
              ceilToNearest(upperLimit, unitMilliseconds) + aggregation.map(_ => 0).getOrElse(1),
              granularityMilliseconds,
              numPartitions
            )
      }
      .zipWithIndex
      .map { case (p, idx) => p.copy(index = idx) }
      .toArray
}

object NumericDataPointsRdd {
  private val granularityUnitToMilliseconds = Map(
    "s" -> 1000L,
    "second" -> 1000L,
    "m" -> 60000L,
    "minute" -> 60000L,
    "h" -> 3600000L,
    "hour" -> 3600000L,
    "d" -> 86400000L,
    "day" -> 86400000L
  )
}
