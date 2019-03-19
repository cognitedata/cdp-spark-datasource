package com.cognite.spark.datasource

import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import com.cognite.data.api.v2.DataPoints.NumericDatapoint

case class DataPointsRddPartition(startTime: Long, endTime: Long, index: Int) extends Partition

case class DataPointsRdd(
    @transient override val sparkContext: SparkContext,
    parseResult: Response[Array[Byte]] => Response[Seq[NumericDatapoint]],
    toRow: NumericDatapoint => Row,
    numPartitions: Int,
    aggregation: Option[AggregationFilter],
    granularity: Option[GranularityFilter],
    minTimestamp: Long,
    maxTimestamp: Long,
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig)
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {
  private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  private val granularityMilliseconds = DataPointsRdd.granularityMilliseconds(granularity)

  private def getRows(minTimestamp: Long, maxTimestamp: Long) =
    Batch.withCursor(batchSize, config.limit) { (thisBatchSize, cursor: Option[Long]) =>
      val start = cursor.getOrElse(minTimestamp)
      if (start < maxTimestamp) {
        val uri = getSinglePartitionBaseUri
          .param("start", start.toString)
          .param("end", maxTimestamp.toString)
          .param("limit", thisBatchSize.toString)
        val dataPoints = aggregation match {
          case Some(aggregationFilter) =>
            val g = granularity.getOrElse(
              sys.error("Aggregation requested, but no granularity specified"))
            val uriWithAggregation = uri
              .param("aggregates", s"${aggregationFilter.aggregation}")
              .param("granularity", s"${g.amount.getOrElse("")}${g.unit}")
            getJson[CdpConnector.DataItemsWithCursor[DataPointsItem]](
              config.apiKey,
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
            getProtobuf[Seq[NumericDatapoint]](config.apiKey, uri, parseResult, config.maxRetries)
              .unsafeRunSync()
        }
        if (dataPoints.lastOption.fold(true)(_.timestamp < start)) {
          (Seq.empty, None)
        } else {
          (dataPoints.map(toRow), dataPoints.lastOption.map(_.timestamp + granularityMilliseconds))
        }
      } else {
        (Seq.empty, None)
      }
    }

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
    }

  private val unitMilliseconds =
    DataPointsRdd.granularityMilliseconds(granularity.map(_.copy(amount = Some(1))))
  override def getPartitions: Array[Partition] = {
    val min = DataPointsRdd.floorToNearest(minTimestamp, unitMilliseconds)
    val max = DataPointsRdd.ceilToNearest(maxTimestamp, unitMilliseconds)
    val partitions =
      DataPointsRdd.intervalPartitions(min, max, granularityMilliseconds, numPartitions)
    partitions.asInstanceOf[Array[Partition]]
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[DataPointsRddPartition]
    val rows = getRows(split.startTime, split.endTime)

    new InterruptibleIterator(context, rows)
  }
}

object DataPointsRdd {
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

  private def granularityMilliseconds(granularity: Option[GranularityFilter]): Long =
    granularity
      .map(g => granularityUnitToMilliseconds(g.unit) * g.amount.getOrElse(1L))
      .getOrElse(1)

  private def floorToNearest(x: Long, base: Double) =
    (base * math.floor(x.toDouble / base)).toLong

  private def ceilToNearest(x: Long, base: Double) =
    (base * math.ceil(x.toDouble / base)).toLong

  private def roundToNearest(x: Long, base: Double) =
    (base * math.round(x.toDouble / base)).toLong

  // This is based on the same logic used in the Cognite Python SDK:
  // https://github.com/cognitedata/cognite-sdk-python/blob/8028c80fd5df4415365ce5e50ccae04a5acb0251/cognite/client/_utils.py#L153
  def intervalPartitions(
      start: Long,
      stop: Long,
      granularityMilliseconds: Long,
      numPartitions: Int): Array[DataPointsRddPartition] = {
    val intervalLength = stop - start
    val steps = math.min(numPartitions, math.max(1, intervalLength / granularityMilliseconds))
    val stepSize = roundToNearest((intervalLength.toDouble / steps).toLong, granularityMilliseconds)

    0.until(steps.toInt)
      .map { index =>
        val beginning = start + stepSize * index
        val end = beginning + stepSize
        DataPointsRddPartition(beginning, if (index == steps - 1) { math.max(end, stop) } else {
          end
        }, index)
      }
      .toArray
  }
}
