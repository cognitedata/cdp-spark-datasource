package com.cognite.spark.datasource

import com.cognite.data.api.v1.NumericDatapoint
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class DataPointsRddPartition(startTime: Long, endTime: Long, index: Int) extends Partition

case class DataPointsRdd(@transient override val sparkContext: SparkContext,
                         parseResult: Response[Array[Byte]] => Response[Seq[NumericDatapoint]],
                         toRow: NumericDatapoint => Row,
                         aggregation: Option[AggregationFilter],
                         granularity: Option[GranularityFilter],
                         minTimestamp: Long,
                         maxTimestamp: Long,
                         getSinglePartitionBaseUri: Uri,
                         apiKey: String,
                         project: String,
                         limit: Option[Int],
                         batchSize: Int)
  extends RDD[Row](sparkContext, Nil)
    with CdpConnector {
  private val maxRetries = 10

  private def getRows(minTimestamp: Long, maxTimestamp: Long) = {
    Batch.withCursor(batchSize, limit) { (thisBatchSize, cursor: Option[Long]) =>
      val start = cursor.getOrElse(minTimestamp)
      if (start < maxTimestamp) {
        val uri = getSinglePartitionBaseUri
          .param("start", start.toString)
          .param("end", maxTimestamp.toString)
          .param("limit", thisBatchSize.toString)
        val dataPoints = aggregation match {
          case Some(aggregationFilter) =>
            val g = granularity.getOrElse(sys.error("Aggregation requested, but no granularity specified"))
            val uriWithAggregation = uri.param("aggregates", s"${aggregationFilter.aggregation}")
              .param("granularity", s"${g.amount.getOrElse("")}${g.unit}")
            getJson[CdpConnector.DataItemsWithCursor[DataPointsItem]](apiKey, uriWithAggregation, maxRetries)
              .unsafeRunSync()
              .data.items
              .flatMap(dataPoints =>
                dataPoints.datapoints.map(dataPoint => {
                  NumericDatapoint.newBuilder()
                    .setTimestamp(dataPoint.timestamp)
                    .setValue(getAggregationValue(dataPoint, aggregationFilter))
                    .build()
                }))
          case None =>
            getProtobuf[Seq[NumericDatapoint]](apiKey, uri, parseResult, maxRetries)
              .unsafeRunSync()
        }
        if (dataPoints.lastOption.fold(true)(_.getTimestamp < start)) {
          (Seq.empty, None)
        } else {
          (dataPoints.map(toRow), dataPoints.lastOption.map(_.getTimestamp + 1))
        }
      } else {
        (Seq.empty, None)
      }
    }
  }

  private def getAggregationValue(dataPoint: DataPoint, aggregation: AggregationFilter): Double = {
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
  }

  override def getPartitions: Array[Partition] = {
    Array(DataPointsRddPartition(minTimestamp, maxTimestamp, 0))
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[DataPointsRddPartition]
    val rows = getRows(split.startTime, split.endTime)

    new InterruptibleIterator(context, rows)
  }
}
