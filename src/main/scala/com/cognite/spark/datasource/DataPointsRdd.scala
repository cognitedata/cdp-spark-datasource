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

  // scalastyle:off
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
                  val value = aggregation match {
                    // TODO: make this properly typed
                    case Some(AggregationFilter("average")) | Some(AggregationFilter("avg")) =>
                      dataPoint.average.get
                    case Some(AggregationFilter("max")) => dataPoint.max.get
                    case Some(AggregationFilter("min")) => dataPoint.min.get
                    case Some(AggregationFilter("count")) => dataPoint.count.get
                    case Some(AggregationFilter("sum")) => dataPoint.sum.get
                    case Some(AggregationFilter("stepinterpolation")) | Some(AggregationFilter("step")) =>
                      dataPoint.stepInterpolation.get
                    case Some(AggregationFilter("continuousvariance")) | Some(AggregationFilter("cv")) =>
                      dataPoint.continuousVariance.get
                    case Some(AggregationFilter("discretevariance")) | Some(AggregationFilter("dv")) =>
                      dataPoint.discreteVariance.get
                    case Some(AggregationFilter("totalvariation")) | Some(AggregationFilter("tv")) =>
                      dataPoint.totalVariation.get
                    case None => dataPoint.value.get
                    case _ => dataPoint.value.get
                  }
                  NumericDatapoint.newBuilder()
                    .setTimestamp(dataPoint.timestamp)
                    .setValue(value)
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

  override def getPartitions: Array[Partition] = {
    Array(DataPointsRddPartition(minTimestamp, maxTimestamp, 0))
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[DataPointsRddPartition]
    val rows = getRows(split.startTime, split.endTime)

    new InterruptibleIterator(context, rows)
  }
}
