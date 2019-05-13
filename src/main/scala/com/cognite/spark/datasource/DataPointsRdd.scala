package com.cognite.spark.datasource

import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class DataPointsRddPartition(name: String, startTime: Long, endTime: Long, index: Int)
    extends Partition

abstract case class DataPointsRdd(
    @transient override val sparkContext: SparkContext,
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig)
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {
  private val batchSize = config.batchSize.getOrElse(Constants.DefaultDataPointsBatchSize)

  def getDataPointRows(name: String, uri: Uri, start: Long): (Seq[Row], Option[Long])

  private def getRows(name: String, minTimestamp: Long, maxTimestamp: Long): Iterator[Row] =
    Batch.withCursor(batchSize, config.limit) { (thisBatchSize, cursor: Option[Long]) =>
      val start = cursor.getOrElse(minTimestamp)
      if (start < maxTimestamp) {
        val uri = uri"$getSinglePartitionBaseUri/$name"
          .param("start", start.toString)
          .param("end", maxTimestamp.toString)
          .param("limit", thisBatchSize.toString)
        getDataPointRows(name, uri, start)
      } else {
        (Seq.empty, None)
      }
    }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[DataPointsRddPartition]
    val rows = getRows(split.name, split.startTime, split.endTime)

    new InterruptibleIterator(context, rows)
  }
}

object DataPointsRdd {
  private def roundToNearest(x: Long, base: Double) =
    (base * math.round(x.toDouble / base)).toLong

  // This is based on the same logic used in the Cognite Python SDK:
  // https://github.com/cognitedata/cognite-sdk-python/blob/8028c80fd5df4415365ce5e50ccae04a5acb0251/cognite/client/_utils.py#L153
  def intervalPartitions(
      name: String,
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
        // make sure we read all the way to "stop" in the last partition.
        val partitionEnd = if (index == steps - 1) { math.max(end, stop) } else { end }
        DataPointsRddPartition(name, beginning, partitionEnd, index)
      }
      .toArray
  }
}
