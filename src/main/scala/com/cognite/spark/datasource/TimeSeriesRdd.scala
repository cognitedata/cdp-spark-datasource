package com.cognite.spark.datasource

import com.softwaremill.sttp.Uri
import io.circe.Decoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.sql.Row

case class TimeSeriesRdd[TimeSeriesItem: Decoder](
    @transient override val sparkContext: SparkContext,
    toRow: TimeSeriesItem => Row,
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig,
    urlsWithFilters: Seq[Uri],
    cursors: Iterator[(Option[String], Option[Int])])
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {

  // This needs to be broadcast here since compute will be called from the executors
  // where the SparkContext is not available
  private val broadcastApplicationId = sparkContext.broadcast(sparkContext.applicationId)
  override def getPartitions: Array[Partition] =
    cursors.toIndexedSeq.zipWithIndex.map {
      case ((cursor, size), index) =>
        val partitionSize = (config.limit, size) match {
          case (None, s) => s
          case (Some(l), Some(s)) => Some(scala.math.min(l, s))
          case (l, None) => l
        }
        CdpRddPartition(cursor, partitionSize, index, urlsWithFilters)
    }.toArray

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdpRddPartition]

    val rowIteratorsSeq = split.urlsWithFilters
      .map { url =>
        getV1[TimeSeriesItem](
          config.copy(limit = split.size, applicationId = broadcastApplicationId.value),
          url,
          split.cursor
        )
      }

    val rowIteratorsSet = rowIteratorsSeq.foldLeft(Set.empty[TimeSeriesItem])(_ ++ _)
    val rowIterators = rowIteratorsSet.toList.map(toRow).toIterator

    new InterruptibleIterator(context, rowIterators)
  }
}
