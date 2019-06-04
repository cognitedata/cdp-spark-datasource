package com.cognite.spark.datasource

import com.softwaremill.sttp._
import io.circe.Decoder
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class CdpRddPartition(cursor: Option[String], size: Option[Int], index: Int) extends Partition

case class CdpRdd[A: Decoder](
    @transient override val sparkContext: SparkContext,
    toRow: A => Row,
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig,
    cursors: Iterator[(Option[String], Option[Int])])
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {

  // This needs to be broadcast here since compute will be called from the executors
  // where the SparkContext is not available
  val broadcastedApplicationId = sparkContext.broadcast(sparkContext.applicationId)
  override def getPartitions: Array[Partition] =
    cursors.toIndexedSeq.zipWithIndex.map {
      case ((cursor, size), index) =>
        val partitionSize = (config.limit, size) match {
          case (None, s) => s
          case (Some(l), Some(s)) => Some(scala.math.min(l, s))
          case (l, None) => l
        }
        CdpRddPartition(cursor, partitionSize, index)
    }.toArray

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdpRddPartition]
    val cdpRows =
      get[A](
        config.copy(limit = split.size, applicationId = broadcastedApplicationId.value),
        getSinglePartitionBaseUri,
        split.cursor
      ).map(toRow)

    new InterruptibleIterator(context, cdpRows)
  }
}
