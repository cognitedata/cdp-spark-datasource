package com.cognite.spark.datasource

import io.circe.syntax._
import io.circe.generic.auto._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.softwaremill.sttp._

case class CdpEventByIdsRddPartition(index: Int, uri: Uri, eventIds: Seq[EventId]) extends Partition

case class CdpEventsByIdsRdd(
    @transient override val sparkContext: SparkContext,
    toRow: EventItem => Row,
    config: RelationConfig,
    uri: Uri,
    eventIds: Seq[EventId])
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {
  // This needs to be broadcast here since compute will be called from the executors
  // where the SparkContext is not available
  private val broadcastApplicationId = sparkContext.broadcast(sparkContext.applicationId)
  override def getPartitions: Array[Partition] = {
    val partitionedBodies = partitionUrlBodies(eventIds)
    partitionedBodies.zipWithIndex.map {
      case (ids, i) =>
        CdpEventByIdsRddPartition(i, uri, ids)
    }.toArray
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdpEventByIdsRddPartition]
    val rowIteratorsSeq =
      postWithBody[EventItem, EventId](config, split.uri, split.eventIds)
    val rowIterators = rowIteratorsSeq.toList.map(toRow).toIterator

    new InterruptibleIterator(context, rowIterators)
  }

  private def partitionUrlBodies(eventUriBody: Seq[EventId]): Seq[Seq[EventId]] =
    eventUriBody.grouped(Constants.DefaultBatchSize).toSeq
}
