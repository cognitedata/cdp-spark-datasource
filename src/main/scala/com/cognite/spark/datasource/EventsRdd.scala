package com.cognite.spark.datasource

import com.softwaremill.sttp.Uri
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import io.circe.generic.auto._

case class EventsRddPartition(cursor: Option[String], index: Int, filter: EventFilter)
    extends Partition

case class EventsRdd(
    @transient override val sparkContext: SparkContext,
    getSinglePartitionBaseUri: Uri,
    toRow: EventItem => Row,
    eventFilters: Array[EventFilter],
    config: RelationConfig,
    cursors: Iterator[(Option[String], Option[Int])])
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {

  private val applicationId = Some(sparkContext.applicationId)
  override def getPartitions: Array[Partition] =
    (for {
      (cursor, _) <- cursors
      eventFilter <- eventFilters
    } yield EventsRddPartition(cursor, 0, eventFilter)).zipWithIndex.map {
      case (p, idx) => p.copy(index = idx)
    }.toArray

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[EventsRddPartition]
    val urlWithType =
      split.filter.`type`
        .fold(getSinglePartitionBaseUri)(getSinglePartitionBaseUri.param("type", _))
    val urlWithTypeAndSubType =
      split.filter.subtype.fold(urlWithType)(urlWithType.param("subtype", _))
    val cdpRows: Iterator[Row] =
      get[EventItem](
        config,
        urlWithTypeAndSubType,
        split.cursor
      ).map(toRow)

    new InterruptibleIterator(context, cdpRows)
  }
}
