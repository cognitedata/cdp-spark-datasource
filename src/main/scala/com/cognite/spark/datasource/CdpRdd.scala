package com.cognite.spark.datasource

import io.circe.generic.auto._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class CdpRddPartition(cursor: String, index: Int) extends Partition

class CdpRdd(sparkContext: SparkContext, apiKey: String, project: String, batchSize: Int, limit: Option[Int]) extends RDD[Row](sparkContext, Nil) {
  override def getPartitions: Array[Partition] = {
    val url = EventsRelation.baseEventsURL(project).addQueryParameter("onlyCursors", "true").build()
    CdpConnector.getWithCursor[RawItem](apiKey, url, batchSize, limit)
      .filter(_.cursor.isDefined)
      .map(chunk => chunk.cursor.get)
      .zipWithIndex
      .map { case (cursor, index) => CdpRddPartition(cursor, index) }
      .toArray
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdpRddPartition]
    var eventsReceived = 0
    CdpConnector.get[EventItem](apiKey, EventsRelation.baseEventsURL(project)
      .addQueryParameter("cursor", split.cursor).build(), batchSize, Some(batchSize))
      .map(item => {
        eventsReceived += 1
        if ((eventsReceived % 10000) == 0) {
          println(s"Read ${eventsReceived} events")
        }
//        if (collectMetrics) {
//          eventsRead.inc()
//        }
        Row(item.id, item.startTime, item.endTime, item.description, item.`type`, item.subtype,
          item.metadata, item.assetIds, item.source, item.sourceId)
      })
//    val close = () => { }
//
//    CompletionIterator[Row, Iterator[Row]](
//      new InterruptibleIterator(context, cdpRows), close())
  }
}
