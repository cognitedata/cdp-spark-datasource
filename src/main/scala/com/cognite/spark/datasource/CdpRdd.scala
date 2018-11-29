package com.cognite.spark.datasource

import io.circe.generic.auto._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class CdpRddPartition(cursor: String, index: Int) extends Partition

class CdpRdd(sparkContext: SparkContext, apiKey: String, project: String, batchSize: Int, limit: Option[Int])
  extends RDD[Row](sparkContext, Nil) {
  override def getPartitions: Array[Partition] = {
    val url = EventsRelation.baseEventsURL(project).addQueryParameter("onlyCursors", "true").build()
    val parts = CdpConnector.getWithCursor[EventItem](apiKey, url, batchSize, limit)
      .filter(_.cursor.isDefined)
      .map(chunk => chunk.cursor.get)
    scala.util.Random.shuffle(parts)
      .toIndexedSeq
      .zipWithIndex
      .map { case (cursor, index) => CdpRddPartition(cursor, index) }
      .toArray
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    println("COMPUTE")
    val split = _split.asInstanceOf[CdpRddPartition]
    val cdpRows = CdpConnector.get[EventItem](apiKey, EventsRelation.baseEventsURL(project).build(), batchSize, Some(batchSize))
      .map(item => {
        Row(item.id, item.startTime, item.endTime, item.description, item.`type`, item.subtype,
          item.metadata, item.assetIds, item.source, item.sourceId)
      })

    new InterruptibleIterator(context, cdpRows)
  }
}
