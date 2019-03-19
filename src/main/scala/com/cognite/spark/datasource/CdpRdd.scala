package com.cognite.spark.datasource

import com.softwaremill.sttp._
import io.circe.generic.auto._
import io.circe.generic.decoding.DerivedDecoder
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class CdpRddPartition(cursor: Option[String], size: Option[Int], index: Int) extends Partition

case class CdpRdd[A: DerivedDecoder](
    @transient override val sparkContext: SparkContext,
    toRow: A => Row,
    getPartitionsBaseUri: Uri,
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig)
    extends RDD[Row](sparkContext, Nil)
    with CdpConnector {

  private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

  private def cursors(url: Uri): Iterator[(Option[String], Option[Int])] =
    new Iterator[(Option[String], Option[Int])] {
      private var nItemsRead = 0
      private var nextCursor = Option.empty[String]
      private var isFirst = true

      override def hasNext: Boolean =
        isFirst || nextCursor.isDefined && config.limit.fold(true)(_ > nItemsRead)

      override def next(): (Option[String], Option[Int]) = {
        isFirst = false
        val next = nextCursor
        val thisBatchSize = math.min(
          batchSize,
          config.limit
            .map(_ - nItemsRead)
            .getOrElse(batchSize))
        val urlWithLimit = url.param("limit", thisBatchSize.toString)
        val getUrl = nextCursor.fold(urlWithLimit)(urlWithLimit.param("cursor", _))
        val dataWithCursor =
          getJson[CdpConnector.DataItemsWithCursor[A]](config.apiKey, getUrl, config.maxRetries)
            .unsafeRunSync()
            .data
        nextCursor = dataWithCursor.nextCursor
        nItemsRead += thisBatchSize
        (next, nextCursor.map(_ => thisBatchSize))
      }
    }

  override def getPartitions: Array[Partition] =
    scala.util.Random
      .shuffle(cursors(getPartitionsBaseUri))
      .toIndexedSeq
      .zipWithIndex
      .map { case ((cursor, size), index) => CdpRddPartition(cursor, size, index) }
      .toArray

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdpRddPartition]
    val cdpRows =
      get[A](
        config.apiKey,
        getSinglePartitionBaseUri,
        batchSize,
        split.size,
        config.maxRetries,
        split.cursor)
        .map(toRow)

    new InterruptibleIterator(context, cdpRows)
  }
}
