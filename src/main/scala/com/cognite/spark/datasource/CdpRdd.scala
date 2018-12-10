package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.spark.datasource.CdpConnector.{DataItemsWithCursor, retryWithBackoff}
import io.circe.generic.auto._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import com.softwaremill.sttp.asynchttpclient.cats._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

case class CdpRddPartition(cursor: Option[String], size: Option[Int], index: Int) extends Partition

class CdpRdd(sparkContext: SparkContext, apiKey: String, project: String, batchSize: Int, limit: Option[Int])
  extends RDD[Row](sparkContext, Nil) {

  @transient implicit val timer = cats.effect.IO.timer(ExecutionContext.global)
  @transient implicit val sttpBackend = AsyncHttpClientCatsBackend[IO]()
  private val maxRetries = 3

  private def cursors(url: Uri): Iterator[(Option[String], Option[Int])] = {
    new Iterator[(Option[String], Option[Int])] {
      private var nItemsRead = 0
      private var nextCursor = Option.empty[String]
      private var isFirst = true

      override def hasNext: Boolean = {
        if (isFirst) {
          isFirst = false
          true
        } else {
          nextCursor.isDefined && limit.fold(true)(_ > nItemsRead)
        }
      }

      override def next(): (Option[String], Option[Int]) = {
        val next = nextCursor
        val thisBatchSize = math.min(batchSize, limit.map(_ - nItemsRead).getOrElse(batchSize))
        val urlWithLimit = url.param("limit", thisBatchSize.toString)
        val getUrl = nextCursor.fold(urlWithLimit)(urlWithLimit.param("cursor", _))
        val result = sttp.header("Accept", "application/json")
          .header("api-key", apiKey).get(getUrl).response(asJson[DataItemsWithCursor[EventItem]])
          .parseResponseIf(_ => true)
          .send()
          .map(r => r.unsafeBody match {
            case Left(error) =>
              throw new RuntimeException(s"boom ${error.message}, ${r.statusText} ${r.code.toString} ${r.toString()}")
            case Right(items) => items
          })
        val dataWithCursor = retryWithBackoff(result, 30.millis, maxRetries)
          .unsafeRunSync()
          .data
        nextCursor = dataWithCursor.nextCursor
        nItemsRead += thisBatchSize
        (next, nextCursor.map(_ => thisBatchSize))
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val url = EventsRelation.baseEventsURL(project)
    scala.util.Random.shuffle(cursors(url.param("onlyCursors", "true")))
      .toIndexedSeq
      .zipWithIndex
      .map { case ((cursor, size), index) => CdpRddPartition(cursor, size, index) }
      .toArray
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdpRddPartition]
    val getUrl = EventsRelation.baseEventsURL(project)
    val cdpRows = CdpConnector.get[EventItem](apiKey,
      getUrl,
      batchSize, split.size, 10, split.cursor)
      .map(item => {
        Row(item.id, item.startTime, item.endTime, item.description, item.`type`, item.subtype,
          item.metadata, item.assetIds, item.source, item.sourceId)
      })

    new InterruptibleIterator(context, cdpRows)
  }
}
