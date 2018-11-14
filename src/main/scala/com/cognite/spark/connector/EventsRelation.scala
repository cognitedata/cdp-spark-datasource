package com.cognite.spark.connector

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.cognite.spark.connector.CdpConnector.DataItemsWithCursor
import io.circe.Encoder
import okhttp3.{HttpUrl, Response}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import io.circe.generic.auto._
import io.circe.parser._
import FailureCallbackStatus._

case class EventItem(id: Option[Long],
                      startTime: Option[Long],
                      endTime: Option[Long],
                      description: Option[String],
                      `type`: Option[String],
                      subtype: Option[String],
                      metadata: Option[Map[String, String]],
                      assetIds: Option[Seq[Long]],
                      source: Option[String],
                      sourceId: Option[String])

case class SourceWithResourceId(id: Long, source: String, sourceId: String)
case class Error[A](error: A)
case class EventConflict(duplicates: Seq[SourceWithResourceId])

class EventsRelation(apiKey: String,
                      project: String,
                      limit: Option[Int],
                      batchSizeOption: Option[Int],
                      metricsPrefix: String,
                      collectMetrics: Boolean)
                    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {

  @transient lazy val batchSize: Int = batchSizeOption.getOrElse(10000)

  lazy private val eventsCreated = UserMetricsSystem.counter(s"${metricsPrefix}events.created")
  lazy private val eventsRead = UserMetricsSystem.counter(s"${metricsPrefix}events.read")

  override def schema: StructType = {
    StructType(Seq(
      StructField("id", LongType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("description", StringType),
      StructField("type", StringType),
      StructField("subtype", StringType),
      StructField("metadata", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("assetIds", ArrayType(LongType)),
      StructField("source", StringType),
      StructField("sourceId", StringType)))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.foreachPartition(rows => {
      val batches = rows.grouped(batchSize).toSeq
      val remainingRequests = new CountDownLatch(batches.length)
      batches.foreach(postEvent(_, remainingRequests))
      remainingRequests.await(10, TimeUnit.MINUTES)
    })
  }

  override def buildScan(): RDD[Row] = {
    val finalRows = CdpConnector.get[EventItem](apiKey, EventsRelation.baseEventsURL(project).build(), batchSize, limit,
      batchCompletedCallback = if (collectMetrics) {
        Some((items: DataItemsWithCursor[_]) => eventsRead.inc(items.data.items.length))
      } else {
        None
      })
      .map(item => Row(item.id, item.startTime, item.endTime, item.description,
        item.`type`, item.subtype, item.metadata, item.assetIds, item.source, item.sourceId))
      .toList
    sqlContext.sparkContext.parallelize(finalRows)
  }

  def postEvent(rows: Seq[Row], remainingRequests: CountDownLatch): Unit = {
    val eventItems = rows.map(r =>
      EventItem(Option(r.getAs(0)), Option(r.getAs(1)), Option(r.getAs(2)), Option(r.getString(3)),
        Option(r.getAs(4)), Option(r.getAs(5)), Option(r.getAs(6)),
        Option(r.getAs(7)), Option(r.getAs(8)), Option(r.getAs(9)))
    )

    CdpConnector.post(apiKey, EventsRelation.baseEventsURL(project).build(),
      items=eventItems,
      wantAsync = true,
      successCallback = Some(_ => {
        if (collectMetrics) {
          eventsCreated.inc(rows.length)
        }
        remainingRequests.countDown()
      }),
      failureCallback = Some(response => handleFailure(eventItems, response, remainingRequests))
    )(Encoder[EventItem])
  }

  def handleFailure(eventItems: Seq[EventItem], response: Option[Response], remainingRequests: CountDownLatch): FailureCallbackStatus = {
    val conflict = response.filter(_.code() == 409)
    for (someResponse <- conflict) {
      decode[Error[EventConflict]](someResponse.body().string()) match {
        case Right(eventConflict) => resolveConflict(eventItems, eventConflict.error, remainingRequests)
        case Left(e) =>
          remainingRequests.countDown()
          throw new RuntimeException(s"Failed to decode conflict response (${someResponse.code()}): ${e.getMessage}")
      }
    }
    if (conflict.isEmpty) {
      remainingRequests.countDown()
    }

    conflict.map(_ => FailureCallbackStatus.Handled)
      .getOrElse(FailureCallbackStatus.Unhandled)
  }

  def resolveConflict(eventItems: Seq[EventItem], eventConflict: EventConflict, remainingRequests: CountDownLatch): Unit = {
    val duplicateEventMap = eventConflict.duplicates
      .map(conflict => (conflict.source, conflict.sourceId) -> conflict.id)
      .toMap

    val conflictingEvents: Seq[EventItem] = for {
      event <- eventItems
      source <- event.source
      sourceId <- event.sourceId
      conflictingId <- duplicateEventMap.get((source, sourceId))
      updatedEvent = event.copy(id = Some(conflictingId))
    } yield updatedEvent

    if (conflictingEvents.isEmpty) {
      // Early out
      return
    }

    CdpConnector.post(apiKey,
      EventsRelation.baseEventsURLOld(project).addPathSegment("update").build(),
      items=conflictingEvents,
      wantAsync=true,
      successCallback = Some(_ => {
        if (collectMetrics) {
          eventsCreated.inc(eventItems.length)
        }
        remainingRequests.countDown()
      }),
      failureCallback = Some(_ => {
        remainingRequests.countDown()
        FailureCallbackStatus.Unhandled
      }))(Encoder[EventItem])
  }
}

object EventsRelation {
  def baseEventsURL(project: String): HttpUrl.Builder = {
    CdpConnector.baseUrl(project, "0.6")
      .addPathSegment("events")
  }

  def baseEventsURLOld(project: String): HttpUrl.Builder = {
    // TODO: API is failing with "Invalid field - items[0].starttime - expected an object but got number" in 0.6
    // That's why we need 0.5 support, however should be removed when fixed
    CdpConnector.baseUrl(project, "0.5")
      .addPathSegment("events")
  }
}
