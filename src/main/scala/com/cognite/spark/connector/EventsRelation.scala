package com.cognite.spark.connector

import cats.effect.IO
import cats.implicits._
import io.circe.generic.auto._
import okhttp3.HttpUrl
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.http4s.Status.Conflict
import org.http4s.circe.CirceEntityCodec._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

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
      val batches = rows.grouped(batchSize).toVector
      val batchPosts = fs2.async.parallelTraverse(batches)(postEvent)
      if (batchPosts.unsafeRunTimed(10.minutes).isEmpty) {
        throw new RuntimeException("Posting events timed out after 10 minutes.")
      }
    })
  }

  override def buildScan(): RDD[Row] = {
    val finalRows = CdpConnectorV2.get[EventItem](apiKey, EventsRelation.baseEventsURL(project).build(), batchSize, limit)
      .map(item => Row(item.id, item.startTime, item.endTime, item.description,
          item.`type`, item.subtype, item.metadata, item.assetIds, item.source, item.sourceId))
      .map(tap(_ => {
        if (collectMetrics) {
          eventsRead.inc()
        }
      }))
    sqlContext.sparkContext.parallelize(finalRows.toStream)
  }

  def postEvent(rows: Seq[Row]): IO[Unit] = {
    val eventItems = rows.map(r =>
      EventItem(Option(r.getAs(0)), Option(r.getAs(1)), Option(r.getAs(2)), Option(r.getString(3)),
        Option(r.getAs(4)), Option(r.getAs(5)), Option(r.getAs(6)),
        Option(r.getAs(7)), Option(r.getAs(8)), Option(r.getAs(9)))
    )

    CdpConnectorV2.postOr(apiKey, EventsRelation.baseEventsURL(project).build(), items = eventItems) {
      case Conflict(resp) => resp.as[Error[EventConflict]]
        .flatMap(conflict => resolveConflict(eventItems, conflict.error))
    }
    .map(tap(_ => {
      if (collectMetrics) {
        eventsCreated.inc(rows.length)
      }
    }))
  }

  def resolveConflict(eventItems: Seq[EventItem], eventConflict: EventConflict): IO[Unit] = {
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
      IO.unit
    } else {
      CdpConnectorV2.post(apiKey,
        EventsRelation.baseEventsURLOld(project).addPathSegment("update").build(),
        items = conflictingEvents)
    }
  }

  def tap[A](effect: A => Unit)(x: A): A = {
    effect(x)
    x
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
