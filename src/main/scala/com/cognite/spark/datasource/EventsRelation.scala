package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp.{Response, Uri, _}
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.concurrent.ExecutionContext

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
                     maxRetriesOption: Option[Int],
                     metricsPrefix: String,
                     collectMetrics: Boolean)
                    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with CdpConnector
    with Serializable {

  @transient lazy private val batchSize: Int = batchSizeOption.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = maxRetriesOption.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val eventsCreated = UserMetricsSystem.counter(s"${metricsPrefix}events.created")
  @transient lazy private val eventsRead = UserMetricsSystem.counter(s"${metricsPrefix}events.read")

  override val schema: StructType =
    StructType(Seq(
      StructField("id", LongType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("description", StringType),
      StructField("type", StringType),
      StructField("subtype", StringType),
      StructField("metadata", MapType(DataTypes.StringType, DataTypes.StringType, valueContainsNull = true)),
      StructField("assetIds", ArrayType(LongType)),
      StructField("source", StringType),
      StructField("sourceId", StringType)))

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(postEvent).unsafeRunSync()
    })
  }

  override def buildScan(): RDD[Row] = {
    val baseUrl = baseEventsURL(project)
    CdpRdd[EventItem](sqlContext.sparkContext,
      (e: EventItem) => {
        if (collectMetrics) {
          eventsRead.inc()
        }
        Row(e.id, e.startTime, e.endTime, e.description,
          e.`type`, e.subtype, e.metadata, e.assetIds, e.source, e.sourceId)
      },
      baseUrl.param("onlyCursors", "true"), baseUrl, apiKey, project, batchSize, maxRetries, limit)
  }

  def postEvent(rows: Seq[Row]): IO[Unit] = {
    val eventItems = rows.map(r =>
      EventItem(
        Option(r.getAs(0)),
        Option(r.getAs(1)),
        Option(r.getAs(2)),
        Option(r.getAs(3)),
        Option(r.getAs(4)),
        Option(r.getAs(5)),
        Option(r.getAs[Map[String, String]](6)
          // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
          // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
          // Since null keys don't make sense to CDP either, remove them as well.
          .filter { case (k, v) => k != null && v != null }), // scalastyle:ignore null
        Option(r.getAs(7)),
        Option(r.getAs(8)),
        Option(r.getAs(9)))
    )

    postOr(apiKey, baseEventsURL(project), eventItems, maxRetries) {
      case Response(Right(body), StatusCodes.Conflict, _, _, _) =>
        decode[Error[EventConflict]](body) match {
          case Right(conflict) => resolveConflict(eventItems, conflict.error)
          case Left(error) => IO.raiseError(error)
        }
      }.flatTap { _ =>
        IO {
          if (collectMetrics) {
            eventsCreated.inc(rows.length)
          }
        }
      }
  }

  def resolveConflict(eventItems: Seq[EventItem], eventConflict: EventConflict): IO[Unit] = {
    // not totally sure if this needs to be here, instead of being a @transient private implicit val,
    // but we saw some strange errors about it not being serializable (which should be fixed with the
    // @transient annotation). leaving it here for now, but should double check this in the future.
    // shouldn't do any harm to have it here, but it's a bit too unusual.
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
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

    val postUpdate = if (conflictingEvents.isEmpty) {
      // Do nothing
      IO.unit
    } else {
      post(apiKey, uri"${baseEventsURLOld(project)}/update", conflictingEvents, maxRetries)
    }

    val newEvents = eventItems.map(_.copy(id = None)).diff(conflictingEvents.map(_.copy(id = None)))
    val postNewItems = if (newEvents.isEmpty) {
      IO.unit
    } else {
      post(apiKey, baseEventsURL(project), newEvents, maxRetries)
    }

    (postUpdate, postNewItems).parMapN((_, _) => ())
  }

  def baseEventsURL(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/events"
  }

  def baseEventsURLOld(project: String): Uri = {
    // TODO: API is failing with "Invalid field - items[0].starttime - expected an object but got number" in 0.6
    // That's why we need 0.5 support, however should be removed when fixed
    uri"https://api.cognitedata.com/api/0.5/projects/$project/events"
  }
}
