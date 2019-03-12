package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.circe.generic.auto._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import io.circe.parser.decode
import com.softwaremill.sttp._
import SparkSchemaHelper._

import scala.concurrent.ExecutionContext

case class EventItem(
    id: Option[Long],
    startTime: Option[Long],
    endTime: Option[Long],
    description: Option[String],
    `type`: Option[String],
    subtype: Option[String],
    metadata: Option[Map[String, Option[String]]],
    assetIds: Option[Seq[Long]],
    source: Option[String],
    sourceId: Option[String])

case class SourceWithResourceId(id: Long, source: String, sourceId: String)
case class EventConflict(duplicates: Seq[SourceWithResourceId])

class EventsRelation(config: RelationConfig)(@transient val sqlContext: SQLContext)
    extends CdpRelation[EventItem](config, "events")
    with InsertableRelation
    with CdpConnector {
  @transient lazy private val eventsCreated = metricsSource.getOrCreateCounter(s"events.created")

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize).toVector
      batches.parTraverse(postEvent).unsafeRunSync()
    })

  def postEvent(rows: Seq[Row]): IO[Unit] = {
    val eventItems = rows.map(r => fromRow[EventItem](r))

    postOr(config.apiKey, baseEventsURL(config.project), eventItems, config.maxRetries) {
      case Response(Right(body), StatusCodes.Conflict, _, _, _) =>
        decode[Error[EventConflict]](body) match {
          case Right(conflict) => resolveConflict(eventItems, conflict.error)
          case Left(error) => IO.raiseError(error)
        }
    }.flatTap { _ =>
      IO {
        if (config.collectMetrics) {
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
      post(
        config.apiKey,
        uri"${baseEventsURLOld(config.project)}/update",
        conflictingEvents,
        config.maxRetries)
    }

    val newEvents = eventItems.map(_.copy(id = None)).diff(conflictingEvents.map(_.copy(id = None)))
    val postNewItems = if (newEvents.isEmpty) {
      IO.unit
    } else {
      post(config.apiKey, baseEventsURL(config.project), newEvents, config.maxRetries)
    }

    (postUpdate, postNewItems).parMapN((_, _) => ())
  }

  def baseEventsURL(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/events"

  def baseEventsURLOld(project: String): Uri =
    // TODO: API is failing with "Invalid field - items[0].starttime - expected an object but got number" in 0.6
    // That's why we need 0.5 support, however should be removed when fixed
    uri"${config.baseUrl}/api/0.5/projects/$project/events"

  override def schema: StructType = structType[EventItem]

  override def toRow(t: EventItem): Row = asRow(t)

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/events"

  override def cursorsUrl(relationConfig: RelationConfig): Uri =
    listUrl(relationConfig).param("onlyCursors", "true")
}
