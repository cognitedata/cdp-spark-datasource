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
    metadata: Option[Map[String, String]],
    assetIds: Option[Seq[Long]],
    source: Option[String],
    sourceId: Option[String],
    createdTime: Long,
    lastUpdatedTime: Long)

case class PostEventItem(
    startTime: Option[Long],
    endTime: Option[Long],
    description: Option[String],
    `type`: Option[String],
    subtype: Option[String],
    metadata: Option[Map[String, String]],
    assetIds: Option[Seq[Long]],
    source: Option[String],
    sourceId: Option[String]
)
object PostEventItem {
  def apply(eventItem: EventItem): PostEventItem =
    new PostEventItem(
      eventItem.startTime,
      eventItem.endTime,
      eventItem.description,
      eventItem.`type`,
      eventItem.subtype,
      eventItem.metadata,
      eventItem.assetIds,
      eventItem.source,
      eventItem.sourceId
    )
}
case class UpdateEventItem(
    id: Option[Long],
    startTime: Option[Setter[Long]],
    endTime: Option[Setter[Long]],
    description: Option[Setter[String]],
    `type`: Option[Setter[String]],
    subtype: Option[Setter[String]],
    metadata: Option[Setter[Map[String, String]]],
    assetIds: Option[Setter[Seq[Long]]],
    source: Option[Setter[String]],
    sourceId: Option[Setter[String]]
)
object UpdateEventItem {
  def apply(eventItem: EventItem): UpdateEventItem =
    new UpdateEventItem(
      eventItem.id,
      Setter[Long](eventItem.startTime),
      Setter[Long](eventItem.endTime),
      Setter[String](eventItem.description),
      Setter[String](eventItem.`type`),
      Setter[String](eventItem.subtype),
      Setter[Map[String, String]](eventItem.metadata),
      Setter[Seq[Long]](eventItem.assetIds),
      Setter[String](eventItem.source),
      Setter[String](eventItem.sourceId)
    )
}
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
      val batches = rows.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.parTraverse(postEvent).unsafeRunSync()
    })

  def postEvent(rows: Seq[Row]): IO[Unit] = {
    val eventItems = rows.map { r =>
      val eventItem = fromRow[EventItem](r)
      // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
      // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
      // Since null keys don't make sense to CDP either, remove them as well.
      val filteredMetadata = eventItem.metadata.map(_.filter {
        case (k, v) => k != null && v != null
      })
      eventItem.copy(metadata = filteredMetadata)
    }

    val postEventItems = eventItems.map(e => PostEventItem(e))

    postOr(config.apiKey, baseEventsURL(config.project), postEventItems, config.maxRetries) {
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
      val updateEventItems = conflictingEvents.map(e => UpdateEventItem(e))
      post(
        config.apiKey,
        uri"${baseEventsURL(config.project)}/update",
        updateEventItems,
        config.maxRetries)
    }

    val newEvents = eventItems.map(_.copy(id = None)).diff(conflictingEvents.map(_.copy(id = None)))
    val postNewItems = if (newEvents.isEmpty) {
      IO.unit
    } else {
      val newPostEventItems = newEvents.map(e => PostEventItem(e))
      post(config.apiKey, baseEventsURL(config.project), newPostEventItems, config.maxRetries)
    }

    (postUpdate, postNewItems).parMapN((_, _) => ())
  }

  def baseEventsURL(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/events"

  override def schema: StructType = structType[EventItem]

  override def toRow(t: EventItem): Row = asRow(t)

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/events"

  override def cursorsUrl(relationConfig: RelationConfig): Uri =
    listUrl(relationConfig).param("onlyCursors", "true")
}
