package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

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
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

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
    assetIds: Option[Map[String, Seq[Long]]],
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
      eventItem.assetIds.map(a => Map("set" -> a)),
      Setter[String](eventItem.source),
      Setter[String](eventItem.sourceId)
    )
}

case class SourceWithResourceId(id: Long, source: String, sourceId: String)
case class EventConflict(duplicates: Seq[SourceWithResourceId])

class EventsRelation(config: RelationConfig)(@transient val sqlContext: SQLContext)
    extends CdpRelation[EventItem](config, "events")
    with InsertableRelation
    with PrunedFilteredScan {
  @transient lazy private val eventsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"events.created")
  @transient lazy private val eventsRead =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"events.read")

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val postEventItems = rows.map(r => fromRow[PostEventItem](r))
    post(config, baseEventsURL(config.project), postEventItems)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = postEvents(rows)

  override def update(rows: Seq[Row]): IO[Unit] = {
    val updateEventItems = rows.map(r => UpdateEventItem(fromRow[EventItem](r)))

    // Events must have an id when using update
    if (updateEventItems.exists(_.id.isEmpty)) {
      throw new IllegalArgumentException("Events must have an id when using update")
    }

    post(config, uri"${baseEventsURL(config.project)}/update", updateEventItems)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup.parTraverse(postEvents).unsafeRunSync()
      }
      ()
    })

  override def delete(rows: Seq[Row]): IO[Unit] =
    deleteItems(config, baseEventsURL(config.project), rows)

  def postEvents(rows: Seq[Row]): IO[Unit] = {
    val eventItems = rows.map { r =>
      val eventItem = fromRow[EventItem](r)
      eventItem.copy(metadata = filterMetadata(eventItem.metadata))
    }

    val postEventItems = eventItems.map(e => PostEventItem(e))

    postOr(config, baseEventsURL(config.project), postEventItems) {
      case response @ Response(Right(body), StatusCodes.Conflict, _, _, _) =>
        decode[Error[EventConflict]](body) match {
          case Right(conflict) => resolveConflict(eventItems, conflict.error)
          case Left(_) => IO.raiseError(onError(baseEventsURL(config.project), response))
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
      post(config, uri"${baseEventsURL(config.project)}/update", updateEventItems)
    }

    val newEvents = eventItems.map(_.copy(id = None)).diff(conflictingEvents.map(_.copy(id = None)))
    val postNewItems = if (newEvents.isEmpty) {
      IO.unit
    } else {
      val newPostEventItems = newEvents.map(e => PostEventItem(e))
      post(config, baseEventsURL(config.project), newPostEventItems)
    }

    (postUpdate, postNewItems).parMapN((_, _) => ())
  }

  override val fieldsWithPushdownFilter: Seq[String] = Seq("source", "type")

  def baseEventsURL(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/events"

  override def schema: StructType = structType[EventItem]

  override def toRow(t: EventItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/events"

  private val cursorsUrl = uri"${config.baseUrl}/api/0.6/projects/${config.project}/events/cursors"
  override def cursors(): Iterator[(Option[String], Option[Int])] =
    CursorsCursorIterator(cursorsUrl.param("divisions", config.partitions.toString), config)
}
