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
import org.apache.spark.rdd.RDD

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
    sourceId: Option[String],
    createdTime: Long,
    lastUpdatedTime: Long)

case class UpdateEventItem(
    id: Option[Long],
    startTime: Option[Setter[Long]],
    endTime: Option[Setter[Long]],
    description: Option[Setter[String]],
    `type`: Option[Setter[String]],
    subType: Option[Setter[String]],
    metadata: Option[Setter[Map[String, Option[String]]]],
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
      Setter[Map[String, Option[String]]](eventItem.metadata),
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
    with PrunedFilteredScan
    with CdpConnector {

  @transient lazy private val eventsCreated = metricsSource.getOrCreateCounter(s"events.created")
  @transient lazy private val eventsRead = metricsSource.getOrCreateCounter(s"events.read")

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
      post(config.apiKey, baseEventsURL(config.project), newEvents, config.maxRetries)
    }

    (postUpdate, postNewItems).parMapN((_, _) => ())
  }

  sealed case class TypeFilter(`type`: String)

  // scalastyle:off cyclomatic.complexity
  def getTypeFilters(filter: Filter): Seq[TypeFilter] =
    filter match {
      case IsNotNull("type") => Seq()
      case EqualTo("type", value) => Seq(TypeFilter(value.toString))
      case EqualNullSafe("type", value) => Seq(TypeFilter(value.toString))
      case In("type", values) => values.map(v => TypeFilter(v.toString))
      case And(f1, f2) => getTypeFilters(f1) ++ getTypeFilters(f2)
      case Or(f1, f2) => getTypeFilters(f1) ++ getTypeFilters(f2)
      // TODO: add support for String filtering using the 'Search for events' endpoint
      case StringStartsWith("type", value) =>
        sys.error(
          s"Filtering using 'string starts with' not allowed for events, attempted for ${value.toString}")
      case StringEndsWith("type", value) =>
        sys.error(
          s"Filtering using 'string ends with' not allowed for events, attempted for ${value.toString}")
      case StringContains("type", value) =>
        sys.error(
          s"Filtering using 'string contains' not allowed for data points, attempted for ${value.toString}")
      case _ =>
        Seq()
    }

  sealed case class SubTypeFilter(subType: String)

  def getSubTypeFilters(filter: Filter): Seq[SubTypeFilter] =
    filter match {
      case IsNotNull("subtype") => Seq()
      case EqualTo("subtype", value) => Seq(SubTypeFilter(value.toString))
      case EqualNullSafe("subtype", value) => Seq(SubTypeFilter(value.toString))
      case In("subtype", values) => values.map(v => SubTypeFilter(v.toString))
      case And(f1, f2) => getSubTypeFilters(f1) ++ getSubTypeFilters(f2)
      case Or(f1, f2) => getSubTypeFilters(f1) ++ getSubTypeFilters(f2)
      // TODO: add support for String filtering using the 'Search for events' endpoint
      case StringStartsWith("subtype", value) =>
        sys.error(
          s"Filtering using 'string starts with' not allowed for events, attempted for ${value.toString}")
      case StringEndsWith("subtype", value) =>
        sys.error(
          s"Filtering using 'string ends with' not allowed for events, attempted for ${value.toString}")
      case StringContains("subtype", value) =>
        sys.error(
          s"Filtering using 'string contains' not allowed for data points, attempted for ${value.toString}")
      case _ =>
        Seq()
    }
  // scalastyle:on cyclomatic.complexity
  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val types = filters.flatMap(getTypeFilters).map(_.`type`)
    val subTypes = filters.flatMap(getSubTypeFilters).map(_.subType)

    val params = Map("type" ->types.toString, "subtype"->subTypes.toString)

    val rdds = for {
      eventType <- if (types.isEmpty) { Array(None) } else { types}
      eventSubType <- if (subTypes.isEmpty) { Array(None) } else { subTypes }
    } yield {
      val urlWithType =
        eventType match {
        case t: String => listUrl(config).param("type", t.toString)
        case None => listUrl(config)
      }

      val urlWithTypeAndSubType = {
        eventSubType match {
          case et: String => urlWithType.param("subtype", et.toString)
          case None => urlWithType
        }
      }

      CdpRdd[EventItem](
        sqlContext.sparkContext,
        (e: EventItem) => {
          if (config.collectMetrics) {
            eventsRead.inc()
          }
          toRow(e, requiredColumns)
        },
        cursorsUrl(config),
        urlWithTypeAndSubType,
        config
      )
    }

    rdds.foldLeft(sqlContext.sparkContext.emptyRDD[Row])((a, b) => a.union(b))
  }

  override def baseUrl(project: String, version: String, baseUrl: String): Uri =
    super.baseUrl(project, version, baseUrl)

  def baseEventsURL(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/events"

  override def schema: StructType = structType[EventItem]

  override def toRow(t: EventItem): Row = asRow(t)

  def toRow(t: EventItem, requiredColumns: Array[String]): Row = {
    val values = t.productIterator
    val eventMap = t.getClass.getDeclaredFields.map(_.getName -> values.next).toMap

    Row.fromSeq(requiredColumns.map(eventMap(_)).toSeq)

  }

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/events"

  override def cursorsUrl(relationConfig: RelationConfig): Uri =
    listUrl(relationConfig).param("onlyCursors", "true")
}
