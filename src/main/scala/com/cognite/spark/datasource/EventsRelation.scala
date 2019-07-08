package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.concurrent.ExecutionContext
import scala.util.Try

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

case class EventId(id: Long)
case class IdSourceAndResourceId(id: Long, source: String, sourceId: String)
case class EventConflict(duplicates: Seq[IdSourceAndResourceId])

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

  override val fieldsWithPushdownFilter: Seq[String] =
    Seq("type", "subtype", "assetIds", "startTime", "source")

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val otherFilters = urlsWithFilters(filters, listUrl())
    val idFilters = urlWithBody(filters)

    if (idFilters.nonEmpty) {
      val byIds = CdpEventsByIdsRdd(
        sqlContext.sparkContext,
        (e: EventItem) => {
          if (config.collectMetrics) {
            itemsRead.inc()
          }
          toRow(e, requiredColumns)
        },
        config,
        byIdsUrl(),
        idFilters
      )

      val hasOtherFilters = otherFilters.headOption match {
        case Some(head) => head != listUrl()
        case None => false
      }

      // If we have filters both on eventId and other filters
      // then we must do requests for both and join them
      if (hasOtherFilters) {
        val other = super.buildScan(requiredColumns, filters)
        other ++ byIds
      } else { byIds }
    } else {
      super.buildScan(requiredColumns, filters)
    }
  }

  def urlWithBody(filters: Array[Filter]): Seq[EventId] =
    for {
      filter <- filters
      id <- getFilter(filter, "id")
    } yield EventId(id.toLong)

  override def urlsWithFilters(filters: Array[Filter], uri: Uri): Seq[Uri] = {
    val filterMaps =
      fieldsWithPushdownFilter
        .map(col => (col, filters.flatMap(getFilter(_, col))))
        .filter(_._2.nonEmpty)
        .toMap
    val assetIdsFilterOpt = filterMaps.get("assetIds")
    val typeFilterOpt = filterMaps.get("type")
    val subtypeFilterOpt = filterMaps.get("subtype")
    val sourceFilterOpt = filterMaps.get("source")
    val startTimeFilters = getStartTimeFilters(filters)

    val assetIds = getAssetIdsUrls(uri, assetIdsFilterOpt)
    val subtypesAndTypes = getTypeAndSubtypeUrls(uri, typeFilterOpt, subtypeFilterOpt)
    val startTimes = getStartTimeUrls(uri, startTimeFilters)
    val sources = getSourceUrls(uri, sourceFilterOpt)

    val res = assetIds ++ subtypesAndTypes ++ startTimes ++ sources
    if (res.isEmpty) Seq(uri) else res
  }

  private def getAssetIdsUrls(uri: Uri, assetIdsFiltersOpt: Option[Array[String]]): Seq[Uri] =
    assetIdsFiltersOpt match {
      case Some(filters) =>
        val ids = filters.flatMap(_.split("\\D+").filter(_.nonEmpty))
        ids.map(uri.param("assetId", _)) // Endpoint uses singular
      case None => Seq()
    }

  private def getStartTimeUrls(
      uri: Uri,
      startTimeFilters: (Option[String], Option[String])): Seq[Uri] = startTimeFilters match {
    case (Some(minFilter), None) => Seq(uri.param("minStartTime", minFilter))
    case (None, Some(maxFilter)) => Seq(uri.param("maxStartTime", maxFilter))
    case (Some(minFilter), Some(maxFilter)) =>
      val urlWithMinFilter = uri.param("minStartTime", minFilter)
      Seq(urlWithMinFilter.param("maxStartTime", maxFilter))
    case (None, None) => Seq()
  }

  private def getSourceUrls(uri: Uri, sourceFilterOpt: Option[Array[String]]): Seq[Uri] =
    sourceFilterOpt match {
      case Some(sourceFilter) => sourceFilter.map(p => uri.param("source", p)).toSeq
      case None => Seq()
    }

  private def getStartTimeFilters(filters: Array[Filter]): (Option[String], Option[String]) = {
    val startTimeFilters = filters.flatMap(getStartTimeFilter)

    Tuple2(
      // Note that this way of aggregating filters will not work with "Or" predicates.
      Try(startTimeFilters.filter(_.isInstanceOf[Min]).max).toOption.map(_.value.toString),
      Try(startTimeFilters.filter(_.isInstanceOf[Max]).min).toOption.map(_.value.toString)
    )
  }

  private def getTypeAndSubtypeUrls(
      uri: Uri,
      typeFilterOpt: Option[Array[String]],
      subtypeFilterOpt: Option[Array[String]]): Seq[Uri] =
    (typeFilterOpt, subtypeFilterOpt) match {
      case (Some(typeFilter), Some(subtypeFilter)) =>
        val typeAndSubtypes = for {
          _type <- typeFilter
          subtype <- subtypeFilter
        } yield (_type, subtype)
        typeAndSubtypes.toSeq.map { p =>
          uri.param("type", p._1).param("subtype", p._2)
        }
      case (Some(typeFilter), None) => typeFilter.map(p => uri.param("type", p)).toSeq
      case (None, Some(_)) =>
        throw new IllegalArgumentException("Type must be set when filtering on sub-type.")
      case (None, None) => Seq()
    }

  private def getStartTimeFilter(filter: Filter): Seq[Limit] =
    filter match {
      case LessThan("startTime", value) =>
        Seq(Max(value.toString.toLong - 1)) // end point is inclusive
      case LessThanOrEqual("startTime", value) => Seq(Max(value.toString.toLong))
      case GreaterThan("startTime", value) =>
        Seq(Min(value.toString.toLong + 1)) // end point is inclusive
      case GreaterThanOrEqual("startTime", value) => Seq(Min(value.toString.toLong))
      case And(f1, f2) => getStartTimeFilter(f1) ++ getStartTimeFilter(f2)
      case _ => Seq()
    }

  def baseEventsURL(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/events"

  override def schema: StructType = structType[EventItem]

  override def toRow(t: EventItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/events"

  private def byIdsUrl(): Uri = uri"${listUrl().toString()}/byids"
  private val cursorsUrl = uri"${config.baseUrl}/api/0.6/projects/${config.project}/events/cursors"
  override def cursors(): Iterator[(Option[String], Option[Int])] =
    CursorsCursorIterator(cursorsUrl.param("divisions", config.partitions.toString), config)
}

object EventsRelation extends DeleteSchema with UpsertSchema with InsertSchema with UpdateSchema {
  val insertSchema: StructType = structType[PostEventItem]
  val upsertSchema: StructType = structType[PostEventItem]
  val updateSchema: StructType = StructType(structType[EventItem].filterNot(field =>
    Seq("createdTime", "lastUpdatedTime").contains(field.name)))
}
