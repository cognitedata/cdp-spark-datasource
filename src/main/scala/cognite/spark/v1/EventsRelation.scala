package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.v1.{Event, EventCreate, EventUpdate, EventsFilter, GenericClient}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import com.cognite.sdk.scala.common.{WithExternalId, WithId}
import PushdownUtilities._
import com.cognite.sdk.scala.v1.resources.Events
import fs2.Stream

class EventsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Event, Long](config, "events")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, Event]] = {
    val fieldNames =
      Array(
        "source",
        "type",
        "subtype",
        "assetIds",
        "minStartTime",
        "maxStartTime",
        "minEndTime",
        "maxEndTime",
        "minCreatedTime",
        "maxCreatedTime",
        "minLastUpdatedTime",
        "maxLastUpdatedTime"
      )
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val shouldGetAllRows = shouldGetAll(pushdownFilterExpression, fieldNames)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val eventsFilterSeq = if (filtersAsMaps.isEmpty || shouldGetAllRows) {
      Seq(EventsFilter())
    } else {
      filtersAsMaps.distinct.map(eventsFilterFromMap)
    }

    val streamsPerFilter = eventsFilterSeq
      .map { f =>
        client.events.filterPartitions(f, numPartitions, limit)
      }

    // Merge streams related to each partition to make sure duplicate values are read into
    // the same RDD partition
    streamsPerFilter.transpose
      .map(s => s.reduce(_.merge(_)))
  }

  def eventsFilterFromMap(m: Map[String, String]): EventsFilter =
    EventsFilter(
      source = m.get("source"),
      `type` = m.get("type"),
      subtype = m.get("subtype"),
      startTime = timeRangeFromMinAndMax(m.get("minStartTime"), m.get("maxStartTime")),
      endTime = timeRangeFromMinAndMax(m.get("minEndTime"), m.get("maxEndTime")),
      assetIds = m.get("assetIds").map(assetIdsFromWrappedArray),
      createdTime = timeRangeFromMinAndMax(m.get("minCreatedTime"), m.get("maxCreatedTime")),
      lastUpdatedTime = timeRangeFromMinAndMax(m.get("minLastUpdatedTime"), m.get("maxLastUpdatedTime"))
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val events = fromRowWithFilteredMetadata(rows)
    client.events
      .create(events)
      .flatTap(_ => incMetrics(itemsCreated, events.size)) *> IO.unit
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val eventUpdates = rows.map(r => fromRow[EventsUpsertSchema](r))
    updateByIdOrExternalId[EventsUpsertSchema, EventUpdate, Events[IO], Event](
      eventUpdates,
      client.events
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => fromRow[DeleteItem](r))
    deleteWithIgnoreUnknownIds(client.events, deletes, config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val events = rows.map { r =>
      val event = fromRow[EventsUpsertSchema](r)
      event.copy(metadata = filterMetadata(event.metadata))
    }
    val (eventsToUpdate, eventsToCreate) = events.partition(r => r.id.exists(_ > 0))

    // In each batch we must not have duplicated external IDs.
    // TODO: Is the same true for normal IDs?
    val eventsToCreateWithoutDuplicatesByExternalId = eventsToCreate
      .groupBy(_.externalId)
      .flatMap {
        case (None, events) => events
        case (Some(_), events) => events.take(1)
      }
      .toSeq

    val update = updateByIdOrExternalId[EventsUpsertSchema, EventUpdate, Events[IO], Event](
      eventsToUpdate,
      client.events
    )
    val createOrUpdate = createOrUpdateByExternalId[Event, EventUpdate, EventCreate, Events[IO]](
      Seq.empty,
      eventsToCreateWithoutDuplicatesByExternalId.map(_.transformInto[EventCreate]),
      client.events,
      doUpsert = true)
    (update, createOrUpdate).parMapN((_, _) => ())
  }

  def fromRowWithFilteredMetadata(rows: Seq[Row]): Seq[EventCreate] =
    rows.map { r =>
      val event = fromRow[EventCreate](r)
      event.copy(metadata = filterMetadata(event.metadata))
    }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val events = fromRowWithFilteredMetadata(rows)

    createOrUpdateByExternalId[Event, EventUpdate, EventCreate, Events[IO]](
      Seq.empty,
      events,
      client.events,
      doUpsert = true)
  }

  override def schema: StructType = structType[Event]

  override def toRow(a: Event): Row = asRow(a)

  override def uniqueId(a: Event): Long = a.id
}
object EventsRelation extends UpsertSchema {
  val upsertSchema = structType[EventsUpsertSchema]
  val insertSchema = structType[EventsInsertSchema]
  val readSchema = structType[EventsReadSchema]
}

case class EventsUpsertSchema(
    id: Option[Long] = None,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    description: Option[String] = None,
    `type`: Option[String] = None,
    subtype: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    source: Option[String] = None,
    externalId: Option[String] = None
) extends WithExternalId
    with WithId[Option[Long]]

case class EventsInsertSchema(
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    description: Option[String] = None,
    `type`: Option[String] = None,
    subtype: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    source: Option[String] = None,
    externalId: Option[String] = None
)

case class EventsReadSchema(
    id: Long = 0,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    description: Option[String] = None,
    `type`: Option[String] = None,
    subtype: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    source: Option[String] = None,
    externalId: Option[String] = None,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0)
)
