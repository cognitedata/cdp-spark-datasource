package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.v1.{Event, EventCreate, EventUpdate, EventsFilter, GenericClient}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import com.cognite.sdk.scala.common.{CdpApiException, WithExternalId, WithId}
import PushdownUtilities._
import com.cognite.sdk.scala.v1.resources.Events
import fs2.Stream
import io.scalaland.chimney.dsl._

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
    client.events.create(events) *> IO.unit
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

  override def upsert(rows: Seq[Row]): IO[Unit] = getFromRowAndCreate(rows)

  def fromRowWithFilteredMetadata(rows: Seq[Row]): Seq[EventCreate] =
    rows.map { r =>
      val event = fromRow[EventCreate](r)
      event.copy(metadata = filterMetadata(event.metadata))
    }
  override def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] = {
    val events = fromRowWithFilteredMetadata(rows)

    client.events
      .create(events)
      .handleErrorWith {
        case e: CdpApiException =>
          if (e.code == 409) {
            val existingExternalIds =
              e.duplicated.get.map(j => j("externalId").get.asString.get)
            resolveConflict(existingExternalIds, events)
          } else { IO.raiseError(e) }
      } *> IO.unit
  }

  def resolveConflict(existingExternalIds: Seq[String], events: Seq[EventCreate]): IO[Unit] =
    upsertAfterConflict[Event, EventUpdate, EventCreate, Events[IO]](
      existingExternalIds,
      events,
      client.events)

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
