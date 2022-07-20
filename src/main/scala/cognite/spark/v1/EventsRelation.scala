package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.{WithExternalIdGeneric, WithId}
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.Events
import fs2.Stream
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class EventsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Event, Long](config, "events")
    with InsertableRelation
    with WritableRelation {

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, Event]] = {
    val (ids, filters) = pushdownToFilters(sparkFilters, eventsFilterFromMap, EventsFilter())
    executeFilter(client.events, filters, ids, numPartitions, limit)
  }

  def eventsFilterFromMap(m: Map[String, String]): EventsFilter =
    EventsFilter(
      source = m.get("source"),
      `type` = m.get("type"),
      subtype = m.get("subtype"),
      startTime = timeRangeFromMinAndMax(m.get("minStartTime"), m.get("maxStartTime")),
      endTime = timeRangeFromMinAndMax(m.get("minEndTime"), m.get("maxEndTime")),
      assetIds = m.get("assetIds").map(idsFromStringifiedArray),
      createdTime = timeRangeFromMinAndMax(m.get("minCreatedTime"), m.get("maxCreatedTime")),
      lastUpdatedTime = timeRangeFromMinAndMax(m.get("minLastUpdatedTime"), m.get("maxLastUpdatedTime")),
      dataSetIds = m.get("dataSetId").map(idsFromStringifiedArray(_).map(CogniteInternalId(_))),
      externalIdPrefix = m.get("externalIdPrefix")
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val events = rows.map(fromRow[EventCreate](_))
    client.events
      .create(events)
      .flatTap(_ => incMetrics(itemsCreated, events.size)) *> IO.unit
  }

  private def isUpdateEmpty(u: EventUpdate): Boolean = u == EventUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    val eventUpdates = rows.map(r => fromRow[EventsUpsertSchema](r))
    updateByIdOrExternalId[EventsUpsertSchema, EventUpdate, Events[IO], Event](
      eventUpdates,
      client.events,
      isUpdateEmpty
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(fromRow[DeleteItemByCogniteId](_))
    deleteWithIgnoreUnknownIds(client.events, deletes.map(_.toCogniteId), config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    println(s"Coucou events upserts")
    val events = rows.map(fromRow[EventsUpsertSchema](_))

    genericUpsert[Event, EventsUpsertSchema, EventCreate, EventUpdate, Events[IO]](
      events,
      isUpdateEmpty,
      client.events)
  }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val events = rows.map(fromRow[EventCreate](_))

    createOrUpdateByExternalId[Event, EventUpdate, EventCreate, EventCreate, Option, Events[IO]](
      Set.empty,
      events,
      client.events,
      doUpsert = true)
  }

  override def schema: StructType = structType[Event]

  override def toRow(a: Event): Row = asRow(a)

  override def uniqueId(a: Event): Long = a.id
}
object EventsRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[EventsUpsertSchema]
  val insertSchema: StructType = structType[EventsInsertSchema]
  val readSchema: StructType = structType[EventsReadSchema]
}

trait WithNullableExtenalId extends WithExternalIdGeneric[OptionalField] {
  val externalId: OptionalField[String]
  override def getExternalId: Option[String] = externalId.toOption
}

final case class EventsUpsertSchema(
    id: Option[Long] = None,
    startTime: OptionalField[Instant] = FieldNotSpecified,
    endTime: OptionalField[Instant] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    `type`: OptionalField[String] = FieldNotSpecified,
    subtype: OptionalField[String] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    source: OptionalField[String] = FieldNotSpecified,
    externalId: OptionalField[String] = FieldNotSpecified,
    dataSetId: OptionalField[Long] = FieldNotSpecified
) extends WithNullableExtenalId
    with WithId[Option[Long]]

final case class EventsInsertSchema(
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    description: Option[String] = None,
    `type`: Option[String] = None,
    subtype: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    source: Option[String] = None,
    externalId: Option[String] = None,
    dataSetId: Option[Long] = None
)

final case class EventsReadSchema(
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
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    dataSetId: Option[Long] = None
)
