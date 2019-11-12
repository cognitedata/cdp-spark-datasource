package cognite.spark.v1

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.sdk.scala.v1.{Event, EventCreate, EventUpdate, EventsFilter, GenericClient}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}
import com.cognite.sdk.scala.common.CdpApiException
import PushdownUtilities._
import fs2.Stream
import io.scalaland.chimney.dsl._

import scala.concurrent.ExecutionContext

class EventsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Event, Long](config, "events")
    with InsertableRelation {
  @transient implicit lazy val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

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
        "maxEndTime")
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
      assetIds = m.get("assetIds").map(assetIdsFromWrappedArray)
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val events = fromRowWithFilteredMetadata(rows)
    client.events.create(events) *> IO.unit
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val events = rows.map(r => fromRow[EventUpdate](r))
    client.events.update(events) *> IO.unit
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val ids = rows.map(r => fromRow[DeleteItem](r).id)
    client.events.deleteByIds(ids)
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

  def resolveConflict(existingExternalIds: Seq[String], events: Seq[EventCreate]): IO[Unit] = {
    val (eventsToUpdate, eventsToCreate) = events.partition(
      p => if (p.externalId.isEmpty) { false } else { existingExternalIds.contains(p.externalId.get) }
    )

    val idMap = client.events
      .retrieveByExternalIds(existingExternalIds)
      .map(_.map(e => e.externalId -> e.id).toMap)

    val create =
      if (eventsToCreate.isEmpty) IO.unit else client.events.create(eventsToCreate)
    val update =
      if (eventsToUpdate.isEmpty) { IO.unit } else {
        idMap.flatMap(idMap =>
          client.events.update(eventsToUpdate.map(e =>
            e.transformInto[EventUpdate].copy(id = idMap(e.externalId)))))
      }

    (create, update).parMapN((_, _) => ())
  }

  override def schema: StructType = structType[Event]

  override def toRow(a: Event): Row = asRow(a)

  override def uniqueId(a: Event): Long = a.id
}
object EventsRelation extends UpsertSchema {
  val upsertSchema = structType[EventCreate]
}
