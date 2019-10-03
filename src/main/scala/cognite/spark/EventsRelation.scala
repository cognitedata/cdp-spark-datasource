package cognite.spark

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.sdk.scala.v1.{Event, EventUpdate, EventsFilter, GenericClient, TimeRange}
import cognite.spark.SparkSchemaHelper.{asRow, fromRow, structType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}

import com.cognite.sdk.scala.common.CdpApiException
import PushdownUtilities._
import fs2.Stream

import scala.concurrent.ExecutionContext

class EventsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Event](config, "events")
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

  def timeRangeFromMinAndMax(minTime: Option[String], maxTime: Option[String]): Option[TimeRange] =
    (minTime, maxTime) match {
      case (None, None) => None
      case _ => {
        val minimumTimeAsInstant =
          minTime
            .map(java.sql.Timestamp.valueOf(_).toInstant.plusMillis(1))
            .getOrElse(java.time.Instant.ofEpochMilli(0)) //API does not accept values < 0
        val maximumTimeAsInstant =
          maxTime
            .map(java.sql.Timestamp.valueOf(_).toInstant.minusMillis(1))
            .getOrElse(java.time.Instant.ofEpochMilli(Long.MaxValue))
        Some(TimeRange(minimumTimeAsInstant, maximumTimeAsInstant))
      }
    }

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val events = fromRowWithFilteredMetadata(rows)
    client.events.createFromRead(events) *> IO.unit
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

  def fromRowWithFilteredMetadata(rows: Seq[Row]): Seq[Event] =
    rows.map { r =>
      val event = fromRow[Event](r)
      event.copy(metadata = filterMetadata(event.metadata))
    }
  override def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] = {
    val events = fromRowWithFilteredMetadata(rows)

    client.events
      .createFromRead(events)
      .handleErrorWith {
        case e: CdpApiException =>
          if (e.code == 409) {
            val existingExternalIds =
              e.duplicated.get.map(j => j("externalId").get.asString.get)
            resolveConflict(existingExternalIds, events)
          } else { IO.raiseError(e) }
      } *> IO.unit
  }

  def resolveConflict(existingExternalIds: Seq[String], events: Seq[Event]): IO[Unit] = {
    val (eventsToUpdate, eventsToCreate) = events.partition(
      p => existingExternalIds.contains(p.externalId.get)
    )

    val idMap = client.events
      .retrieveByExternalIds(existingExternalIds)
      .unsafeRunSync()
      .map(e => e.externalId -> e.id)
      .toMap

    val create =
      if (eventsToCreate.isEmpty) IO.unit else client.events.createFromRead(eventsToCreate)
    val update =
      if (eventsToUpdate.isEmpty) { IO.unit } else {
        client.events.updateFromRead(eventsToUpdate.map(e => e.copy(id = idMap(e.externalId))))
      }

    (create, update).parMapN((_, _) => ())
  }

  override def schema: StructType = structType[Event]

  override def toRow(a: Event): Row = asRow(a)

  override def uniqueId(a: Event): Long = a.id

}
