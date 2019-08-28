package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.{Event, EventUpdate, GenericClient}
import com.cognite.sdk.scala.v1.resources.Events
import com.cognite.spark.datasource.SparkSchemaHelper.{asRow, fromRow, structType}
import com.softwaremill.sttp.Uri
import com.softwaremill.sttp._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.InsertableRelation
import io.circe.generic.auto._
import org.apache.spark.sql.types.{DataTypes, StructType}
import cats.implicits._
import com.cognite.sdk.scala.common.CdpApiException

class EventsRelationV1(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Event, Events[IO], EventItem](config, "events")
    with InsertableRelation {

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
    import CdpConnector.cs
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

  override def clientToResource(client: GenericClient[IO, Nothing]): Events[IO] =
    client.events

  override def listUrl(version: String): Uri =
    uri"${config.baseUrl}/api/$version/projects/${config.project}/events"

  val cursorsUrl = uri"${listUrl("0.6")}/cursors"

  override def cursors(): Iterator[(Option[String], Option[Int])] =
    CursorsCursorIterator(cursorsUrl.param("divisions", config.partitions.toString), config)
}
