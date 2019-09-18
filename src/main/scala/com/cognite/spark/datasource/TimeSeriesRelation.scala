package com.cognite.spark.datasource

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{GenericClient, TimeSeries, TimeSeriesCreate, TimeSeriesUpdate}
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[TimeSeries](config, "timeseries")
    with InsertableRelation {

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesCreates = rows.map { r =>
      val timeSeriesCreate = fromRow[TimeSeriesCreate](r)
      timeSeriesCreate.copy(metadata = filterMetadata(timeSeriesCreate.metadata))
    }
    client.timeSeries.create(timeSeriesCreates) *> IO.unit
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesUpdates = rows.map(r => fromRow[TimeSeriesUpdate](r))
    client.timeSeries.update(timeSeriesUpdates) *> IO.unit
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val ids = rows.map(r => fromRow[DeleteItem](r).id)
    client.timeSeries.deleteByIds(ids) *> IO.unit
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = getFromRowAndCreate(rows)

  override def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesSeq = rows.map { r =>
      val timeSeries = fromRow[TimeSeries](r)
      timeSeries.copy(metadata = filterMetadata(timeSeries.metadata))
    }

    client.timeSeries
      .createFromRead(timeSeriesSeq)
      .handleErrorWith {
        case e: CdpApiException =>
          if (e.code == 409) {
            val existingExternalIds =
              e.duplicated.get.map(j => j("externalId").get.asString.get)
            resolveConflict(existingExternalIds, timeSeriesSeq)
          } else { IO.raiseError(e) }
      } *> IO.unit
  }

  def resolveConflict(
      existingExternalIds: Seq[String],
      timeSeriesSeq: Seq[TimeSeries]): IO[Unit] = {
    import CdpConnector.cs
    val (timeSeriesToUpdate, timeSeriesToCreate) = timeSeriesSeq.partition(
      p => existingExternalIds.contains(p.externalId.get)
    )

    val idMap = client.timeSeries
      .retrieveByExternalIds(existingExternalIds)
      .map(_.map(ts => ts.externalId -> ts.id).toMap)
      .unsafeRunSync()

    val create =
      if (timeSeriesToCreate.isEmpty) { IO.unit } else {
        client.timeSeries.createFromRead(timeSeriesToCreate)
      }
    val update =
      if (timeSeriesToUpdate.isEmpty) { IO.unit } else {
        client.timeSeries.updateFromRead(timeSeriesToUpdate.map(ts =>
          ts.copy(id = idMap(ts.externalId))))
      }

    (create, update).parMapN((_, _) => ())
  }

  def baseTimeSeriesUrl(project: String, version: String = "v1"): Uri =
    uri"${baseUrl(project, version, config.baseUrl)}/timeseries"

  override def schema: StructType = structType[TimeSeries]

  override def toRow(t: TimeSeries): Row = asRow(t)

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Long],
      numPartitions: StatusCode): Seq[fs2.Stream[IO, TimeSeries]] =
    Seq(
      config.limit.map(client.timeSeries.listWithLimit(_)).getOrElse(client.timeSeries.list)
    )
}
object TimeSeriesRelation
    extends DeleteSchema
    with UpsertSchema
    with InsertSchema
//    with UpdateSchema
    {
  val insertSchema = structType[TimeSeriesCreate]
  val upsertSchema = StructType(structType[TimeSeries].filterNot(field =>
    Seq("createdTime", "lastUpdatedTime").contains(field.name)))
//  val updateSchema = StructType(structType[TimeSeriesCreate])
}
