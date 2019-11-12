package cognite.spark.v1

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{
  GenericClient,
  TimeSeries,
  TimeSeriesCreate,
  TimeSeriesFilter,
  TimeSeriesUpdate
}
import cognite.spark.v1.SparkSchemaHelper._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import PushdownUtilities._
import fs2.Stream
import io.scalaland.chimney.dsl._
import scala.concurrent.ExecutionContext

class TimeSeriesRelation(config: RelationConfig, useLegacyName: Boolean)(val sqlContext: SQLContext)
    extends SdkV1Relation[TimeSeries, Long](config, "timeseries")
    with InsertableRelation {
  @transient implicit lazy val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

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
      val timeSeries = fromRow[TimeSeriesCreate](r)
      val timeSeriesWithMetadata = timeSeries.copy(metadata = filterMetadata(timeSeries.metadata))
      if (useLegacyName) {
        timeSeriesWithMetadata.copy(legacyName = timeSeries.name)
      } else {
        timeSeriesWithMetadata
      }
    }

    client.timeSeries
      .create(timeSeriesSeq)
      .handleErrorWith {
        case e: CdpApiException =>
          if (e.code == 409) {
            val legacyNameConflicts = e.duplicated.get.flatMap(j => j("legacyName")).map(_.asString.get)
            if (legacyNameConflicts.nonEmpty) {
              throw new IllegalArgumentException(
                "Found legacyName conflicts, upserts are not supported with legacyName." +
                  s" Conflicting legacyNames: ${legacyNameConflicts.mkString(", ")}")
            }
            val existingExternalIds =
              e.duplicated.get.map(j => j("externalId").get.asString.get)
            resolveConflict(existingExternalIds, timeSeriesSeq)
          } else { IO.raiseError(e) }
      } *> IO.unit
  }

  def resolveConflict(
      existingExternalIds: Seq[String],
      timeSeriesSeq: Seq[TimeSeriesCreate]): IO[Unit] = {
    import CdpConnector.cs
    val (timeSeriesToUpdate, timeSeriesToCreate) = timeSeriesSeq.partition(
      p => if (p.externalId.isEmpty) { false } else { existingExternalIds.contains(p.externalId.get) }
    )

    val idMap = client.timeSeries
      .retrieveByExternalIds(existingExternalIds)
      .map(_.map(ts => ts.externalId -> ts.id).toMap)

    val create =
      if (timeSeriesToCreate.isEmpty) { IO.unit } else {
        client.timeSeries.create(timeSeriesToCreate)
      }
    val update =
      if (timeSeriesToUpdate.isEmpty) { IO.unit } else {
        idMap.flatMap(idMap =>
          client.timeSeries.update(timeSeriesToUpdate.map(ts =>
            ts.transformInto[TimeSeriesUpdate].copy(id = idMap(ts.externalId)))))
      }

    (create, update).parMapN((_, _) => ())
  }

  override def schema: StructType = structType[TimeSeries]

  override def toRow(t: TimeSeries): Row = asRow(t)

  override def uniqueId(a: TimeSeries): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, TimeSeries]] = {

    val fieldNames = Array("name", "unit", "isStep", "isString", "assetId")
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val shouldGetAllRows = shouldGetAll(pushdownFilterExpression, fieldNames)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)
    val timeSeriesFilterSeq = if (filtersAsMaps.isEmpty || shouldGetAllRows) {
      Seq(TimeSeriesFilter())
    } else {
      filtersAsMaps.distinct.map(timeSeriesFilterFromMap)
    }

    val streamsPerFilter = timeSeriesFilterSeq
      .map { f =>
        client.timeSeries.filterPartitions(f, numPartitions, limit)
      }

    // Merge streams related to each partition to make sure duplicate values are read into
    // the same RDD partition
    streamsPerFilter.transpose
      .map(s => s.reduce(_.merge(_)))
  }
  def timeSeriesFilterFromMap(m: Map[String, String]): TimeSeriesFilter =
    TimeSeriesFilter(
      name = m.get("name"),
      unit = m.get("unit"),
      isStep = m.get("isStep").map(_.toBoolean),
      isString = m.get("isString").map(_.toBoolean),
      assetIds = m.get("assetId").map(assetIdsFromWrappedArray)
    )
}
object TimeSeriesRelation extends UpsertSchema {
  val upsertSchema = structType[TimeSeriesCreate]
}
