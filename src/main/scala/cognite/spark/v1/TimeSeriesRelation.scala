package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.{WithExternalId, WithId}
import com.cognite.sdk.scala.v1._
import cognite.spark.v1.SparkSchemaHelper._
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import PushdownUtilities._
import com.cognite.sdk.scala.v1.resources.TimeSeriesResource
import fs2.Stream

class TimeSeriesRelation(config: RelationConfig, useLegacyName: Boolean)(val sqlContext: SQLContext)
    extends SdkV1Relation[TimeSeries, Long](config, "timeseries")
    with WritableRelation
    with InsertableRelation {
  import CdpConnector._

  override def insert(rows: Seq[Row]): IO[Unit] =
    getFromRowsAndCreate(rows, doUpsert = false)

  override def update(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesUpdates = rows.map(r => fromRow[TimeSeriesUpsertSchema](r))
    updateByIdOrExternalId[TimeSeriesUpsertSchema, TimeSeriesUpdate, TimeSeriesResource[IO], TimeSeries](
      timeSeriesUpdates,
      client.timeSeries
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val ids = rows.map(r => fromRow[DeleteItem](r).id)
    client.timeSeries.deleteByIds(ids) *> IO.unit
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val timeSeriess = rows.map { r =>
      val timeSeries = fromRow[TimeSeriesUpsertSchema](r)
      timeSeries.copy(metadata = filterMetadata(timeSeries.metadata))
    }
    val (timeSeriesToUpdate, timeSeriesToCreate) = timeSeriess.partition(r => r.id.exists(_ > 0))

    // scalastyle:off no.whitespace.after.left.bracket
    val update = updateByIdOrExternalId[
      TimeSeriesUpsertSchema,
      TimeSeriesUpdate,
      TimeSeriesResource[IO],
      TimeSeries](
      timeSeriesToUpdate,
      client.timeSeries
    )
    val createOrUpdate =
      createOrUpdateByExternalId[TimeSeries, TimeSeriesUpdate, TimeSeriesCreate, TimeSeriesResource[IO]](
        Seq.empty,
        timeSeriesToCreate.map(_.transformInto[TimeSeriesCreate]),
        client.timeSeries,
        doUpsert = true)
    (update, createOrUpdate).parMapN((_, _) => ())
  }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val timeSeriesSeq = rows.map { r =>
      val timeSeries = fromRow[TimeSeriesCreate](r)
      val timeSeriesWithMetadata = timeSeries.copy(metadata = filterMetadata(timeSeries.metadata))
      if (useLegacyName) {
        timeSeriesWithMetadata.copy(legacyName = timeSeries.name)
      } else {
        timeSeriesWithMetadata
      }
    }

    createOrUpdateByExternalId[TimeSeries, TimeSeriesUpdate, TimeSeriesCreate, TimeSeriesResource[IO]](
      Seq.empty,
      timeSeriesSeq,
      client.timeSeries,
      doUpsert = doUpsert)
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
  val upsertSchema = structType[TimeSeriesUpsertSchema]
  val insertSchema = structType[TimeSeriesInsertSchema]
  val readSchema = structType[TimeSeriesReadSchema]
}

case class TimeSeriesUpsertSchema(
    id: Option[Long] = None,
    name: Option[String] = None,
    externalId: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetId: Option[Long] = None,
    description: Option[String] = None,
    securityCategories: Option[Seq[Long]] = None
) extends WithExternalId
    with WithId[Option[Long]]

case class TimeSeriesInsertSchema(
    externalId: Option[String] = None,
    name: Option[String] = None,
    isString: Boolean = false,
    metadata: Option[Map[String, String]] = None,
    unit: Option[String] = None,
    assetId: Option[Long] = None,
    isStep: Boolean = false,
    description: Option[String] = None,
    securityCategories: Option[Seq[Long]] = None
)

case class TimeSeriesReadSchema(
    name: Option[String] = None,
    isString: Boolean = false,
    metadata: Option[Map[String, String]] = None,
    unit: Option[String] = None,
    assetId: Option[Long] = None,
    isStep: Boolean = false,
    description: Option[String] = None,
    securityCategories: Option[Seq[Long]] = None,
    id: Long = 0,
    externalId: Option[String] = None,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0)
)
