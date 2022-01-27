package cognite.spark.v1

import java.time.Instant
import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.WithId
import com.cognite.sdk.scala.v1._
import cognite.spark.v1.SparkSchemaHelper._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import PushdownUtilities._
import com.cognite.sdk.scala.v1.resources.TimeSeriesResource
import fs2.Stream
import io.scalaland.chimney.Transformer

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[TimeSeries, Long](config, "timeseries")
    with WritableRelation
    with InsertableRelation {
  import CdpConnector._

  override def insert(rows: Seq[Row]): IO[Unit] =
    getFromRowsAndCreate(rows, doUpsert = false)

  private def isUpdateEmpty(u: TimeSeriesUpdate): Boolean = u == TimeSeriesUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesUpdates = rows.map(r => fromRow[TimeSeriesUpsertSchema](r))
    updateByIdOrExternalId[TimeSeriesUpsertSchema, TimeSeriesUpdate, TimeSeriesResource[IO], TimeSeries](
      timeSeriesUpdates,
      client.timeSeries,
      isUpdateEmpty
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes: Seq[DeleteItem] = rows.map(fromRow[DeleteItem](_))
    val internalIds = deletes.flatMap(_.id).map(CogniteInternalId.apply)
    val externalIds = deletes.flatMap(_.externalId).map(CogniteExternalId.apply)

    for {
      _ <- deleteWithIgnoreUnknownIds(client.timeSeries, internalIds, config.ignoreUnknownIds)
      _ <- deleteWithIgnoreUnknownIds(client.timeSeries, externalIds, config.ignoreUnknownIds)
    } yield IO.pure(Unit)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val timeSeries = rows.map(fromRow[TimeSeriesUpsertSchema](_))
    // scalastyle:off no.whitespace.after.left.bracket
    genericUpsert[
      TimeSeries,
      TimeSeriesUpsertSchema,
      TimeSeriesCreate,
      TimeSeriesUpdate,
      TimeSeriesResource[IO]](
      timeSeries,
      isUpdateEmpty,
      client.timeSeries
    )
  }

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val timeSeriesSeq = rows.map(fromRow[TimeSeriesCreate](_))

    createOrUpdateByExternalId[
      TimeSeries,
      TimeSeriesUpdate,
      TimeSeriesCreate,
      TimeSeriesCreate,
      Option,
      TimeSeriesResource[IO]](Set.empty, timeSeriesSeq, client.timeSeries, doUpsert = doUpsert)
  }

  override def schema: StructType = structType[TimeSeries]

  override def toRow(t: TimeSeries): Row = asRow(t)

  override def uniqueId(a: TimeSeries): Long = a.id

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, TimeSeries]] = {
    val (ids, filters) = pushdownToFilters(sparkFilters, timeSeriesFilterFromMap, TimeSeriesFilter())
    executeFilter(client.timeSeries, filters, ids, numPartitions, limit)
  }

  def timeSeriesFilterFromMap(m: Map[String, String]): TimeSeriesFilter =
    TimeSeriesFilter(
      name = m.get("name"),
      unit = m.get("unit"),
      isStep = m.get("isStep").map(_.toBoolean),
      isString = m.get("isString").map(_.toBoolean),
      assetIds = m.get("assetId").map(idsFromWrappedArray),
      dataSetIds = m.get("dataSetId").map(idsFromWrappedArray(_).map(CogniteInternalId(_))),
      externalIdPrefix = m.get("externalIdPrefix"),
      createdTime = timeRange(m, "createdTime"),
      lastUpdatedTime = timeRange(m, "lastUpdatedTime")
    )
}
object TimeSeriesRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[TimeSeriesUpsertSchema]
  val insertSchema: StructType = structType[TimeSeriesInsertSchema]
  val readSchema: StructType = structType[TimeSeriesReadSchema]
}

final case class TimeSeriesUpsertSchema(
    id: Option[Long] = None,
    name: OptionalField[String] = FieldNotSpecified,
    externalId: OptionalField[String] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    unit: OptionalField[String] = FieldNotSpecified,
    assetId: OptionalField[Long] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    securityCategories: Option[Seq[Long]] = None,
    isStep: Option[Boolean] = None,
    isString: Option[Boolean] = None,
    dataSetId: OptionalField[Long] = FieldNotSpecified
) extends WithNullableExtenalId
    with WithId[Option[Long]]

object TimeSeriesUpsertSchema {
  implicit val toCreate: Transformer[TimeSeriesUpsertSchema, TimeSeriesCreate] =
    Transformer
      .define[TimeSeriesUpsertSchema, TimeSeriesCreate]
      .withFieldComputed(_.isStep, _.isStep.getOrElse(false))
      .withFieldComputed(_.isString, _.isString.getOrElse(false))
      .buildTransformer

}

final case class TimeSeriesInsertSchema(
    externalId: Option[String] = None,
    name: Option[String] = None,
    isString: Boolean = false,
    metadata: Option[Map[String, String]] = None,
    unit: Option[String] = None,
    assetId: Option[Long] = None,
    isStep: Boolean = false,
    description: Option[String] = None,
    securityCategories: Option[Seq[Long]] = None,
    dataSetId: Option[Long] = None
)

final case class TimeSeriesReadSchema(
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
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    dataSetId: Option[Long] = None
)
