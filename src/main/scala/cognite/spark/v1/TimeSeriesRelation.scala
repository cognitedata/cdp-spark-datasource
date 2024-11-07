package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.common.{WithId}
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.TimeSeriesResource
import fs2.Stream
import io.scalaland.chimney.Transformer
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1InsertableRelation[TimeSeries, Long](config, TimeSeriesRelation.name)
    with WritableRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
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
    val deletes = rows.map(fromRow[DeleteItemByCogniteId](_))
    deleteWithIgnoreUnknownIds(client.timeSeries, deletes.map(_.toCogniteId), config.ignoreUnknownIds)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val timeSeries = rows.map(fromRow[TimeSeriesUpsertSchema](_))
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

  override def schema: StructType = structType[TimeSeries]()

  override def toRow(t: TimeSeries): Row = asRow(t)

  override def uniqueId(a: TimeSeries): Long = a.id

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, TimeSeries]] = {
    val (ids, filters) =
      pushdownToFilters(sparkFilters, f => timeSeriesFilterFromMap(f.fieldValues), TimeSeriesFilter())
    executeFilter(client.timeSeries, filters, ids, config.partitions, config.limitPerPartition)
  }

  def timeSeriesFilterFromMap(m: Map[String, String]): TimeSeriesFilter =
    TimeSeriesFilter(
      name = m.get("name"),
      unit = m.get("unit"),
      isStep = m.get("isStep").map(_.toBoolean),
      isString = m.get("isString").map(_.toBoolean),
      assetIds = m.get("assetId").map(idsFromStringifiedArray),
      dataSetIds = m.get("dataSetId").map(idsFromStringifiedArray(_).map(CogniteInternalId(_))),
      externalIdPrefix = m.get("externalIdPrefix"),
      createdTime = timeRange(m, "createdTime"),
      lastUpdatedTime = timeRange(m, "lastUpdatedTime"),
      unitExternalId = m.get("unitExternalId")
    )
}
object TimeSeriesRelation
    extends UpsertSchema
    with ReadSchema
    with AbortSchema
    with NamedRelation
    with DeleteWithIdSchema {
  override val name: String = "timeseries"
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  val upsertSchema: StructType = structType[TimeSeriesUpsertSchema]()
  val abortSchema: StructType = structType[TimeSeriesInsertSchema]()
  val readSchema: StructType = structType[TimeSeriesReadSchema]()

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
    dataSetId: OptionalField[Long] = FieldNotSpecified,
    unitExternalId: OptionalField[String] = FieldNotSpecified
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
    dataSetId: Option[Long] = None,
    unitExternalId: Option[String] = None
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
    dataSetId: Option[Long] = None,
    unitExternalId: Option[String] = None
)
