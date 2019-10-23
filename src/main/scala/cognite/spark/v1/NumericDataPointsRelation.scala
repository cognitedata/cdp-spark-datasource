package cognite.spark.v1

import java.time.Instant

import com.cognite.sdk.scala.common.{DataPoint => SdkDataPoint}
import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.v1.GenericClient
import PushdownUtilities.{pushdownToParameters, toPushdownFilterExpression}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

case class DataPointsFilter(
    id: Option[Long],
    externalId: Option[String],
    aggregates: Option[Seq[String]],
    granularity: Option[String])

case class DataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    isString: Boolean,
    isStep: Boolean,
    unit: Option[String],
    timestamp: java.sql.Timestamp,
    value: Double,
    aggregation: Option[String],
    granularity: Option[String]
)
case class InsertDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: Double)

class NumericDataPointsRelationV1(config: RelationConfig)(sqlContext: SQLContext)
    extends DataPointsRelationV1[DataPointsItem](config)(sqlContext) {

  import PushdownUtilities.filtersToTimestampLimits

  override def schema: StructType =
    StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("isString", BooleanType, nullable = false),
        StructField("isStep", BooleanType, nullable = false),
        StructField("unit", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("aggregation", StringType, nullable = true),
        StructField("granularity", StringType, nullable = true)
      ))

  override def toRow(a: DataPointsItem): Row = asRow(a)

  override def toRow(requiredColumns: Array[String])(item: DataPointsItem): Row = {
    val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
    val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf[String](f))
    val rowOfAllFields = toRow(item)
    Row.fromSeq(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
  }

  def insertSeqOfRows(rows: Seq[Row]): IO[Unit] = {
    val (dataPointsWithId, dataPointsWithExternalId) =
      rows.map(r => fromRow[InsertDataPointsItem](r)).partition(p => p.id.isDefined)
    IO {
      dataPointsWithId.groupBy(_.id).map {
        case (id: Option[Long], p: Seq[InsertDataPointsItem]) =>
          client.dataPoints
            .insertById(id.get, p.map(dp => SdkDataPoint(dp.timestamp, dp.value)))
            .unsafeRunSync()
      }
    } *>
      IO {
        dataPointsWithExternalId.groupBy(_.externalId).map {
          case (extId: Option[String], p: Seq[InsertDataPointsItem]) =>
            client.dataPoints
              .insertByExternalId(extId.get, p.map(dp => SdkDataPoint(dp.timestamp, dp.value)))
              .unsafeRunSync()
        }
      }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    NumericDataPointsRdd(sqlContext.sparkContext, config, getIOs(filters), toRow(requiredColumns))

  def getIOs(filters: Array[Filter])(client: GenericClient[IO, Nothing])
    : Seq[(DataPointsFilter, IO[Map[String, Seq[SdkDataPoint]]])] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val ids = filtersAsMaps.flatMap(m => m.get("id")).map(_.toLong).distinct
    val externalIds = filtersAsMaps.flatMap(m => m.get("externalId")).distinct

    val (lowerTimeLimit, upperTimeLimit) = filtersToTimestampLimits(filters, "timestamp")
    val (aggregations, granularities) = getAggregationSettings(filters)

    if (aggregations.isEmpty) {
      getIOsWithoutAggregates(ids, externalIds, lowerTimeLimit, upperTimeLimit)
    } else {
      getIOsWithAggregates(
        ids,
        externalIds,
        lowerTimeLimit,
        upperTimeLimit,
        granularities,
        aggregations,
        client)
    }
  }

  def getIOsWithoutAggregates(
      ids: Seq[Long],
      externalIds: Seq[String],
      lowerTimeLimit: Instant,
      upperTimeLimit: Instant): Seq[(DataPointsFilter, IO[Map[String, Seq[SdkDataPoint]]])] = {
    val ioFromId = ids.map(
      id =>
        (
          DataPointsFilter(Some(id), None, None, None),
          client.dataPoints
            .queryById(id, lowerTimeLimit, upperTimeLimit, config.limit)
            .map(res => Map("none" -> res)))
    )
    val ioFromExternalId = externalIds.map(
      extId =>
        (
          DataPointsFilter(None, Some(extId), None, None),
          client.dataPoints
            .queryByExternalId(extId, lowerTimeLimit, upperTimeLimit, config.limit)
            .map(res => Map("none" -> res)))
    )
    ioFromId ++ ioFromExternalId
  }

  def getIOsWithAggregates(
      ids: Seq[Long],
      externalIds: Seq[String],
      lowerTimeLimit: Instant,
      upperTimeLimit: Instant,
      granularities: Seq[String],
      aggregations: Seq[AggregationFilter],
      client: GenericClient[IO, Nothing])
    : Seq[(DataPointsFilter, IO[Map[String, Seq[SdkDataPoint]]])] = {
    val agg = aggregations.map(_.aggregation)
    val ioFromId = for {
      id <- ids
      g <- granularities
    } yield {
      (
        DataPointsFilter(Some(id), None, Some(aggregations.map(_.aggregation)), Some(g)),
        client.dataPoints
          .queryAggregatesById(id, lowerTimeLimit, upperTimeLimit, g, agg, config.limit))

    }

    val ioFromExternalId = for {
      extId <- externalIds
      granularity <- granularities
    } yield {
      (
        DataPointsFilter(None, Some(extId), Some(aggregations.map(_.aggregation)), Some(granularity)),
        client.dataPoints
          .queryAggregatesByExternalId(
            extId,
            lowerTimeLimit,
            upperTimeLimit,
            granularity,
            agg,
            config.limit
          ))
    }
    ioFromId ++ ioFromExternalId
  }

}
