package cognite.spark.v1

import java.time.Instant
import java.time.temporal.ChronoUnit

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
import fs2._

import scala.util.Random

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

final case class Granularity(amount: Int, unit: ChronoUnit) {
  private val unitString = unit match {
    case ChronoUnit.DAYS => "d"
    case ChronoUnit.WEEKS => "w"
    case ChronoUnit.HOURS => "h"
    case ChronoUnit.MINUTES => "m"
    case ChronoUnit.SECONDS => "s"
    case _ => throw new RuntimeException("Invalid granularity unit")
  }

  override def toString: String = s"$amount$unitString"
}

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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)
    val ids = filtersAsMaps.flatMap(m => m.get("id")).map(_.toLong).distinct
    val externalIds = filtersAsMaps.flatMap(m => m.get("externalId")).distinct
    NumericDataPointsRdd(
      sqlContext.sparkContext,
      config,
      ids,
      externalIds,
      filters,
      toRow(requiredColumns))
  }

  def getIOs(filters: Array[Filter])(
      client: GenericClient[IO, Nothing]): Seq[IO[Seq[DataPointsItem]]] = {
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
      upperTimeLimit: Instant): Seq[IO[Seq[DataPointsItem]]] =
    Seq.empty

  def getIOsWithAggregates(
      ids: Seq[Long],
      externalIds: Seq[String],
      lowerTimeLimit: Instant,
      upperTimeLimit: Instant,
      granularities: Seq[String],
      aggregations: Seq[AggregationFilter],
      client: GenericClient[IO, Nothing]): Seq[IO[Seq[DataPointsItem]]] = {
    val agg = aggregations.map(_.aggregation)
    val ioFromId = for {
      id <- ids
      g <- granularities
    } yield
      client.dataPoints
        .queryAggregatesById(id, lowerTimeLimit, upperTimeLimit, g, agg, config.limit)
        .map(aggs =>
          aggs.flatMap {
            case (aggregation, dataPoints) =>
              dataPoints.map { p =>
                DataPointsItem(
                  Some(id),
                  None,
                  java.sql.Timestamp.from(p.timestamp),
                  p.value,
                  Some(aggregation),
                  Some(g))
              }
          }.toSeq)

    val ioFromExternalId = for {
      extId <- externalIds
      g <- granularities
    } yield
      client.dataPoints
        .queryAggregatesByExternalId(extId, lowerTimeLimit, upperTimeLimit, g, agg, config.limit)
        .map(aggs =>
          aggs.flatMap {
            case (aggregation, dataPoints) =>
              dataPoints.map { p =>
                DataPointsItem(
                  None,
                  Some(extId),
                  java.sql.Timestamp.from(p.timestamp),
                  p.value,
                  Some(aggregation),
                  Some(g))
              }
          }.toSeq)
    ioFromId ++ ioFromExternalId
  }

}
