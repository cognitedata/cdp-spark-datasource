package cognite.spark.v1

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.cognite.sdk.scala.common.{DataPoint => SdkDataPoint}
import cats.effect.IO
import cats.implicits._
import PushdownUtilities.{pushdownToParameters, toPushdownFilterExpression}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow}
import cats.data.Validated.{Invalid, Valid}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import cognite.spark.v1.SparkSchemaHelper.structType

import scala.util.matching.Regex

case class DataPointsFilter(
    id: Option[Long],
    externalId: Option[String],
    aggregates: Option[Seq[String]],
    granularity: Option[String])

case class DataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: Double,
    aggregation: Option[String],
    granularity: Option[String]
)

case class InsertDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: Double)

final case class Granularity(
    amountOption: Option[Long],
    unit: ChronoUnit,
    isLongFormat: Boolean = false) {
  private val unitString = unit match {
    case ChronoUnit.DAYS => if (isLongFormat) "day" else "d"
    case ChronoUnit.WEEKS => if (isLongFormat) "week" else "w"
    case ChronoUnit.HOURS => if (isLongFormat) "hour" else "h"
    case ChronoUnit.MINUTES => if (isLongFormat) "minute" else "m"
    case ChronoUnit.SECONDS => if (isLongFormat) "second" else "s"
    case _ => throw new RuntimeException("Invalid granularity unit")
  }

  val amount: Long = amountOption.getOrElse(1)

  private val amountString = amountOption match {
    case Some(a) => a.toString
    case None => ""
  }

  override def toString: String = s"$amountString$unitString"

  def toMillis: Long = unit.getDuration.multipliedBy(amount).toMillis
}

object Granularity {
  val shortStringToUnit: Map[String, ChronoUnit] = Map(
    "d" -> ChronoUnit.DAYS,
    "w" -> ChronoUnit.WEEKS,
    "h" -> ChronoUnit.HOURS,
    "m" -> ChronoUnit.MINUTES,
    "s" -> ChronoUnit.SECONDS
  )
  val longStringToUnit: Map[String, ChronoUnit] = Map(
    "day" -> ChronoUnit.DAYS,
    "week" -> ChronoUnit.WEEKS,
    "hour" -> ChronoUnit.HOURS,
    "minute" -> ChronoUnit.MINUTES,
    "second" -> ChronoUnit.SECONDS
  )

  private val validUnits = shortStringToUnit.keys.mkString("|") + "|" +
    longStringToUnit.keys.mkString("|")
  val granularityRegex: Regex = f"""([1-9][0-9]*)*($validUnits)""".r

  def parse(s: String): Either[Throwable, Granularity] =
    Either
      .catchNonFatal {
        val granularityRegex(amount, unitString) = s
        val (unit, isLongFormat) = shortStringToUnit.get(unitString) match {
          case Some(unit) => (unit, false)
          case None => (longStringToUnit(unitString), true)
        }
        Granularity(Option(amount).map(_.toInt), unit, isLongFormat)
      }
      .recoverWith {
        case _: RuntimeException =>
          Left(new IllegalArgumentException(s"Invalid granularity specification: $s."))
      }
}

class NumericDataPointsRelationV1(config: RelationConfig)(sqlContext: SQLContext)
    extends DataPointsRelationV1[DataPointsItem](config, "datapoints")(sqlContext)
    with WritableRelation {
  import CdpConnector._

  import PushdownUtilities.filtersToTimestampLimits

  override def insert(rows: Seq[Row]): IO[Unit] =
    throw new RuntimeException("Insert not supported for datapoints. Use upsert instead.")

  override def upsert(rows: Seq[Row]): IO[Unit] = insertSeqOfRows(rows)

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new RuntimeException("Update not supported for datapoints. Use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] =
    throw new RuntimeException("Delete not supported for datapoints.")

  override def schema: StructType =
    StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
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
      rows.map(r => fromRow[InsertDataPointsItem](r)).partition(p => p.id.exists(_ > 0))

    if (dataPointsWithExternalId.exists(_.externalId.isEmpty)) {
      throw new IllegalArgumentException(
        "The id or externalId fields must be set when inserting data points.")
    }

    val updatesById = dataPointsWithId.groupBy(_.id).map {
      case (id, dataPoints) =>
        client.dataPoints
          .insertById(id.get, dataPoints.map(dp => SdkDataPoint(dp.timestamp, dp.value)))
          .flatTap(_ => incMetrics(itemsCreated, dataPoints.size))
    }

    val updatesByExternalId = dataPointsWithExternalId.groupBy(_.externalId).map {
      case (Some(externalId), dataPoints) =>
        client.dataPoints
          .insertByExternalId(externalId, dataPoints.map(dp => SdkDataPoint(dp.timestamp, dp.value)))
          .flatTap(_ => incMetrics(itemsCreated, dataPoints.size))
      case (None, _) =>
        throw new IllegalArgumentException(
          "The id or externalId fields must be set when inserting data points.")
    }

    (updatesById.toVector.parSequence_, updatesByExternalId.toVector.parSequence_)
      .parMapN((_, _) => ())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val timestampLimits = filtersToTimestampLimits(filters, "timestamp")
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)
    val ids = filtersAsMaps.flatMap(m => m.get("id")).map(_.toLong).distinct
    val externalIds = filtersAsMaps.flatMap(m => m.get("externalId")).distinct
    val (aggregations, stringGranularities) = getAggregationSettings(filters)

    val granularitiesOrErrors = stringGranularities
      .map(Granularity.parse)
      .toVector
      .traverse(_.toValidatedNel)
    val granularities = granularitiesOrErrors match {
      case Valid(granularities) => granularities
      case Invalid(errors) =>
        val errorMessages = errors.map(_.getMessage).mkString_("\n")
        throw new IllegalArgumentException(errorMessages)
    }
    NumericDataPointsRdd(
      sqlContext.sparkContext,
      config,
      ids,
      externalIds,
      timestampLimits,
      aggregations,
      granularities,
      (item: DataPointsItem) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(requiredColumns)(item)
      }
    )
  }
}

object NumericDataPointsRelation extends UpsertSchema {
  val upsertSchema = structType[InsertDataPointsItem]
  val readSchema = structType[DataPointsItem]
  val insertSchema = structType[InsertDataPointsItem]
}
