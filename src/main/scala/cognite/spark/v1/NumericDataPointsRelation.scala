package cognite.spark.v1

import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.{
  getIdFromMap,
  pushdownToParameters,
  toPushdownFilterExpression
}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.{DataPoint => SdkDataPoint}
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteId, CogniteInternalId}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.matching.Regex

final case class DataPointsFilter(
    id: Option[Long],
    externalId: Option[String],
    aggregates: Option[Seq[String]],
    granularity: Option[String])

// Note that this *must* be kept in sync with NumericDataPointsRdd.dataPointToRow
// and NumericDataPointsRdd.aggregationDataPointToRow.
final case class DataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: Double,
    aggregation: Option[String],
    granularity: Option[String]
)

abstract class RowWithCogniteId(contextMsg: String) {
  val id: Option[Long]
  val externalId: Option[String]
  def getCogniteId: CogniteId =
    id.filter(_ > 0)
      .map(CogniteInternalId(_))
      .orElse[CogniteId](externalId.map(CogniteExternalId(_)))
      .getOrElse(throw new CdfSparkIllegalArgumentException(
        s"The id or externalId fields must be set when $contextMsg."))
}

final case class InsertDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: Double)
    extends RowWithCogniteId("inserting data points")

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
    case _ => throw new CdfSparkException("Invalid granularity unit.")
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

  @SuppressWarnings(Array("scalafix:DisableSyntax.noValPatterns"))
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
          Left(new CdfSparkIllegalArgumentException(s"Invalid granularity specification: $s."))
      }
}

class NumericDataPointsRelationV1(config: RelationConfig)(sqlContext: SQLContext)
    extends DataPointsRelationV1[DataPointsItem](config, "datapoints")(sqlContext)
    with WritableRelation {
  import PushdownUtilities.filtersToTimestampLimits
  override def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert not supported for datapoints. Use upsert instead.")

  override def upsert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Use insert(DataFrame) instead.")

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update not supported for datapoints. Use upsert instead.")

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
    val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf(f))
    val rowOfAllFields = toRow(item)
    new GenericRow(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
  }

  def insertRowIterator(rows: Iterator[Row]): IO[Unit] = {
    // we basically use Stream.fromIterator instead of Seq.grouped, because it's significantly more efficient
    val dataPoints = Stream.fromIterator[IO](
      rows.map(r => fromRow[InsertDataPointsItem](r)),
      chunkSize = Constants.CreateDataPointsLimit
    )

    dataPoints.chunks
      .flatMap(c => Stream.emits(c.toVector.groupBy(_.getCogniteId).toVector))
      .parEvalMapUnordered(config.parallelismPerPartition) {
        case (id, dataPoints) =>
          client.dataPoints
            .insert(id, dataPoints.map(dp => SdkDataPoint(dp.timestamp, dp.value)))
            .flatTap(_ => incMetrics(itemsCreated, dataPoints.size))
      }
      .compile
      .drain
  }

  private val namesToFields: Map[String, Int] = Map(
    "id" -> 0,
    "externalId" -> 1,
    "timestamp" -> 2,
    "value" -> 3,
    "aggregation" -> 4,
    "granularity" -> 5
  )

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val timestampLimits = filtersToTimestampLimits(filters, "timestamp")
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)
    val ids = filtersAsMaps.flatMap(getIdFromMap).distinct

    // Notify users that they need to supply one or more ids/externalIds when reading data points
    if (ids.isEmpty) {
      throw new CdfSparkIllegalArgumentException(
        "Please filter by one or more ids or externalIds when reading data points." +
          " Note that specifying id or externalId through joins is not possible at the moment."
      )
    }
    val (aggregations, stringGranularities) = getAggregationSettings(filters)

    val granularitiesOrErrors = stringGranularities
      .map(Granularity.parse)
      .toVector
      .traverse(_.toValidatedNel)
    val granularities = granularitiesOrErrors match {
      case Valid(granularities) => granularities
      case Invalid(errors) =>
        val errorMessages = errors.map(_.getMessage).mkString_("\n")
        throw new CdfSparkIllegalArgumentException(errorMessages)
    }
    NumericDataPointsRdd(
      sqlContext.sparkContext,
      config,
      ids,
      timestampLimits,
      aggregations,
      granularities,
      (i: Int) => {
        if (config.collectMetrics) {
          itemsRead.inc(i)
        }
      },
      requiredColumns.map(namesToFields)
    )
  }
}

object NumericDataPointsRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[InsertDataPointsItem]()
  val readSchema: StructType = structType[DataPointsItem]()
  val insertSchema: StructType = structType[InsertDataPointsItem]()
  val deleteSchema: StructType = structType[DeleteDataPointsItem]()
}
