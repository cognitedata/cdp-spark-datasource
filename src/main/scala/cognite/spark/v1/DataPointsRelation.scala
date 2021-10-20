package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.cognite.sdk.scala.v1._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._

abstract class Limit extends Ordered[Limit] with Serializable {
  def value: Instant

  override def compare(that: Limit): Int = this.value.compareTo(that.value)
}

sealed case class Min(value: Instant) extends Limit

sealed case class Max(value: Instant) extends Limit

final case class AggregationFilter(aggregation: String)

import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}

abstract class DataPointsRelationV1[A](config: RelationConfig, shortName: String)(
    override val sqlContext: SQLContext)
    extends CdfRelation(config, shortName)
    with TableScan
    with PrunedFilteredScan
    with Serializable
    with InsertableRelation {
  import CdpConnector._
  import DataPointsRelationV1._

  def toRow(a: A): Row

  def toRow(requiredColumns: Array[String])(item: A): Row

  def delete(rows: Seq[Row]): IO[Unit] = {
    val deleteRanges =
      rows
        .map(fromRow[DeleteDataPointsItem](_))
        .map(processDeleteRow)
    client.dataPoints.deleteRanges(deleteRanges)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      val batches = rows.grouped(Constants.CreateDataPointsLimit).toVector
      batches
        .parTraverse_(insertSeqOfRows)
        .unsafeRunSync()
    })

  def insertSeqOfRows(rows: Seq[Row]): IO[Unit]

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  def getAggregationSettings(filters: Array[Filter]): (Array[AggregationFilter], Seq[String]) = {
    val aggregations = filters.flatMap(getAggregation).distinct
    val granularities = filters.flatMap(getGranularity).distinct

    if (aggregations.nonEmpty && granularities.isEmpty) {
      throw new CdfSparkIllegalArgumentException(
        s"Aggregations requested but granularity is not specified")
    }

    if (aggregations.isEmpty && granularities.nonEmpty) {
      throw new CdfSparkIllegalArgumentException(s"Granularity specified but no aggregation requested")
    }

    (aggregations, granularities)
  }

  // scalastyle:off cyclomatic.complexity
  def toAggregationFilter(aggregation: String): AggregationFilter = {
    val allowedAggregations = Seq(
      "average",
      "max",
      "min",
      "count",
      "sum",
      "interpolation",
      "stepInterpolation",
      "totalVariation",
      "continuousVariance",
      "discreteVariance")
    aggregation match {
      case agg: String if allowedAggregations.contains(agg) => AggregationFilter(agg)
      case _ => throw new CdfSparkIllegalArgumentException(s"Invalid aggregation $aggregation")
    }
  }

  def getAggregation(filter: Filter): Seq[AggregationFilter] =
    filter match {
      case IsNotNull("aggregation") => Seq()
      case EqualTo("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case EqualNullSafe("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case In("aggregation", values) =>
        values.map(v => toAggregationFilter(v.toString))
      case And(_, _) => throw new CdfSparkIllegalArgumentException("AND is not allowed for aggregations")
      case Or(f1, f2) => getAggregation(f1) ++ getAggregation(f2)
      case StringStartsWith("aggregation", value) =>
        throw new CdfSparkIllegalArgumentException(
          s"Choosing aggregation using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("aggregation", value) =>
        throw new CdfSparkIllegalArgumentException(
          s"Choosing aggregation using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("aggregation", value) =>
        throw new CdfSparkIllegalArgumentException(
          s"Choosing aggregation using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
    }

  def toGranularityFilter(granularity: String): Seq[String] =
    granularity.split('|')

  def getGranularity(filter: Filter): Seq[String] =
    filter match {
      case IsNotNull("granularity") => Seq()
      case EqualTo("granularity", value) => toGranularityFilter(value.toString)
      case EqualNullSafe("granularity", value) => toGranularityFilter(value.toString)
      case In("granularity", values) =>
        values.flatMap(v => toGranularityFilter(v.toString))
      case And(_, _) => throw new CdfSparkIllegalArgumentException("AND is not allowed for granularity")
      case Or(f1, f2) => getGranularity(f1) ++ getGranularity(f2)
      case StringStartsWith("granularity", value) =>
        throw new CdfSparkIllegalArgumentException(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for $value")
      case StringEndsWith("granularity", value) =>
        throw new CdfSparkIllegalArgumentException(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for $value")
      case StringContains("granularity", value) =>
        throw new CdfSparkIllegalArgumentException(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for $value")
      case _ => Seq()
    }
}

object DataPointsRelationV1 {
  def limitForCall(nPointsRemaining: Option[Int], batchSize: Int): Int =
    nPointsRemaining match {
      case Some(remaining) => remaining.min(batchSize)
      case None => batchSize
    }

  def getAllDataPoints[R](
      queryMethod: (CogniteId, Instant, Instant, Int) => IO[(Option[Instant], Seq[R])],
      batchSize: Int,
      id: CogniteId,
      lowerLimit: Instant,
      upperLimit: Instant,
      nPointsRemaining: Option[Int] = None,
      allPoints: Seq[R] = Seq.empty): IO[Seq[R]] =
    if (lowerLimit.toEpochMilli >= upperLimit.toEpochMilli || nPointsRemaining.exists(_ <= 0)) {
      IO.pure(allPoints)
    } else {
      val queryPoints =
        queryMethod(id, lowerLimit, upperLimit, limitForCall(nPointsRemaining, batchSize))

      queryPoints
        .flatMap {
          case (_, Nil) => IO.pure(allPoints)
          case (None, points) => IO.pure(allPoints ++ points)
          case (Some(lastTimestamp), points) =>
            val newLowerLimit = lastTimestamp.plusMillis(1)
            val pointsFromResponse = nPointsRemaining match {
              case None => points
              case Some(maxNumPointsToInclude) => points.take(maxNumPointsToInclude)
            }
            val newAllPoints = allPoints ++ pointsFromResponse
            if (points.size == batchSize) {
              // we hit the batch size, let's load the next page
              getAllDataPoints(
                queryMethod,
                batchSize,
                id,
                newLowerLimit,
                upperLimit,
                nPointsRemaining.map(_ - points.size),
                newAllPoints)
            } else {
              IO.pure(newAllPoints)
            }
        }
    }

  // The API only supports exclusiveEnd and inclusiveBegin, so we must adjust the row format for this
  // Since the minimum datapoint precision is 1ms, we can just assume exclusiveEnd = inclusiveEnd + 1ms
  // and inclusiveBegin = exclusiveBegin + 1ms
  private def toLowerbound(
      lower: Option[Instant],
      upper: Option[Instant],
      part: String,
      row: Any): Long =
    (lower, upper) match {
      case (Some(_), Some(_)) =>
        throw new CdfSparkIllegalArgumentException(
          s"Delete row for data points can not contain both inclusive$part and exclusive$part (on row $row)")
      case (Some(lower), None) =>
        lower.toEpochMilli
      case (None, Some(upper)) =>
        upper.toEpochMilli + 1
      case (None, None) =>
        throw new CdfSparkIllegalArgumentException(
          s"Delete row for data points must contain inclusive$part or exclusive$part (on row $row)")
    }

  def processDeleteRow(x: DeleteDataPointsItem): DeleteDataPointsRange = {
    val id = (x.id, x.externalId) match {
      case (Some(id), _) => CogniteInternalId(id)
      case (None, Some(externalId)) => CogniteExternalId(externalId)
      case (None, None) =>
        throw new CdfSparkIllegalArgumentException(
          s"Delete row for data points must contain id or externalId (on row $x)")
    }

    val inclusiveBegin = toLowerbound(x.inclusiveBegin, x.exclusiveBegin, "Begin", x)
    val exclusiveEnd = toLowerbound(x.exclusiveEnd, x.inclusiveEnd, "End", x)
    if (exclusiveEnd <= inclusiveBegin) {
      throw new CdfSparkIllegalArgumentException(
        s"Delete range [$inclusiveBegin, $exclusiveEnd) is invalid (on row $x)")
    }
    DeleteDataPointsRange(id, inclusiveBegin, exclusiveEnd)
  }
}

final case class DeleteDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    exclusiveBegin: Option[Instant],
    inclusiveBegin: Option[Instant],
    exclusiveEnd: Option[Instant],
    inclusiveEnd: Option[Instant]
)
