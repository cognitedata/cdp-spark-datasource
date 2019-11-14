package cognite.spark.v1

import java.time.Instant

import com.cognite.sdk.scala.v1.TimeRange
import com.softwaremill.sttp._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.util.Try

case class DeleteItem(id: Long)

sealed trait PushdownExpression
final case class PushdownFilter(fieldName: String, value: String) extends PushdownExpression
final case class PushdownAnd(left: PushdownExpression, right: PushdownExpression)
    extends PushdownExpression
final case class PushdownFilters(filters: Seq[PushdownExpression]) extends PushdownExpression
final case class NoPushdown() extends PushdownExpression

object PushdownUtilities {
  def pushdownToUri(parameters: Seq[Map[String, String]], uri: Uri): Seq[Uri] =
    parameters.map(params => uri.params(params))

  def pushdownToParameters(p: PushdownExpression): Seq[Map[String, String]] =
    p match {
      case PushdownAnd(left, right) =>
        handleAnd(pushdownToParameters(left), pushdownToParameters(right))
      case PushdownFilter(field, value) => Seq(Map[String, String](field -> value))
      case PushdownFilters(filters) => filters.flatMap(pushdownToParameters)
      case NoPushdown() => Seq()
    }

  def handleAnd(
      left: Seq[Map[String, String]],
      right: Seq[Map[String, String]]): Seq[Map[String, String]] =
    if (left.isEmpty) {
      right
    } else if (right.isEmpty) {
      left
    } else {
      for {
        l <- left
        r <- right
      } yield l ++ r
    }

  def toPushdownFilterExpression(filters: Array[Filter]): PushdownExpression =
    if (filters.isEmpty) {
      NoPushdown()
    } else {
      filters
        .map(getFilter)
        .reduce(PushdownAnd)
    }

  // Spark will still filter the result after pushdown filters are applied, see source code for
  // PrunedFilteredScan, hence it's ok that our pushdown filter reads some data that should ideally
  // be filtered out
  // scalastyle:off
  def getFilter(filter: Filter): PushdownExpression =
    filter match {
      case IsNotNull(colName) => NoPushdown()
      case EqualTo(colName, value) => PushdownFilter(colName, value.toString)
      case EqualNullSafe(colName, value) => PushdownFilter(colName, value.toString)
      case GreaterThan(colName, value) =>
        PushdownFilter("min" + colName.capitalize, value.toString)
      case GreaterThanOrEqual(colName, value) =>
        PushdownFilter("min" + colName.capitalize, value.toString)
      case LessThan(colName, value) =>
        PushdownFilter("max" + colName.capitalize, value.toString)
      case LessThanOrEqual(colName, value) =>
        PushdownFilter("max" + colName.capitalize, value.toString)
      case In(colName, values) =>
        PushdownFilters(values.map(v => PushdownFilter(colName, v.toString)))
      case And(f1, f2) => PushdownAnd(getFilter(f1), getFilter(f2))
      case Or(f1, f2) => PushdownFilters(Seq(getFilter(f1), getFilter(f2)))
      case _ => NoPushdown()
    }

  def shouldGetAll(
      pushdownExpression: PushdownExpression,
      fieldsWithPushdownFilter: Seq[String]): Boolean =
    pushdownExpression match {
      case PushdownAnd(left, right) =>
        shouldGetAll(left, fieldsWithPushdownFilter) && shouldGetAll(right, fieldsWithPushdownFilter)
      case PushdownFilter(field, _) => !fieldsWithPushdownFilter.contains(field)
      case PushdownFilters(filters) =>
        filters
          .map(shouldGetAll(_, fieldsWithPushdownFilter))
          .exists(identity)
      case NoPushdown() => false
    }

  def assetIdsFromWrappedArray(wrappedArray: String): Seq[Long] =
    wrappedArray.split("\\D+").filter(_.nonEmpty).map(_.toLong)

  def filtersToTimestampLimits(filters: Array[Filter], colName: String): (Instant, Instant) = {
    val timestampLimits = filters.flatMap(getTimestampLimit(_, colName))

    if (timestampLimits.exists(_.value.isBefore(Instant.ofEpochMilli(0)))) {
      sys.error("timestamp limits must exceed 1970-01-01T00:00:00Z")
    }

    Tuple2(
      // Note that this way of aggregating filters will not work with "Or" predicates.
      Try(timestampLimits.filter(_.isInstanceOf[Min]).max).toOption
        .map(_.value)
        .getOrElse(Instant.ofEpochMilli(0)),
      Try(timestampLimits.filter(_.isInstanceOf[Max]).min).toOption
        .map(_.value)
        .getOrElse(Instant.ofEpochMilli(Constants.millisSinceEpochIn2100)) // Year 2100 should be sufficient
    )
  }

  def timeRangeFromMinAndMax(minTime: Option[String], maxTime: Option[String]): Option[TimeRange] =
    (minTime, maxTime) match {
      case (None, None) => None
      case _ => {
        val minimumTimeAsInstant =
          minTime
            .map(java.sql.Timestamp.valueOf(_).toInstant)
            .getOrElse(java.time.Instant.ofEpochMilli(0)) //API does not accept values < 0
        val maximumTimeAsInstant =
          maxTime
            .map(java.sql.Timestamp.valueOf(_).toInstant)
            .getOrElse(java.time.Instant.ofEpochMilli(Long.MaxValue))
        Some(TimeRange(minimumTimeAsInstant, maximumTimeAsInstant))
      }
    }

  def getTimestampLimit(filter: Filter, colName: String): Seq[Limit] =
    filter match {
      case LessThan(colName, value) => Seq(timeStampStringToMax(value, -1))
      case LessThanOrEqual(colName, value) => Seq(timeStampStringToMax(value, 0))
      case GreaterThan(colName, value) => Seq(timeStampStringToMin(value, 1))
      case GreaterThanOrEqual(colName, value) => Seq(timeStampStringToMin(value, 0))
      case And(f1, f2) => getTimestampLimit(f1, colName) ++ getTimestampLimit(f2, colName)
      // case Or(f1, f2) => we might possibly want to do something clever with joining an "or" clause
      //                    with timestamp limits on each side (including replacing "max of Min's" with the less strict
      //                    "min of Min's" when aggregating filters on the same side); just ignore them for now
      case _ => Seq.empty
    }

  def timeStampStringToMin(value: Any, adjustment: Long): Min =
    Min(java.sql.Timestamp.valueOf(value.toString).toInstant.plusMillis(adjustment))

  def timeStampStringToMax(value: Any, adjustment: Long): Max =
    Max(java.sql.Timestamp.valueOf(value.toString).toInstant.plusMillis(adjustment))
}

trait InsertSchema {
  val insertSchema: StructType
}

trait UpsertSchema {
  val upsertSchema: StructType
}

trait UpdateSchema {
  val updateSchema: StructType
}

abstract class DeleteSchema {
  val deleteSchema: StructType = StructType(Seq(StructField("id", DataTypes.LongType)))
}
