package cognite.spark.v1

import cats.effect.Concurrent
import com.cognite.sdk.scala.common.{
  PartitionedFilter,
  RetrieveByExternalIdsWithIgnoreUnknownIds,
  RetrieveByIdsWithIgnoreUnknownIds,
  WithGetExternalId,
  WithId
}

import java.time.Instant
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteId, CogniteInternalId, ContainsAny, TimeRange}
import fs2.Stream
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import sttp.model.Uri

import scala.util.Try

final case class DeleteItem(id: Long)

final case class DeleteItemByCogniteId(
    id: Option[Long],
    externalId: Option[String]
) {
  def toCogniteId: CogniteId = (id, externalId) match {
    case (Some(id), _) =>
      // internalId takes place prior to externalId because delete return conflict error
      // if input contains id and externalId that represent the same item
      CogniteInternalId(id)
    case (None, Some(externalId)) => CogniteExternalId(externalId)
    case (None, None) =>
      throw new CdfSparkIllegalArgumentException(
        "Unexpected error, at least id or externalId must be provided")
  }
}

sealed trait PushdownExpression
final case class PushdownFilter(fieldName: String, value: String) extends PushdownExpression
final case class PushdownAnd(left: PushdownExpression, right: PushdownExpression)
    extends PushdownExpression
final case class PushdownFilters(filters: Seq[PushdownExpression]) extends PushdownExpression
final case class NoPushdown() extends PushdownExpression

object PushdownUtilities {
  def pushdownToUri(parameters: Seq[Map[String, String]], uri: Uri): Seq[Uri] =
    parameters.map(params => uri.params(params))

  def pushdownToFilters[F](
      sparkFilters: Array[Filter],
      mapping: Map[String, String] => F,
      allFilter: F): (Vector[CogniteId], Vector[F]) = {
    val pushdownFilterExpression = toPushdownFilterExpression(sparkFilters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression).toVector
    val (idFilterMaps, filterMaps) =
      filtersAsMaps.partition(m => m.contains("id") || m.contains("externalId"))
    val ids = idFilterMaps.map(
      m =>
        m.get("id")
          .map(id => CogniteInternalId(id.toLong))
          .getOrElse(CogniteExternalId(m("externalId"))))
    val filters = filterMaps.map(mapping)
    val shouldGetAll = filters.contains(allFilter) || (filters.isEmpty && ids.isEmpty)
    if (shouldGetAll) {
      (Vector.empty, Vector(allFilter))
    } else {
      (ids.distinct, filters.distinct)
    }
  }

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
      case IsNotNull(_) | IsNull(_) | EqualNullSafe(_, null) => NoPushdown()
      case EqualTo(_, null) | GreaterThan(_, null) | GreaterThanOrEqual(_, null) | LessThan(_, null) |
          LessThanOrEqual(_, null) =>
        throw new CdfInternalSparkException(
          "Unexpected error, seems that Spark query optimizer is misbehaving. Please contact support@cognite.com and tell them.")
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
      case StringStartsWith(colName, value) =>
        PushdownFilter(colName + "Prefix", value)
      case In(colName, values) =>
        PushdownFilters(
          values
          // X in (null, Y) will result in `NULL`, which is treated like false.
          // X AND NULL is NULL (like with false)
          // true OR NULL is true (like with false)
          // false OR NULL is NULL. Almost like with false, since null is like false
          // This is not true for negation, but we can't process negation in pushdown filters anyway
            .filter(_ != null)
            .map(v => PushdownFilter(colName, v.toString)))
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

  def externalIdsSeqFromWrappedArray(wrappedArray: String): Seq[String] =
    // We get "WrappedArray(ext1, ext2)" here, so we can remove irrelevant parts of the string
    // and extract ext1 and ext2 from it -> return Seq('ext1', 'ext2')
    wrappedArray.length match {
      case 14 => Seq() // "WrappedArray()".length
      case _ => wrappedArray.slice(13, wrappedArray.length - 1).split(',')
    }

  def idsFromWrappedArray(wrappedArray: String): Seq[Long] =
    wrappedArray.split("\\D+").filter(_.nonEmpty).map(_.toLong)

  def filtersToTimestampLimits(filters: Array[Filter], colName: String): (Instant, Instant) = {
    val timestampLimits = filters.flatMap(getTimestampLimit(_, colName))

    if (timestampLimits.exists(_.value.isBefore(Instant.ofEpochMilli(0)))) {
      throw new CdfSparkIllegalArgumentException("timestamp limits must exceed 1970-01-01T00:00:00Z")
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

  private def parseTimestamp(v: String) =
    java.sql.Timestamp.valueOf(v).toInstant

  def timeRangeFromMinAndMax(minTime: Option[String], maxTime: Option[String]): Option[TimeRange] =
    (minTime, maxTime) match {
      case (None, None) => None
      case _ =>
        val minimumTimeAsInstant =
          minTime
            .map(parseTimestamp)
            .getOrElse(java.time.Instant.ofEpochMilli(0)) // API does not accept values < 0
        val maximumTimeAsInstant =
          maxTime
            .map(parseTimestamp)
            .getOrElse(java.time.Instant.ofEpochMilli(32503680000000L)) // 01.01.3000 (hardcoded for relationships instead of Long.MaxValue)
        Some(TimeRange(Some(minimumTimeAsInstant), Some(maximumTimeAsInstant)))
    }

  def timeRange(m: Map[String, String], fieldName: String): Option[TimeRange] =
    m.get(fieldName)
      .map(parseTimestamp)
      .map(exactValue => TimeRange(Some(exactValue), Some(exactValue)))
      .orElse(
        timeRangeFromMinAndMax(m.get("min" + fieldName.capitalize), m.get("max" + fieldName.capitalize)))

  def getTimestampLimit(filter: Filter, colName: String): Seq[Limit] =
    filter match {
      case LessThan(`colName`, value) => Seq(timeStampStringToMax(value, -1))
      case LessThanOrEqual(`colName`, value) => Seq(timeStampStringToMax(value, 0))
      case GreaterThan(`colName`, value) => Seq(timeStampStringToMin(value, 1))
      case GreaterThanOrEqual(`colName`, value) => Seq(timeStampStringToMin(value, 0))
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

  def cogniteExternalIdSeqToStringSeq(
      cogniteExternalIds: Option[Seq[CogniteExternalId]]): Option[Seq[String]] =
    cogniteExternalIds match {
      case Some(Seq()) => None
      case _ => cogniteExternalIds.map(l => l.map(_.externalId))
    }

  def stringSeqToCogniteExternalIdSeq(
      strExternalIds: Option[Seq[String]]): Option[Seq[CogniteExternalId]] =
    strExternalIds match {
      case Some(Seq()) => None
      case _ => strExternalIds.map(l => l.filter(_ != null).map(CogniteExternalId(_)))
    }

  def externalIdsToContainsAny(externalIds: String): Option[ContainsAny] = {
    val externalIdSeq = externalIdsSeqFromWrappedArray(externalIds)
    externalIdSeq.isEmpty match {
      case true => None
      case _ => Some(ContainsAny(containsAny = externalIdSeq.map(CogniteExternalId(_))))
    }
  }

  def getIdFromMap(m: Map[String, String]): Option[CogniteId] =
    m.get("id")
      .map(id => CogniteInternalId(id.toLong))
      .orElse(m.get("externalId").map(CogniteExternalId(_)))

  def mergeStreams[T, F[_]: Concurrent](streams: Seq[Stream[F, T]]): Stream[F, T] =
    streams.reduceOption(_.merge(_)).getOrElse(Stream.empty)

  def getFromIds[T, F[_]: Concurrent](
      ids: Vector[CogniteId],
      clientGetByIds: Vector[Long] => F[Seq[T]],
      clientGetByExternalIds: Vector[String] => F[Seq[T]]): Stream[F, T] = {
    val internalIds = ids.collect { case CogniteInternalId(id) => id }
    val externalIds = ids.collect { case CogniteExternalId(externalId) => externalId }
    // grouped does not produce a group with 0 elements
    val requests =
      internalIds
        .grouped(Constants.DefaultBatchSize)
        .map(clientGetByIds) ++
        externalIds
          .grouped(Constants.DefaultBatchSize)
          .map(clientGetByExternalIds)

    mergeStreams(requests.map(fs2.Stream.evalSeq).toVector)
  }

  def getFromIds[T, F[_]: Concurrent](
      ids: Vector[CogniteId],
      resource: RetrieveByIdsWithIgnoreUnknownIds[T, F] with RetrieveByExternalIdsWithIgnoreUnknownIds[
        T,
        F]
  ): Stream[F, T] =
    getFromIds(
      ids,
      resource.retrieveByIds(_, ignoreUnknownIds = true),
      resource.retrieveByExternalIds(_, ignoreUnknownIds = true)
    )

  def checkDuplicateOnIdsOrExternalIds(
      id: String,
      externalId: Option[String],
      ids: Seq[String],
      externalIds: Seq[String]): Boolean =
    !ids.contains(id) && (externalId.isEmpty || !externalIds.contains(externalId.getOrElse("")))

  def checkDuplicateCogniteIds[R <: WithGetExternalId with WithId[Long]](
      ids: Vector[CogniteId]): R => Boolean = {
    val idSet = ids.collect { case CogniteInternalId(id) => id }.toSet
    val externalIdSet = ids.collect { case CogniteExternalId(externalId) => externalId }.toSet
    r =>
      !idSet.contains(r.id) && !r.getExternalId.exists(externalIdSet.contains)
  }

  def executeFilterOnePartition[R, Fi, F[_]: Concurrent](
      resource: com.cognite.sdk.scala.common.Filter[R, Fi, F] with RetrieveByIdsWithIgnoreUnknownIds[
        R,
        F] with RetrieveByExternalIdsWithIgnoreUnknownIds[R, F],
      filters: Vector[Fi],
      ids: Vector[CogniteId],
      limit: Option[Int]
  ): Stream[F, R] = {
    val streamsPerFilter: Vector[Stream[F, R]] =
      filters.map(f => resource.filter(f, limit))

    mergeStreams(streamsPerFilter).merge(getFromIds(ids, resource))
  }
  def executeFilter[R <: WithGetExternalId with WithId[Long], Fi, F[_]: Concurrent](
      resource: PartitionedFilter[R, Fi, F] with RetrieveByIdsWithIgnoreUnknownIds[R, F] with RetrieveByExternalIdsWithIgnoreUnknownIds[
        R,
        F],
      filters: Vector[Fi],
      ids: Vector[CogniteId],
      numPartitions: Int,
      limit: Option[Int]
  ): Vector[Stream[F, R]] = {
    if (numPartitions == 1) {
      return Vector(executeFilterOnePartition(resource, filters, ids, limit))
    }

    val streamsPerFilter: Vector[Seq[Stream[F, R]]] =
      filters.map(f => resource.filterPartitions(f, numPartitions, limit))
    var partitionStreams =
      streamsPerFilter.transpose
        .map(mergeStreams(_))
        .map(_.filter(checkDuplicateCogniteIds(ids)))
    if (ids.nonEmpty) {
      // only add the ids partition when it's not empty, Spark partitions are not super cheap
      partitionStreams ++= Vector(getFromIds(ids, resource))
    }

    partitionStreams
  }
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
