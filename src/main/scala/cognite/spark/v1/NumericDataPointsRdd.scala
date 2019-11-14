package cognite.spark.v1

import java.time.{Duration, Instant}
import java.time.temporal.ChronoUnit

import cats.effect.IO
import com.cognite.sdk.scala.v1.GenericClient
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.cognite.sdk.scala.common.{Auth, DataPoint => SdkDataPoint}
import com.softwaremill.sttp.SttpBackend
import cats.implicits._
import fs2.Stream

import Ordering.Implicits._
import scala.util.Random

sealed trait Range {
  val count: Long
}
final case class DataPointsRange(id: Either[Long, String], start: Instant, end: Instant, count: Long)
    extends Range
final case class AggregationRange(
    id: Either[Long, String],
    start: Instant,
    end: Instant,
    count: Long,
    granularity: Granularity,
    aggregation: String)
    extends Range

final case class Bucket(index: Int, ranges: Seq[Range]) extends Partition

case class NumericDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    ids: Seq[Long],
    externalIds: Seq[String],
    timestampLimits: (Instant, Instant),
    aggregations: Array[AggregationFilter],
    granularities: Seq[Granularity],
    toRow: DataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {
  import CdpConnector._
  implicit val auth: Auth = config.auth

  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  private val (lowerTimeLimit, upperTimeLimit) = timestampLimits
  private def countsToRanges(
      id: Either[Long, String],
      counts: Seq[SdkDataPoint],
      granularity: Granularity,
      ranges: Seq[Range] = Seq.empty,
      countSum: Long = 0,
      countStart: Option[Instant] = None): Seq[Range] =
    counts match {
      case count +: moreCounts =>
        if (count.value > maxPointsPerPartition) {
          throw new RuntimeException(
            s"More than ${maxPointsPerPartition} for id $id in interval starting at ${count.timestamp.toString}" +
              " with granularity ${granularity.toString}. Please report this to Cognite.")
        }
        val accumulatedCount = count.value.toLong + countSum
        if (accumulatedCount > partitionSize) {
          val newRange = DataPointsRange(
            id,
            countStart.getOrElse(count.timestamp),
            countStart
              .map(_ => count.timestamp)
              .getOrElse(count.timestamp.plus(granularity.amount, granularity.unit)),
            countSum
          )
          countsToRanges(id, counts, granularity, newRange +: ranges, 0, None)
        } else {
          countsToRanges(
            id,
            moreCounts,
            granularity,
            ranges,
            accumulatedCount,
            countStart.orElse(Some(count.timestamp))
          )
        }
      case _ =>
        countStart match {
          case Some(start) =>
            val lastRange =
              DataPointsRange(id, start, start.plus(granularity.amount, granularity.unit), countSum)
            lastRange +: ranges
          case _ => ranges
        }
    }

  // We must not exceed this. We're assuming there are less than this many
  // points for the smallest interval (1s) which seems reasonable, but we
  // could choose to do paging when that is not the case.
  val maxPointsPerPartition = Constants.DefaultDataPointsLimit
  val partitionSize = 100000
  val bucketSize = 2000000
  val maxPointsPerAggregationRange = 10000

  private val granularitiesToTry = Seq(
    Granularity(Some(300), ChronoUnit.DAYS),
    Granularity(Some(150), ChronoUnit.DAYS),
    Granularity(Some(75), ChronoUnit.DAYS),
    Granularity(Some(37), ChronoUnit.DAYS),
    Granularity(Some(16), ChronoUnit.DAYS),
    Granularity(Some(8), ChronoUnit.DAYS),
    Granularity(Some(1), ChronoUnit.DAYS),
    Granularity(Some(12), ChronoUnit.HOURS),
    Granularity(Some(6), ChronoUnit.HOURS),
    Granularity(Some(3), ChronoUnit.HOURS),
    Granularity(Some(1), ChronoUnit.HOURS),
    Granularity(Some(30), ChronoUnit.MINUTES),
    Granularity(Some(1), ChronoUnit.MINUTES),
    Granularity(Some(30), ChronoUnit.SECONDS),
    Granularity(Some(1), ChronoUnit.SECONDS)
  )

  private def queryAggregates(
      idOrExternalId: Either[Long, String],
      start: Instant,
      end: Instant,
      granularity: String,
      aggregates: Seq[String],
      limit: Int) = idOrExternalId match {
    case Left(id) =>
      client.dataPoints
        .queryAggregatesById(
          id,
          start,
          end,
          granularity.toString,
          aggregates,
          limit = Some(limit)
        )
    case Right(externalId) =>
      client.dataPoints
        .queryAggregatesByExternalId(
          externalId,
          start,
          end,
          granularity.toString,
          aggregates,
          limit = Some(limit)
        )
  }

  private def floorToNearest(x: Long, base: Double) =
    (base * math.floor(x.toDouble / base)).toLong

  private def ceilToNearest(x: Long, base: Double) =
    (base * math.ceil(x.toDouble / base)).toLong

  private def smallEnoughRanges(
      id: Either[Long, String],
      start: Instant,
      end: Instant,
      granularities: Seq[Granularity] = granularitiesToTry): IO[Seq[Range]] =
    granularities match {
      case granularity +: moreGranular =>
        // convert start to closest previous granularity unit
        // convert end to closest next granularity unit(?)
        val granularityUnitMillis = granularity.unit.getDuration.toMillis

        queryAggregates(
          id,
          Instant.ofEpochMilli(floorToNearest(start.toEpochMilli, granularityUnitMillis)),
          Instant.ofEpochMilli(ceilToNearest(end.toEpochMilli, granularityUnitMillis)),
          granularity.toString,
          Seq("count"),
          10000
        ).flatMap { aggregates =>
          val counts = aggregates("count").flatMap(_.datapoints)
          if (counts.map(_.value).max > partitionSize && moreGranular.nonEmpty) {
            smallEnoughRanges(id, start, end, moreGranular)
          } else {
            IO.pure(countsToRanges(id, counts, granularity))
          }
        }
    }

  private def queryById(idOrExternalId: Either[Long, String], start: Instant, end: Instant, limit: Int) =
    idOrExternalId match {
      case Left(id) =>
        client.dataPoints
          .queryById(
            id,
            start,
            end,
            limit = Some(limit)
          )
      case Right(externalId) =>
        client.dataPoints
          .queryByExternalId(
            externalId,
            start,
            end,
            limit = Some(limit)
          )
    }

  private def getFirstAndLastConcurrentlyById(
      idOrExternalIds: Vector[Either[Long, String]],
      start: Instant,
      end: Instant): IO[Vector[(Either[Long, String], Option[Instant], Option[Instant])]] = {
    val firsts = idOrExternalIds.map { id =>
      queryById(id, start, end.max(start.plusMillis(1)), 1)
        .map(queryResponse => queryResponse.flatMap(_.datapoints))
        .map(_.headOption)
        .map(p => id -> p)
    }.parSequence
    val ids = idOrExternalIds.flatMap(_.left.toOption)
    val externalIds = idOrExternalIds.flatMap(_.right.toOption)
    val latestByInternalIds = if (ids.nonEmpty) {
      client.dataPoints.getLatestDataPointsByIds(ids)
    } else {
      IO.pure(Map.empty)
    }
    val latestByExternalIds = if (externalIds.nonEmpty) {
      client.dataPoints.getLatestDataPointsByExternalIds(externalIds)
    } else {
      IO.pure(Map.empty)
    }
    val lastsByInternalId: IO[Map[Either[Long, String], Option[Instant]]] = for {
      latestByIds <- latestByInternalIds
    } yield
      for {
        (id, maybeLatest) <- latestByIds
      } yield (Left[Long, String](id), maybeLatest.map(latest => latest.timestamp.min(end)))
    val lastsByExternalId: IO[Map[Either[Long, String], Option[Instant]]] = for {
      latestByIds <- latestByExternalIds
    } yield
      for {
        (id, maybeLatest) <- latestByIds
      } yield (Right[Long, String](id), maybeLatest.map(latest => latest.timestamp.min(end)))
    val lasts = for {
      byInternalId <- lastsByInternalId
      byExternalId <- lastsByExternalId
    } yield byExternalId ++ byInternalId

    (firsts, lasts).parMapN {
      case (f, l) =>
        f.map {
          case (id, first) =>
            (id, first.map(_.timestamp), l.getOrElse(id, None))
        }
    }
  }

  private def rangesToBuckets(ranges: Seq[Range]): Vector[Bucket] = {
    // Fold into a sequence of buckets, where each bucket has some ranges with a
    // total of data points <= bucketSize
    val bucketsFold = ranges.foldLeft((Seq.empty[Bucket], Seq.empty[Range], 0L)) {
      case ((buckets, ranges, sum), r) =>
        val sumTotal = sum + r.count
        if (sumTotal > bucketSize) {
          // Create a new bucket from the ranges, and assign an index of "1" temporarily.
          // We'll change the index before returning the buckets.
          (Bucket(1, ranges) +: buckets, Seq(r), r.count)
        } else {
          (buckets, r +: ranges, sumTotal)
        }
    }

    // If there are any non-empty ranges left, put them in a final bucket.
    val buckets = bucketsFold match {
      case (buckets, _, 0) => buckets
      case (buckets, ranges, _) => Bucket(1, ranges) +: buckets
    }

    buckets.zipWithIndex.map { case (bucket, index) => bucket.copy(index = index) }.toVector
  }

  private def buckets(
      ids: Seq[Long],
      externalIds: Seq[String],
      firstLatest: Stream[IO, (Either[Long, String], Option[Instant], Option[Instant])])
    : IO[Seq[Bucket]] = {

    val ranges = firstLatest
      .parEvalMapUnordered(50) {
        case (id, Some(first), Some(latest)) =>
          if (latest >= first) {
            smallEnoughRanges(id, first, latest)
          } else {
            IO(Seq(DataPointsRange(id, first, latest.plusMillis(1), 1)))
          }
        case _ => IO(Seq.empty)
      }
      .map(Stream.emits)
      .flatten

    // Shuffle the ranges to get a more even distribution across potentially
    // many time series and ranges, to reduce the possibility of hotspotting.
    ranges.compile.toVector.map(Random.shuffle(_)).map(rangesToBuckets)
  }

  private def aggregationBuckets(
      aggregations: Seq[AggregationFilter],
      granularity: Granularity,
      ids: Seq[Long],
      externalIds: Seq[String],
      firstLatest: Stream[IO, (Either[Long, String], Option[Instant], Option[Instant])]
  ): IO[Vector[Bucket]] = {
    val granularityUnitMillis = granularity.unit.getDuration.toMillis
    // TODO: make sure we have a test that covers more than 10000 units
    firstLatest
      .parEvalMapUnordered(50) {
        case (id, Some(first), Some(latest)) =>
          val aggStart = Instant.ofEpochMilli(floorToNearest(first.toEpochMilli, granularityUnitMillis))
          val aggEnd = Instant.ofEpochMilli(
            floorToNearest(
              latest.toEpochMilli,
              granularity.unit.getDuration.multipliedBy(granularity.amount).toMillis))

          val d1 = Duration.between(aggStart, aggEnd)
          val numValues = d1.toMillis / granularity.unit.getDuration.toMillis
          val numRanges = (numValues / maxPointsPerAggregationRange).max(1)

          val ranges = for {
            a <- aggregations
            i <- 0L until numRanges
            rangeStart = aggStart.plus((maxPointsPerAggregationRange * i).max(0), granularity.unit)
            rangeEnd = rangeStart.plus(maxPointsPerAggregationRange, granularity.unit).min(aggEnd)
            nPoints = Duration
              .between(rangeStart, rangeEnd)
              .toMillis / granularity.unit.getDuration.toMillis
          } yield AggregationRange(id, rangeStart, rangeEnd, nPoints, granularity, a.aggregation)
          IO(ranges)
        case _ => IO(Seq.empty)
      }
      .map(Stream.emits)
      .flatten
      .compile
      .toVector
      .map(Random.shuffle(_))
      .map(rangesToBuckets)
  }

  override def getPartitions: Array[Partition] = {
    val firstLatest = Stream
      .emits(ids.map(Left(_)) ++ externalIds.map(Right(_)))
      .covary[IO]
      .chunkLimit(100)
      .parEvalMapUnordered(50) { chunk =>
        getFirstAndLastConcurrentlyById(chunk.toVector, lowerTimeLimit, upperTimeLimit)
      }
      .flatMap(Stream.emits)
    val partitions = if (granularities.isEmpty) {
      buckets(ids, externalIds, firstLatest)
    } else {
      granularities.toVector
        .map(g => aggregationBuckets(aggregations, g, ids, externalIds, firstLatest))
        .parFlatSequence
    }
    partitions
      .map(_.toArray[Partition])
      .unsafeRunSync()
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val bucket = _split.asInstanceOf[Bucket]

    bucket.ranges.toVector
      .parFlatTraverse {
        case r: DataPointsRange =>
          queryById(r.id, r.start, r.end, 100000)
            .map(queryResponse => queryResponse.flatMap(_.datapoints))
            .map(dataPoints =>
              dataPoints
                .map { p =>
                  DataPointsItem(
                    r.id.left.toOption,
                    r.id.right.toOption,
                    java.sql.Timestamp.from(p.timestamp),
                    p.value,
                    None,
                    None)
                }
                .map(toRow)
                .toVector)
        case r: AggregationRange =>
          queryAggregates(r.id, r.start, r.end, r.granularity.toString, Seq(r.aggregation), 10000)
            .map(queryResponse =>
              queryResponse.mapValues(dataPointsResponse => dataPointsResponse.flatMap(_.datapoints)))
            .map(dataPoints =>
              dataPoints(r.aggregation)
                .map { p =>
                  DataPointsItem(
                    r.id.left.toOption,
                    r.id.right.toOption,
                    java.sql.Timestamp.from(p.timestamp),
                    p.value,
                    Some(r.aggregation),
                    Some(r.granularity.toString))
                }
                .map(toRow)
                .toVector)
      }
      .map(r => config.limitPerPartition.fold(r)(limit => r.take(limit)))
      .unsafeRunSync()
      .toIterator
  }
}
