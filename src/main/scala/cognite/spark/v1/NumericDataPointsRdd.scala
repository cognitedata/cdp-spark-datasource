package cognite.spark.v1

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.effect.{ContextShift, IO}
import com.cognite.sdk.scala.v1.GenericClient
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.cognite.sdk.scala.common.{Auth, DataPoint => SdkDataPoint}
import com.softwaremill.sttp.SttpBackend
import cats.syntax._
import cats.implicits._
import cognite.spark.PushdownUtilities.{pushdownToParameters, toPushdownFilterExpression}
import fs2._
import org.apache.spark.sql.sources._
import Ordering.Implicits._

import scala.concurrent.ExecutionContext
import scala.util.{Random, Try}

final case class Range(id: Long, start: Instant, end: Instant, count: Long)

final case class Bucket(index: Int, ranges: Seq[Range]) extends Partition

case class NumericDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    ids: Seq[Long],
    externalIds: Seq[String],
    filters: Array[Filter],
    timestampLimits: (Option[Instant], Option[Instant]),
    toRow: DataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {

  implicit val auth: Auth = config.auth
  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  private val (lowerTimeLimit, upperTimeLimit) = timestampLimits
  private def countsToRanges(
      id: Long,
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
          val newRange = Range(
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
            val lastRange = Range(id, start, start.plus(granularity.amount, granularity.unit), countSum)
            lastRange +: ranges
          case _ => ranges
        }
    }

  // We must not exceed this. We're assuming there are less than this many
  // points for the smallest interval (1s) which seems reasonable, but we
  // could choose to do paging when that is not the case.
  val maxPointsPerPartition = 100000
  val partitionSize = 100000
  val bucketSize = 2000000

  private val granularitiesToTry = Seq(
    Granularity(300, ChronoUnit.DAYS),
    Granularity(150, ChronoUnit.DAYS),
    Granularity(75, ChronoUnit.DAYS),
    Granularity(37, ChronoUnit.DAYS),
    Granularity(16, ChronoUnit.DAYS),
    Granularity(8, ChronoUnit.DAYS),
    Granularity(1, ChronoUnit.DAYS),
    Granularity(12, ChronoUnit.HOURS),
    Granularity(6, ChronoUnit.HOURS),
    Granularity(3, ChronoUnit.HOURS),
    Granularity(1, ChronoUnit.HOURS),
    Granularity(30, ChronoUnit.MINUTES),
    Granularity(1, ChronoUnit.MINUTES),
    Granularity(30, ChronoUnit.SECONDS),
    Granularity(1, ChronoUnit.SECONDS)
  )

  private def smallEnoughRanges(
      id: Long,
      start: Instant,
      end: Instant,
      granularities: Seq[Granularity] = granularitiesToTry): IO[Seq[Range]] =
    granularities match {
      case granularity +: moreGranular =>
        client.dataPoints
          .queryAggregatesById(
            id,
            start,
            end,
            granularity.toString,
            Seq("count"),
            limit = Some(10000)
          )
          .flatMap { aggregates =>
            val counts = aggregates("count")
            if (counts.map(_.value).max > partitionSize && moreGranular.nonEmpty) {
              smallEnoughRanges(id, start, end, moreGranular)
            } else {
              IO.pure(countsToRanges(id, counts, granularity))
            }
          }
    }

  private def getFirstAndLastConcurrently(ids: Vector[Long], start: Instant, end: Instant) = {
    val firsts = ids.map { id =>
      client.dataPoints
        .queryById(
          id,
          start,
          end,
          limit = Some(1)
        )
        .map(_.headOption)
        .map(p => id -> p)
    }.parSequence
    val lasts = for {
      latestByIds <- client.dataPoints.getLatestDataPointsByIds(ids)
    } yield
      for {
        (id, maybeLatest) <- latestByIds
      } yield (id, maybeLatest.map(latest => latest.timestamp.min(end)))
    (firsts, lasts).parMapN {
      case (f, l) =>
        f.map {
          case (id, first) =>
            (id, first.map(_.timestamp), l.getOrElse(id, None))
        }
    }
  }

  private def rangesToBuckets(ranges: Seq[Range]) = {
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

    buckets.zipWithIndex.map {
      case (bucket, index) => bucket.copy(index = index)
    }
  }

  private def buckets(
      ids: Seq[Long],
      externalIds: Seq[String],
      maybeStart: Option[Instant],
      maybeEnd: Option[Instant]): Seq[Bucket] = {
    val start = maybeStart.getOrElse(Instant.ofEpochMilli(0))
    val end = maybeEnd.getOrElse(Instant.ofEpochMilli(Long.MaxValue))
    val firstLatest = Stream
      .emits(ids)
      .covary[IO]
      .chunkLimit(100)
      .parEvalMapUnordered(50) { chunk =>
        getFirstAndLastConcurrently(chunk.toVector, start, end)
      }
      .flatMap(Stream.emits)

    val ranges = firstLatest
      .parEvalMapUnordered(50) {
        case (id, Some(first), Some(latest)) =>
          if (latest >= first) {
            smallEnoughRanges(id, first, latest)
          } else {
            IO(Seq(Range(id, first, latest.plusMillis(1), 1)))
          }
        case _ => IO(Seq.empty)
      }
      .map(Stream.emits)
      .flatten

    // Shuffle the ranges to get a more even distribution across potentially
    // many time series and ranges, to reduce the possibility of hotspotting.
    val shuffledRanges = ranges.compile.toVector.map(Random.shuffle(_)).unsafeRunSync()

    rangesToBuckets(shuffledRanges)
  }

  override def getPartitions: Array[Partition] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val ids = filtersAsMaps.flatMap(m => m.get("id")).map(_.toLong).distinct
    val externalIds = filtersAsMaps.flatMap(m => m.get("externalId")).distinct

    buckets(ids, externalIds, lowerTimeLimit, upperTimeLimit).toArray
  }

  @transient lazy implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val bucket = _split.asInstanceOf[Bucket]

    bucket.ranges.toVector
      .flatTraverse { r =>
        client.dataPoints
          .queryById(r.id, r.start, r.end, limit = Some(100000))
          .map(
            dataPoints =>
              dataPoints
                .map { p =>
                  DataPointsItem(
                    Some(r.id),
                    None,
                    java.sql.Timestamp.from(p.timestamp),
                    p.value,
                    None,
                    None)
                }
                .map(toRow)
                .toVector)
      }
      .map(r => config.limitPerPartition.fold(r)(limit => r.take(limit)))
      .unsafeRunSync()
      .toIterator
  }
}
