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
import cats.data._
import cognite.spark.PushdownUtilities.{pushdownToParameters, toPushdownFilterExpression}
import fs2.Stream
import org.apache.spark.sql.sources.{
  And,
  Filter,
  GreaterThan,
  GreaterThanOrEqual,
  LessThan,
  LessThanOrEqual
}

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
    toRow: DataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {

  implicit val auth: Auth = config.auth
  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  private def countsToRanges(
      id: Long,
      counts: Seq[SdkDataPoint],
      granularity: Granularity,
      ranges: Seq[Range] = Seq.empty,
      countSum: Long = 0,
      countStart: Option[Instant] = None): Seq[Range] =
    counts match {
      case count +: moreCounts =>
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

  val partitionSize = 100000
  val bucketSize = 2000000

  private def smallEnoughRanges(
      id: Long,
      start: Instant,
      end: Instant,
      granularities: Seq[Granularity] = Seq(
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
        Granularity(1, ChronoUnit.HOURS)
      )): IO[Seq[Range]] =
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

  private def buckets(
      ids: Seq[Long],
      externalIds: Seq[String],
      start: Instant,
      end: Instant): Seq[Bucket] = {
    val firstLatest = Stream
      .emits(ids)
      .covary[IO]
      .chunkLimit(100)
      .parEvalMapUnordered(50) { chunk =>
        val firsts = chunk.map { id =>
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
        val lasts = client.dataPoints.getLatestDataPointsByIds(chunk.toList)
        val b = (firsts, lasts).parMapN {
          case (f, l) =>
            f.map {
              case (id, first) =>
                (id, first, l.getOrElse(id, None))
            }
        }
        b
      }
      .flatMap(Stream.chunk)

    val ranges = firstLatest
      .parEvalMapUnordered(50) {
        case (id, Some(first), Some(latest)) =>
          if (latest.timestamp.compareTo(first.timestamp) > 0) {
            smallEnoughRanges(id, first.timestamp, latest.timestamp)
          } else {
            IO(Seq(Range(id, first.timestamp, latest.timestamp.plusMillis(1), 1)))
          }
        case _ => IO(Seq.empty)
      }
      .map(Stream.emits)
      .flatten

    val ranges1 = ranges.compile.toVector.map(Random.shuffle(_)).unsafeRunSync()

    val bucketsFold = ranges1.foldLeft((Seq.empty[Bucket], Seq.empty[Range], 0L)) {
      case ((buckets, ranges, sum), r) =>
        val sumTotal = sum + r.count
        if (sumTotal > bucketSize) {
          (Bucket(1, ranges) +: buckets, Seq(r), r.count)
        } else {
          (buckets, r +: ranges, sumTotal)
        }
    }
    val buckets = bucketsFold match {
      case (buckets, _, 0) => buckets
      case (buckets, ranges, _) => Bucket(1, ranges) +: buckets
    }

    buckets
  }

  def timeStampStringToMin(value: Any, adjustment: Long): Min =
    Min(java.sql.Timestamp.valueOf(value.toString).toInstant.plusMillis(adjustment))

  def timeStampStringToMax(value: Any, adjustment: Long): Max =
    Max(java.sql.Timestamp.valueOf(value.toString).toInstant.plusMillis(adjustment))

  def getTimestampLimit(filter: Filter): Seq[Limit] =
    filter match {
      case LessThan("timestamp", value) => Seq(timeStampStringToMax(value, -1))
      case LessThanOrEqual("timestamp", value) => Seq(timeStampStringToMax(value, 0))
      case GreaterThan("timestamp", value) => Seq(timeStampStringToMin(value, 1))
      case GreaterThanOrEqual("timestamp", value) => Seq(timeStampStringToMin(value, 0))
      case And(f1, f2) => getTimestampLimit(f1) ++ getTimestampLimit(f2)
      // case Or(f1, f2) => we might possibly want to do something clever with joining an "or" clause
      //                    with timestamp limits on each side (including replacing "max of Min's" with the less strict
      //                    "min of Min's" when aggregating filters on the same side); just ignore them for now
      case _ => Seq.empty
    }

  def filtersToTimestampLimits(filters: Array[Filter]): (Instant, Instant) = {
    val timestampLimits = filters.flatMap(getTimestampLimit)

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
        .getOrElse(Instant.ofEpochMilli(Long.MaxValue))
    )
  }

  override def getPartitions: Array[Partition] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val ids = filtersAsMaps.flatMap(m => m.get("id")).map(_.toLong).distinct
    val externalIds = filtersAsMaps.flatMap(m => m.get("externalId")).distinct

    val (lowerTimeLimit, upperTimeLimit) = filtersToTimestampLimits(filters)
    buckets(ids, externalIds, lowerTimeLimit, upperTimeLimit).zipWithIndex.map {
      case (bucket, index) => bucket.copy(index = index)
    }.toArray
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
      .unsafeRunSync()
      .toIterator
  }
}
