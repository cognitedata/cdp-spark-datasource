package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.{DataPoint => SdkDataPoint}
import com.cognite.sdk.scala.v1._
import fs2.concurrent.SignallingRef
import fs2.{Chunk, Stream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.Ordering.Implicits._
import scala.annotation.tailrec

sealed trait Range {
  val count: Option[Long]
  val id: CogniteId
}

final case class DataPointsRange(id: CogniteId, start: Instant, end: Instant, count: Option[Long])
    extends Range

final case class AggregationRange(
    id: CogniteId,
    start: Instant,
    end: Instant,
    count: Option[Long],
    granularity: Granularity,
    aggregation: String)
    extends Range

final case class Bucket(index: Int, ranges: Seq[Range]) extends Partition

final case class WrongDatapointTypeException(
    id: Long,
    externalId: Option[String],
    shouldBeString: Boolean)
    extends CdfSparkException((shouldBeString match {
      case false =>
        s"""Cannot read string data points as numeric datapoints. Use .option("type", "stringdatapoints") or _cdf.stringdatapoints. """
      case true =>
        s"""Cannot read numeric data points as string datapoints. Use .option("type", "datapoints") or _cdf.datapoints. """
    }) + s"The timeseries id=$id, externalId=${externalId.getOrElse("NULL")}")

object WrongDatapointTypeException {
  def check(isString: Boolean, id: Long, externalId: Option[String], shouldBeString: Boolean): Unit =
    if (isString != shouldBeString) {
      throw WrongDatapointTypeException(id, externalId, shouldBeString)
    }
}

final case class NumericDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    ids: Seq[CogniteId],
    timestampLimits: (Instant, Instant),
    aggregations: Array[AggregationFilter],
    granularities: Seq[Granularity],
    increaseReadMetrics: Int => Unit,
    rowIndices: Array[Int]
) extends RDD[Row](sparkContext, Nil) {
  import CdpConnector._

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  private val (lowerTimeLimit, upperTimeLimit) = timestampLimits

  @tailrec
  private def countsToRanges(
      id: CogniteId,
      counts: Seq[SdkDataPoint],
      granularity: Granularity,
      ranges: Seq[Range] = Seq.empty,
      countSum: Long = 0,
      countStart: Option[Instant] = None,
      countEnd: Option[Instant] = None): Seq[Range] =
    counts match {
      case count +: moreCounts =>
        val accumulatedCount = count.value.toLong + countSum
        if (accumulatedCount > partitionSize) {
          val newRange = DataPointsRange(
            id,
            countStart.getOrElse(count.timestamp),
            countStart
              .map(_ => count.timestamp)
              .getOrElse(count.timestamp.plus(granularity.amount, granularity.unit)),
            Some(countSum)
          )
          countsToRanges(id, counts, granularity, newRange +: ranges, 0, None)
        } else {
          countsToRanges(
            id,
            moreCounts,
            granularity,
            ranges,
            accumulatedCount,
            countStart.orElse(Some(count.timestamp)),
            Some(count.timestamp.plus(granularity.amount, granularity.unit))
          )
        }
      case _ =>
        countStart match {
          case Some(start) =>
            val end = countEnd
              .getOrElse(start.plus(granularity.amount, granularity.unit))
              .plusMillis(1) // Add 1 millisecond as end is exclusive
            val lastRange =
              DataPointsRange(id, start, end, Some(countSum))
            lastRange +: DataPointsRange(id, end, end.plus(granularity.amount, granularity.unit), None) +: ranges
          case _ => ranges
        }
    }

  // Max number of datapoints we *want* to have per Range
  // In case the range is bigger than 100k - the CDF limit, we'll
  // use paging to download all datapoints.
  // 10 * Constants.DefaultDataPointsLimit will give us at most 10 pages per range,
  // which should not slow us down significantly
  // We can also have at most 10k separate ranges, because the API does not give us more than
  // 10k aggregates. These cases should be quite rare, but in such case,
  // we'll likely have larger Range than this partitionSize and have to page
  // through more than 10 pages.
  private val partitionSize = 10 * Constants.DefaultDataPointsLimit
  // This bucketSize results in a partition size of around 100-150 MiB,
  // which is a reasonable number. We could increase this, at the cost
  // of reducing the parallelism of smaller time series.
  private val bucketSize = 5000000
  // This must not exceed the limits of CDF.
  private val maxPointsPerAggregationRange = 10000

  private val granularitiesToTry = Seq(
    Granularity(Some(900), ChronoUnit.DAYS),
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
    Granularity(Some(15), ChronoUnit.MINUTES),
    Granularity(Some(1), ChronoUnit.MINUTES),
    Granularity(Some(30), ChronoUnit.SECONDS),
    Granularity(Some(1), ChronoUnit.SECONDS)
  )

  private def queryAggregates(
      id: CogniteId,
      start: Instant,
      end: Instant,
      granularity: String,
      aggregates: Seq[String],
      limit: Int) =
    client.dataPoints
      .queryAggregates(
        Seq(id),
        start,
        end,
        granularity.toString,
        aggregates,
        limit = Some(limit),
        ignoreUnknownIds = true
      )

  private def floorToNearest(x: Long, base: Double) =
    (base * math.floor(x.toDouble / base)).toLong

  private def ceilToNearest(x: Long, base: Double) =
    (base * math.ceil(x.toDouble / base)).toLong

  /** Splits the range into ranges small enough to fit into $partitionSize. Returns:
    * None = that means that we could not split it into ranges, and the higher granularity must be used
    * Some(list of ranges) ->
    *   - Some(Seq.empty) would be treated as "there are no ranges" */
  private def smallEnoughRanges(
      id: CogniteId,
      start: Instant,
      end: Instant,
      granularities: Seq[Granularity] = granularitiesToTry): IO[Option[Seq[Range]]] =
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
          maxPointsPerAggregationRange
        ).flatMap { aggregates =>
          aggregates.get("count") match {
            case Some(countsResponses) =>
              val counts = countsResponses.flatMap(_.datapoints)
              lazy val rangesFromCounts = Some(countsToRanges(id, counts, granularity))
              if (counts.length == maxPointsPerAggregationRange) {
                // we have probably ran out of the limit of 10k partitions,
                // so we'll refuse this granularity
                IO.pure(None)
              } else if (counts.map(_.value).max > partitionSize && moreGranular.nonEmpty) {
                smallEnoughRanges(id, start, end, moreGranular)
                  .map(_.orElse(rangesFromCounts))
              } else {
                IO.pure(rangesFromCounts)
              }
            case None =>
              // can't rely on aggregates for this range so page through it
              IO.pure(Some(Seq(DataPointsRange(id, start, end, None))))
          }
        }
    }

  private def queryDatapointsById(id: CogniteId, start: Instant, end: Instant, limit: Int) =
    client.dataPoints
      .query(
        Seq(id),
        start,
        end,
        limit = Some(limit),
        ignoreUnknownIds = true
      )
      .map(_.headOption
        .map { ts =>
          WrongDatapointTypeException
            .check(ts.isString, ts.id, ts.externalId, shouldBeString = false)
          ts.datapoints
        }
        .getOrElse(Seq.empty))

  private def getFirstAndLastConcurrentlyById(
      ids: Vector[CogniteId],
      start: Instant,
      end: Instant): IO[Vector[(CogniteId, Option[Instant], Option[Instant])]] =
    for {
      // fetch firsts before lasts since that can correctly handle when accidentally reading
      // stringdatapoints with a reasonable error message
      firsts <- ids.map { id =>
        queryDatapointsById(id, start, end.max(start.plusMillis(1)), 1)
          .map(datapoints => id -> datapoints.headOption)
      }.parSequence
      latest <- client.dataPoints.getLatestDataPoints(
        ids,
        ignoreUnknownIds = true,
        end.toEpochMilli.toString // use end instant as upper bound so we can read dataPoints in the future
      )
    } yield
      firsts.map {
        case (id, first) =>
          (id, first.map(_.timestamp), latest.getOrElse(id, None).map(_.timestamp.min(end)))
      }

  private def rangesToBuckets(ranges: Seq[Range]): Vector[Bucket] = {
    // Fold into a sequence of buckets, where each bucket has some ranges with a
    // total of data points <= bucketSize
    val bucketsFold = ranges.foldLeft((Seq.empty[Bucket], Seq.empty[Range], 0L)) {
      case ((buckets, ranges, sum), r) =>
        r.count match {
          case Some(count) =>
            val sumTotal = sum + count
            if (sumTotal > bucketSize) {
              // Create a new bucket from the ranges, and assign an index of "1" temporarily.
              // We'll change the index before returning the buckets.
              (Bucket(1, ranges) +: buckets, Seq(r), count)
            } else {
              (buckets, r +: ranges, sumTotal)
            }
          case None =>
            // Ranges without a count get their own buckets,
            // because we don't know how many points they contain.
            val newBuckets = if (ranges.isEmpty) {
              buckets
            } else {
              Bucket(1, ranges) +: buckets
            }
            (Bucket(1, Seq(r)) +: newBuckets, Seq.empty, 0)
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
      firstLatest: Stream[IO, (CogniteId, Option[Instant], Option[Instant])]): IO[Seq[Bucket]] = {

    val ranges = firstLatest
      .parEvalMapUnordered(50) {
        case (id, Some(first), Some(latest)) =>
          if (latest >= first) {
            smallEnoughRanges(id, first, latest).map(
              _.getOrElse(sys.error(s"Too many datapoints even for the highest granularity.")))
          } else {
            IO.pure(Seq(DataPointsRange(id, first, latest.plusMillis(1), Some(1))))
          }
        case _ => IO.pure(Seq.empty)
      }
      .map(Stream.emits)
      .flatten

    ranges.compile.toVector.map(rangesToBuckets)
  }

  private def aggregationBuckets(
      aggregations: Seq[AggregationFilter],
      granularity: Granularity,
      firstLatest: Stream[IO, (CogniteId, Option[Instant], Option[Instant])]
  ): IO[Vector[Bucket]] = {
    val granularityMillis = granularity.unit.getDuration.multipliedBy(granularity.amount).toMillis
    // TODO: make sure we have a test that covers more than 10000 units
    firstLatest
      .parEvalMapUnordered(50) {
        case (id, Some(first), Some(latest)) =>
          val aggStart = Instant.ofEpochMilli(floorToNearest(first.toEpochMilli, granularityMillis))
          val aggEnd = Instant
            .ofEpochMilli(ceilToNearest(latest.toEpochMilli, granularityMillis))
            .plusMillis(1) // Add 1 millisecond as end is exclusive

          val d1 = Duration.between(aggStart, aggEnd)
          val numValues = d1.toMillis / granularity.unit.getDuration.toMillis
          val numRanges = Math.ceil(numValues.toDouble / maxPointsPerAggregationRange).toLong.max(1)

          val ranges = for {
            a <- aggregations
            i <- 0L until numRanges
            rangeStart = aggStart.plus((maxPointsPerAggregationRange * i).max(0), granularity.unit)
            rangeEnd = rangeStart.plus(maxPointsPerAggregationRange, granularity.unit).min(aggEnd)
            nPoints = Duration
              .between(rangeStart, rangeEnd)
              .toMillis / granularity.unit.getDuration.toMillis
          } yield AggregationRange(id, rangeStart, rangeEnd, Some(nPoints), granularity, a.aggregation)
          IO(ranges)
        case _ => IO(Seq.empty)
      }
      .map(Stream.emits)
      .flatten
      .compile
      .toVector
      .map(rangesToBuckets)
  }

  override def getPartitions: Array[Partition] = {
    val firstLatest = Stream
      .emits(ids)
      .covary[IO]
      .chunkLimit(100)
      .parEvalMapUnordered(50) { chunk =>
        getFirstAndLastConcurrentlyById(chunk.toVector, lowerTimeLimit, upperTimeLimit)
      }
      .flatMap(Stream.emits)
    val partitions = if (granularities.isEmpty) {
      buckets(firstLatest)
    } else {
      granularities.toVector
        .map(g => aggregationBuckets(aggregations, g, firstLatest))
        .parFlatSequence
    }
    partitions
      .map(_.toArray[Partition])
      .unsafeRunSync()
  }

  private def queryDoubles(id: CogniteId, lowerLimit: Instant, upperLimit: Instant, limit: Int) =
    queryDatapointsById(id, lowerLimit, upperLimit, limit).map { dataPoints =>
      val lastTimestamp = dataPoints.lastOption.map(_.timestamp)
      (lastTimestamp, dataPoints)
    }

  private def queryDataPointsRange(dataPointsRange: DataPointsRange) = {
    val points = DataPointsRelationV1
      .getAllDataPoints[SdkDataPoint](
        queryDoubles,
        config.batchSize.getOrElse(Constants.DefaultDataPointsLimit),
        dataPointsRange.id,
        dataPointsRange.start,
        dataPointsRange.end)
      .stream
      .mapChunks { allDataPoints =>
        increaseReadMetrics(allDataPoints.size)
        allDataPoints.map(p => dataPointToRow(dataPointsRange.id, p))
      }
    points
  }

  private val rowIndicesLength = rowIndices.length

  // Those methods are made to go fast, not to look pretty.
  // Called for every data point received. Make sure to run benchmarks checking
  // total time taken, garbage collection time, and memory usage after changes.
  @inline
  // scalastyle:off cyclomatic.complexity
  private def dataPointToRow(id: CogniteId, dataPoint: SdkDataPoint): Row = {
    val array = new Array[Any](rowIndicesLength)
    var i = 0
    while (i < rowIndicesLength) {
      rowIndices(i) match {
        case 0 =>
          array(i) = id match {
            case CogniteInternalId(id) => id
            case _ => null // scalastyle:off null
          }
        case 1 =>
          array(i) = id match {
            case CogniteExternalId(externalId) => externalId
            case _ => null // scalastyle:off null
          }
        case 2 => array(i) = java.sql.Timestamp.from(dataPoint.timestamp)
        case 3 => array(i) = dataPoint.value
        case 4 | 5 => array(i) = null // scalastyle:off null
      }
      i += 1
    }
    new GenericRow(array)
  }

  @inline
  private def aggregationDataPointToRow(r: AggregationRange, dataPoint: SdkDataPoint): Row = {
    val array = new Array[Any](rowIndicesLength)
    var i = 0
    while (i < rowIndicesLength) {
      rowIndices(i) match {
        case 0 =>
          array(i) = r.id match {
            case CogniteInternalId(id) => id
            case _ => null // scalastyle:off null
          }
        case 1 =>
          array(i) = r.id match {
            case CogniteExternalId(externalId) => externalId
            case _ => null // scalastyle:off null
          }
        case 2 => array(i) = java.sql.Timestamp.from(dataPoint.timestamp)
        case 3 => array(i) = dataPoint.value
        case 4 => array(i) = r.aggregation
        case 5 => array(i) = r.granularity.toString
      }
      i += 1
    }
    new GenericRow(array)
  }

  private def maybeLimitStream[A](stream: Stream[IO, A]) =
    config.limitPerPartition match {
      case Some(limit) => stream.take(limit)
      case None => stream
    }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val bucket = _split.asInstanceOf[Bucket]
    val maxParallelism = scala.math.max(bucket.ranges.size, 500)

    // See comments in SdkV1Rdd.compute for an explanation.
    val shouldStop = SignallingRef[IO, Boolean](false).unsafeRunSync()
    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit] { _ =>
        shouldStop.set(true).unsafeRunSync()
      }
    }

    val stream: Stream[IO, Stream[IO, Row]] = Stream
      .emits(bucket.ranges.toVector)
      .covary[IO]
      // We use Int.MaxValue because we want this to be limited only by the parallelism
      // offered by the runtime environment (execution contexts etc.)
      // which is similar to what parJoin does: parJoinUnbounded = parJoin(Int.MaxValue)
      // As of 2020-04-03 there is no parEvalMapUnorderedUnbounded (which is a mouthful).
      // In fact: parEvalMapUnordered = map(o => Stream.eval(f(o))).parJoin(maxConcurrent)
      .parEvalMapUnordered(Int.MaxValue) {
        case r: DataPointsRange => IO(maybeLimitStream(queryDataPointsRange(r)))
        case r: AggregationRange =>
          queryAggregates(r.id, r.start, r.end, r.granularity.toString, Seq(r.aggregation), 10000)
            .flatMap { queryResponse =>
              val dataPointsAggregates =
                queryResponse.mapValues(dataPointsResponse =>
                  dataPointsResponse.flatMap(_.datapoints.map(aggregationDataPointToRow(r, _))))
              dataPointsAggregates.get(r.aggregation) match {
                case Some(dataPoints) =>
                  IO {
                    increaseReadMetrics(dataPoints.size)
                    maybeLimitStream(Stream.chunk(Chunk.seq(dataPoints)).covary[IO])
                  }
                case None => IO(Stream.chunk(Chunk.empty[Row]).covary[IO])
              }
            }
        case _ => IO(Stream.chunk(Chunk.empty[Row]).covary[IO])
      }

    val it = StreamIterator(
      stream.parJoin(maxParallelism).interruptWhen(shouldStop),
      maxParallelism,
      None
    )
    Option(context) match {
      case Some(ctx) => new InterruptibleIterator(ctx, it)
      case None => it
    }
  }
}
