package cognite.spark.v1

import java.util.concurrent.{ArrayBlockingQueue, Executors}

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.common.Auth
import com.softwaremill.sttp.SttpBackend
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import fs2.Stream

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

case class CdfPartition(index: Int) extends Partition

case class SdkV1Rdd[A, I](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    toRow: A => Row,
    uniqueId: A => I,
    getStreams: (GenericClient[IO, Nothing], Option[Int], Int) => Seq[Stream[IO, A]])
    extends RDD[Row](sparkContext, Nil) {
  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)

  type EitherQueue = ArrayBlockingQueue[Either[Throwable, Vector[A]]]

  implicit val auth: Auth = config.auth
  @transient lazy val client: GenericClient[IO, Nothing] =
    CdpConnector.clientFromConfig(config)

  override def getPartitions: Array[Partition] = {
    val numberOfPartitions =
      getStreams(client, config.limitPerPartition, config.partitions)
        .grouped(config.parallelismPerPartition)
        .length
    0.until(numberOfPartitions).toArray.map(CdfPartition)
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition]
    val drainPool =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    // Do not increase number of threads, as this will make processedIds non-blocking
    implicit val singleThreadedCs: ContextShift[IO] =
      IO.contextShift(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1)))

    val processedIds = mutable.Set.empty[I]

    val streams =
      getStreams(client, config.limitPerPartition, config.partitions)
    val groupedStreams = streams.grouped(config.parallelismPerPartition).toSeq

    val currentStreamsAsSingleStream = groupedStreams(split.index)
      .reduce(_.merge(_))

    // Local testing show this queue never holds more than 5 chunks since CDF is the bottleneck.
    // Still setting this to 2x the number of streams being read to makes sure this doesn't block
    // too early, for example in the event that all streams return a chunk at the same time.
    val queue =
      new EitherQueue(config.parallelismPerPartition * 2)

    val putOnQueueStream = enqueueStreamResults(currentStreamsAsSingleStream, queue, processedIds, singleThreadedCs)
      .handleErrorWith(e => Stream.eval(IO(queue.put(Left(e)))) ++ Stream.raiseError[IO](e))

    // Continuously read the stream data into the queue on a separate thread
    val streamsToQueue = Future {
      putOnQueueStream.compile.drain.unsafeRunSync()
    }(drainPool)

    queueIterator(queue, streamsToQueue)
  }

  // To avoid draining all streams from CDF completely and then building the Iterator, we read the
  // streams in chunks and add the chunks to a queue continuously.
  def enqueueStreamResults(
      stream: Stream[IO, A],
      queue: EitherQueue,
      processedIds: mutable.Set[I],
      singleThreadedCs: ContextShift[IO]): Stream[IO, Unit] =
    for {
      // Ensure that only one chunk is processed at a time
      s <- stream.chunks.flatMap(c => Stream.eval(singleThreadedCs.shift *> IO.pure(c)))
      put <- {
        // Filter out and keep track of already seen items
        // since overlapping pushdown filters may read duplicates.
        val freshIds = s.toVector.filterNot(i => processedIds.contains(uniqueId(i)))
        processedIds ++= freshIds.map(i => uniqueId(i))
        Stream.eval(IO(queue.put(Right(freshIds))))
      }
    } yield put

  def queueIterator(queue: EitherQueue, f: Future[Unit]): Iterator[Row] =
    new Iterator[Row] {
      var nextItems: Iterator[A] = Iterator.empty

      override def hasNext: Boolean =
        if (nextItems.hasNext) {
          true
        } else {
          nextItems = iteratorFromQueue()
          // The queue might be empty even if all streams have not yet been completely drained.
          // We keep polling the queue until new data is enqueued, or the stream is complete.
          while (nextItems.isEmpty && !f.isCompleted) {
            Thread.sleep(1)
            nextItems = iteratorFromQueue()
          }
          nextItems.hasNext
        }

      override def next(): Row = toRow(nextItems.next())

      def iteratorFromQueue(): Iterator[A] =
        Option(queue.poll())
          .map {
            case Right(value) => value.toIterator
            case Left(err) => throw err
          }
          .getOrElse(Iterator.empty)
    }
}
