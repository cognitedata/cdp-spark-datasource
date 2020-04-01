package cognite.spark.v1

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, Executors}

import cats.effect.IO
import com.cognite.sdk.scala.common.Auth
import com.cognite.sdk.scala.v1._
import com.softwaremill.sttp.SttpBackend
import fs2.{Chunk, Stream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.concurrent.{ExecutionContext, Future}

final case class CdfPartition(index: Int) extends Partition

final case class SdkV1Rdd[A, I](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    toRow: A => Row,
    uniqueId: A => I,
    getStreams: (GenericClient[IO, Nothing], Option[Int], Int) => Seq[Stream[IO, A]])
    extends RDD[Row](sparkContext, Nil) {
  import CdpConnector._
  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)

  type EitherQueue = ArrayBlockingQueue[Either[Throwable, Chunk[A]]]

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

    val streams = getStreams(client, config.limitPerPartition, config.partitions)
    val groupedStreams = streams.grouped(config.parallelismPerPartition).toSeq
    val currentStreamsAsSingleStream = groupedStreams(split.index)
      .reduce(_.merge(_))

    // This pool will be used for draining the queue
    // Draining needs to have a separate pool to continuously drain the queue
    // while another thread pool fills the queue with data from CDF
    val drainPool = Executors.newFixedThreadPool(1)
    val drainContext = ExecutionContext.fromExecutor(drainPool)

    // Local testing show this queue never holds more than 5 chunks since CDF is the bottleneck.
    // Still setting this to 2x the number of streams being read to makes sure this doesn't block
    // too early, for example in the event that all streams return a chunk at the same time.
    val queue = new EitherQueue(config.parallelismPerPartition * 2)

    val putOnQueueStream =
      enqueueStreamResults(currentStreamsAsSingleStream, queue)
        .handleErrorWith(e => Stream.eval(IO(queue.put(Left(e)))) ++ Stream.raiseError[IO](e))

    // Continuously read the stream data into the queue on a separate thread pool
    val streamsToQueue: Future[Unit] = Future {
      putOnQueueStream.compile.drain.unsafeRunSync()
    }(drainContext)

    queueIterator(queue, streamsToQueue) {
      if (!drainPool.isShutdown) {
        drainPool.shutdown()
      }
    }
  }

  // We avoid draining all streams from CDF completely and then building the Iterator,
  // by using a blocking EitherQueue.
  def enqueueStreamResults(stream: Stream[IO, A], queue: EitherQueue): Stream[IO, Unit] = {
    val processedIds = new ConcurrentHashMap[I, Unit]
    stream.chunks.parEvalMapUnordered(config.parallelismPerPartition) { chunk =>
      val freshIds = chunk.filter(i => !processedIds.containsKey(uniqueId(i)))
      IO {
        freshIds.foreach { i =>
          processedIds.put(uniqueId(i), ())
        }
        queue.put(Right(freshIds))
      }
    }
  }

  def queueIterator(queue: EitherQueue, f: Future[Unit])(doCleanup: => Unit): Iterator[Row] =
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
          if (f.isCompleted) {
            doCleanup
          }
          nextItems.hasNext
        }

      override def next(): Row = toRow(nextItems.next())

      def iteratorFromQueue(): Iterator[A] =
        Option(queue.poll())
          .map {
            case Right(value) => value.iterator
            case Left(err) =>
              doCleanup
              throw err
          }
          .getOrElse(Iterator.empty)
    }
}
