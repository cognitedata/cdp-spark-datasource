package cognite.spark

import java.util.concurrent.{ArrayBlockingQueue, Executors}

import cats.effect.{ContextShift, IO}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.common.Auth
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import fs2._
import scala.math.ceil
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

case class CdfPartition(index: Int) extends Partition

case class SdkV1Rdd[A](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    toRow: A => Row,
    uniqueId: A => Long,
    getStreams: (GenericClient[IO, Nothing], Option[Int], Int) => Seq[Stream[IO, A]])
    extends RDD[Row](sparkContext, Nil) {

  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  override def getPartitions: Array[Partition] = {
    val numberOfPartitions =
      getStreams(client, config.limit, config.partitions).grouped(config.parallelismPerPartition).length
    0.until(numberOfPartitions).toArray.map(CdfPartition)
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition]
    val drainPool =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val contextShift: ContextShift[IO] =
      IO.contextShift(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1)))

    val processedItems = mutable.Set[Long]()

    val streams =
      getStreams(client, config.limit, config.partitions)
    val groupedStreams = streams.grouped(config.parallelismPerPartition)

    val currentStreamsAsSingleStream = groupedStreams
      .drop(split.index)
      .next
      .reduce(_.merge(_))

    val queue: ArrayBlockingQueue[Vector[A]] =
      new ArrayBlockingQueue[Vector[A]](config.parallelismPerPartition * 2)

    val putOnQueueStream = enqueueStreamResults(currentStreamsAsSingleStream, queue, processedItems)

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
      queue: ArrayBlockingQueue[Vector[A]],
      processedItems: mutable.Set[Long]): Stream[IO, Unit] =
    for {
      s <- stream.chunks
      put <- {
        // Filter out and keep track of already seen items
        // since overlapping pushdown filters may read duplicates.
        val freshItems = s.toVector.filterNot(i => processedItems.contains(uniqueId(i)))
        processedItems ++= freshItems.map(uniqueId)
        Stream.eval(IO(queue.put(freshItems)))
      }
    } yield put

  def queueIterator(queue: ArrayBlockingQueue[Vector[A]], f: Future[Unit]): Iterator[Row] =
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
        Option(queue.poll()).map(_.toIterator).getOrElse(Iterator.empty)
    }
}
