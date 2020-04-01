package cognite.spark.v1

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, Executors}

import cats.effect.{Concurrent, IO}
import fs2.{Chunk, Stream}
import org.apache.spark.sql.Row

import scala.concurrent.{ExecutionContext, Future}

object StreamIterator {
  type EitherQueue = ArrayBlockingQueue[Either[Throwable, Chunk[Row]]]

  def apply(
      stream: Stream[IO, Row],
      queueBufferSize: Int,
      // rename toId to processItem(s), add filterItem(s)
      toId: Option[Row => String] = None,
      limit: Option[Int])(implicit concurrent: Concurrent[IO]): Iterator[Row] = {
    // This pool will be used for draining the queue
    // Draining needs to have a separate pool to continuously drain the queue
    // while another thread pool fills the queue with data from CDF
    val drainPool = Executors.newFixedThreadPool(1)
    val drainContext = ExecutionContext.fromExecutor(drainPool)

    // Local testing show this queue never holds more than 5 chunks since CDF is the bottleneck.
    // Still setting this to 2x the number of streams being read to makes sure this doesn't block
    // too early, for example in the event that all streams return a chunk at the same time.
    val queue = new EitherQueue(queueBufferSize)

    val putOnQueueStream =
      enqueueStreamResults(stream, queue, queueBufferSize, toId)
        .handleErrorWith(e => Stream.eval(IO(queue.put(Left(e)))) ++ Stream.raiseError[IO](e))

    // Continuously read the stream data into the queue on a separate thread pool
    val streamsToQueue: Future[Unit] = Future {
      putOnQueueStream.compile.drain.unsafeRunSync()
    }(drainContext)

    queueIterator(queue, streamsToQueue, limit) {
      if (!drainPool.isShutdown) {
        drainPool.shutdown()
      }
    }
  }

  // We avoid draining all streams from CDF completely and then building the Iterator,
  // by using a blocking EitherQueue.
  def enqueueStreamResults(
      stream: Stream[IO, Row],
      queue: EitherQueue,
      queueBufferSize: Int,
      toId: Option[Row => String] = None)(implicit concurrent: Concurrent[IO]): Stream[IO, Unit] = {
    val maybeProcessedIds: Option[ConcurrentHashMap[String, Unit]] =
      toId.map(_ => new ConcurrentHashMap[String, Unit])
    stream.chunks.parEvalMapUnordered(queueBufferSize) { chunk =>
      val freshIds = maybeProcessedIds match {
        case Some(processedIds) =>
          val toIdFunction = toId.get
          chunk.filter(item => !processedIds.containsKey(toIdFunction(item)))
        case None =>
          chunk
      }
      IO {
        maybeProcessedIds.foreach { processedIds =>
          val toIdFunction = toId.get
          freshIds.foreach { item =>
            processedIds.put(toIdFunction(item), ())
          }
        }
        queue.put(Right(freshIds))
      }
    }
  }

  def queueIterator(queue: EitherQueue, f: Future[Unit], limit: Option[Int])(
      doCleanup: => Unit): Iterator[Row] =
    new Iterator[Row] {
      var nextItems: Iterator[Row] = Iterator.empty
      var itemsProcessed: Long = 0

      override def hasNext: Boolean =
        if (nextItems.hasNext && limit.forall(itemsProcessed < _)) {
          true
        } else if (limit.exists(itemsProcessed >= _)) {
          doCleanup
          false
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

      override def next(): Row = {
        itemsProcessed += 1
        nextItems.next()
      }

      def iteratorFromQueue(): Iterator[Row] =
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
