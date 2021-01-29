package cognite.spark.v1

import java.util.concurrent.{ArrayBlockingQueue, Executors}

import cats.effect.{Concurrent, IO}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import fs2.{Chunk, Stream}

import scala.concurrent.{ExecutionContext, Future}

object StreamIterator {
  type EitherQueue[A] = ArrayBlockingQueue[Either[Throwable, Chunk[A]]]

  private val threadFactory = new ThreadFactoryBuilder()
    .setNameFormat("CDF-Spark-Drain-%d")
    .setDaemon(true)
    .build()

  def apply[A](stream: Stream[IO, A], queueBufferSize: Int, processChunk: Option[Chunk[A] => Chunk[A]])(
      implicit concurrent: Concurrent[IO]): Iterator[A] = {
    // This pool will be used for draining the queue
    // Draining needs to have a separate pool to continuously drain the queue
    // while another thread pool fills the queue with data from CDF
    val drainPool = Executors.newFixedThreadPool(1, threadFactory)
    val drainContext = ExecutionContext.fromExecutor(drainPool)

    // Local testing show this queue never holds more than 5 chunks since CDF is the bottleneck.
    // Still setting this to 2x the number of streams being read to makes sure this doesn't block
    // too early, for example in the event that all streams return a chunk at the same time.
    val queue = new EitherQueue[A](queueBufferSize)

    val putOnQueueStream =
      enqueueStreamResults(stream, queue, queueBufferSize, processChunk)
        .handleErrorWith(e =>
          Stream.eval(IO(queue.put(Left(e)))) ++ Stream.eval(IO {
            if (!drainPool.isShutdown) {
              drainPool.shutdownNow()
            }
          }))

    // Continuously read the stream data into the queue on a separate thread pool
    val streamsToQueue: Future[Unit] = Future {
      try {
        putOnQueueStream.compile.drain.unsafeRunSync()
      } catch {
        case _: InterruptedException =>
        // Ignore this, as it means there was an exception thrown while draining the
        // stream which caused our thread pool to be shutdown, and an exception thrown
        // in iteratorFromQueue will abort this job.
      }
    }(drainContext).map { _ =>
      if (!drainPool.isShutdown) {
        drainPool.shutdownNow()
      }
      ()
    }(drainContext)

    queueIterator(queue, streamsToQueue)
  }

  // We avoid draining all streams from CDF completely and then building the Iterator,
  // by using a blocking EitherQueue.
  def enqueueStreamResults[A](
      stream: Stream[IO, A],
      queue: EitherQueue[A],
      queueBufferSize: Int,
      processChunk: Option[Chunk[A] => Chunk[A]])(
      implicit concurrent: Concurrent[IO]): Stream[IO, Unit] =
    stream.chunks.parEvalMapUnordered(queueBufferSize) { chunk =>
      IO {
        val processedChunk = processChunk.map(f => f(chunk)).getOrElse(chunk)
        queue.put(Right(processedChunk))
      }
    }

  def queueIterator[A](queue: EitherQueue[A], f: Future[Unit]): Iterator[A] =
    new Iterator[A] {
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

      override def next(): A = nextItems.next()

      def iteratorFromQueue(): Iterator[A] =
        Option(queue.poll())
          .map {
            case Right(value) => value.iterator
            case Left(err) => throw err
          }
          .getOrElse(Iterator.empty)
    }
}
