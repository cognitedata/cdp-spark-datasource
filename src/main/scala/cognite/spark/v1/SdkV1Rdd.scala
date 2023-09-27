package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1._
import fs2.concurrent.SignallingRef
import fs2.{Chunk, Stream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

final case class SdkV1Rdd[A, I](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    toRow: (A, Option[Int]) => Row,
    uniqueId: A => I,
    getStreams: GenericClient[IO] => Seq[Stream[IO, A]],
    deduplicateRows: Boolean = true)
    extends RDD[Row](sparkContext, Nil) {
  import CdpConnector._

  type EitherQueue = ArrayBlockingQueue[Either[Throwable, Chunk[A]]]

  @transient lazy val client: GenericClient[IO] = CdpConnector.clientFromConfig(config)

  private def getNumberOfSparkPartitions(cdfStreams: Seq[Stream[IO, A]]): Int =
    Math.min(config.sparkPartitions, cdfStreams.length)

  override def getPartitions: Array[Partition] = {
    val numPartitions = getNumberOfSparkPartitions(getStreams(client))
    0.until(numPartitions).toArray.map(CdfPartition)
  }

  private def getSinglePartitionStream(streams: Seq[Stream[IO, A]], index: Int): Stream[IO, A] = {
    val nPartitions = getNumberOfSparkPartitions(streams)
    streams
      .grouped(nPartitions) // group into chunks up to nPartitions in size each
      .flatMap(x => x.slice(index, index + 1)) // select at most one element from each
      .reduce(_.merge(_)) // combine into single stream
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition] // scalafix:ok

    // Spark doesn't cancel tasks immediately if using a normal
    // iterator. Instead, they provide InterruptibleIterator,
    // which does. We also need to interrupt our streams to stop
    // reading data from CDF, which can continue for a while even
    // after the iterator has stopped.
    val shouldStop = SignallingRef[IO, Boolean](false).unsafeRunSync()
    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit] { _ =>
        shouldStop.set(true).unsafeRunSync()
      }
    }

    val streams = getStreams(client)
      .map(_.interruptWhen(shouldStop))
    val currentStreamsAsSingleStream = getSinglePartitionStream(streams, split.index)

    val processChunk = if (deduplicateRows) {
      val processedIds = new ConcurrentHashMap[I, Unit]
      Some((chunk: Chunk[A]) => {
        chunk.filter { i =>
          // putIfAbsent returns null if the key did not exist, in which ase we
          // should keep (and process) the item.
          Option(processedIds.putIfAbsent(uniqueId(i), ())).isEmpty
        }
      })
    } else {
      None
    }
    val it =
      StreamIterator(currentStreamsAsSingleStream, config.parallelismPerPartition * 2, processChunk)
        .map(toRow(_, Some(split.index)))
    Option(context) match {
      case Some(ctx) => new InterruptibleIterator(ctx, it)
      case None => it
    }
  }
}
