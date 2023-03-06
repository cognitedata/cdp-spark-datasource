package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1._
import fs2.concurrent.SignallingRef
import fs2.{Chunk, Stream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

final case class CdfPartition(index: Int) extends Partition

final case class SdkV1Rdd[A, I](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    toRow: (A, Option[Int]) => Row,
    uniqueId: A => I,
    getStreams: (GenericClient[IO], Option[Int], Int) => Seq[Stream[IO, A]],
    deduplicateRows: Boolean = true)
    extends RDD[Row](sparkContext, Nil) {
  import CdpConnector._

  type EitherQueue = ArrayBlockingQueue[Either[Throwable, Chunk[A]]]

  @transient lazy val client: GenericClient[IO] = CdpConnector.clientFromConfig(config)

  override def getPartitions: Array[Partition] = {
    val numberOfPartitions =
      getStreams(client, config.limitPerPartition, config.partitions)
        .grouped(config.parallelismPerPartition)
        .length
    0.until(numberOfPartitions).toArray.map(CdfPartition)
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

    val streams = getStreams(client, config.limitPerPartition, config.partitions)
      .map(_.interruptWhen(shouldStop))
    val groupedStreams = streams.grouped(config.parallelismPerPartition).toSeq
    val currentStreamsAsSingleStream = groupedStreams(split.index).reduce(_.merge(_))

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
