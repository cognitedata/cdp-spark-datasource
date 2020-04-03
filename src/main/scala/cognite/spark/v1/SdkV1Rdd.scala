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
    val currentStreamsAsSingleStream = groupedStreams(split.index).reduce(_.merge(_))

    val processedIds = new ConcurrentHashMap[I, Unit]
    val processChunk = (chunk: Chunk[A]) => {
      chunk.filter { i =>
        // putIfAbsent returns null if the key did not exist, in which ase we
        // should keep (and process) the item.
        Option(processedIds.putIfAbsent(uniqueId(i), ())).isEmpty
      }
    }
    StreamIterator(currentStreamsAsSingleStream, config.parallelismPerPartition * 2, Some(processChunk))
      .map(toRow)
  }
}
