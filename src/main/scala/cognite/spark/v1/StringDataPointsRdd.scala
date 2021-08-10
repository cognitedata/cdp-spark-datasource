package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.common.StringDataPoint
import com.cognite.sdk.scala.v1._
import sttp.client3.SttpBackend
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

final case class StringDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    getIOs: GenericClient[IO] => Seq[(StringDataPointsFilter, IO[Seq[StringDataPoint]])],
    toRow: StringDataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  override def getPartitions: Array[Partition] = {
    val numberofIOs = getIOs(client).length
    0.until(numberofIOs).toArray.map(CdfPartition)
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition]
    val (filter, io) = getIOs(client)(split.index)

    new InterruptibleIterator(
      context,
      io.unsafeRunSync()
        .map { sdp =>
          toRow(
            StringDataPointsItem(
              filter.id,
              filter.externalId,
              sdp.timestamp,
              sdp.value
            ))
        }
        .toIterator
    )
  }
}
