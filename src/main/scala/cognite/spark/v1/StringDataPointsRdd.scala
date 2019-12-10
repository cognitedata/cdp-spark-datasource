package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.common.{Auth, StringDataPoint}
import com.cognite.sdk.scala.v1._
import com.softwaremill.sttp.SttpBackend
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class StringDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    getIOs: GenericClient[IO, Nothing] => Seq[(StringDataPointsFilter, IO[Seq[StringDataPoint]])],
    toRow: StringDataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {

  implicit val auth: Auth = config.auth
  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  override def getPartitions: Array[Partition] = {
    val numberofIOs = getIOs(client).length
    0.until(numberofIOs).toArray.map(CdfPartition)
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition]
    val io = getIOs(client)(split.index)
    val filter = io._1

    io._2
      .unsafeRunSync()
      .map { sdp =>
        {
          StringDataPointsItem(
            filter.id,
            filter.externalId,
            sdp.timestamp,
            sdp.value
          )
        }
      }
      .map(toRow)
      .toIterator
  }
}
