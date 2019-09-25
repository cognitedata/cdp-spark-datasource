package cognite.spark

import cats.effect.IO
import com.cognite.sdk.scala.v1.GenericClient
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.cognite.sdk.scala.common.{Auth, DataPoint => SdkDataPoint}

case class NumericDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    getIOs: GenericClient[IO, Nothing] => Seq[(DataPointsFilter, IO[Map[String, Seq[SdkDataPoint]]])],
    toRow: DataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {

  implicit val auth: Auth = config.auth
  import CdpConnector.sttpBackend
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
      .map {
        case (agg, ds) => {
          ds.map(
            p =>
              DataPointsItem(
                filter.id,
                filter.externalId,
                java.sql.Timestamp.from(p.timestamp),
                p.value,
                Some(agg),
                filter.granularity
            ))
        }
      }
      .flatMap(_.map(toRow))
      .toIterator
  }
}
