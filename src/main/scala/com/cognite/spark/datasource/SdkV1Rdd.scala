package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.common.{Auth, Readable}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class SdkPartition(cursor: Option[String], size: Option[Int], index: Int) extends Partition

case class SdkV1Rdd[A](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    cursors: Iterator[(Option[String], Option[Int])],
    toRow: A => Row,
    clientToResource: GenericClient[IO, Nothing] => Readable[A, IO])
    extends RDD[Row](sparkContext, Nil) {

  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  override def getPartitions: Array[Partition] =
    cursors.toIndexedSeq.zipWithIndex.map {
      case ((cursor, size), index) =>
        val partitionSize = (config.limit, size) match {
          case (None, s) => s
          case (Some(l), Some(s)) => Some(scala.math.min(l, s))
          case (l, None) => l
        }
        SdkPartition(cursor, partitionSize, index)
    }.toArray

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[SdkPartition]

    clientToResource(client)
      .readWithCursor(split.cursor, config.limit.map(_.toLong))
      .unsafeRunSync()
      .items
      .map(toRow)
      .toIterator
  }
}
