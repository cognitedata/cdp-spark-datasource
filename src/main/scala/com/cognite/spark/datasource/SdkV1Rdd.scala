package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.common.Auth
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import fs2.Stream

case class CdfPartition(index: Int) extends Partition

case class SdkV1Rdd[A](
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    toRow: A => Row,
    numPartitions: Int,
    getStreams: (GenericClient[IO, Nothing], Option[Long], Int) => Seq[Stream[IO, A]])
    extends RDD[Row](sparkContext, Nil) {

  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  override def getPartitions: Array[Partition] = {
    val numberOfStreams = getStreams(client, config.limit.map(_.toLong), numPartitions).length
    0.until(numberOfStreams).toArray.map(CdfPartition)
  }

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition]

    getStreams(client, config.limit.map(_.toLong), numPartitions)(split.index).compile.toList
      .unsafeRunSync()
      .map(toRow)
      .toIterator
  }
}
