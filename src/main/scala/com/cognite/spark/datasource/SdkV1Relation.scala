package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.common.Auth
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import fs2.Stream
import scala.concurrent.ExecutionContext

abstract class SdkV1Relation[A <: Product](config: RelationConfig, shortName: String)
    extends BaseRelation
    with CdpConnector
    with Serializable
    with TableScan
    with PrunedFilteredScan {
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")
  @transient lazy private val itemsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.created")

  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client = new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  def schema: StructType

  def toRow(a: A): Row

  def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] =
    sys.error(s"Resource type $shortName does not support writing.")

  def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Long],
      numPartitions: Int): Seq[Stream[IO, A]]

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[A](
      sqlContext.sparkContext,
      config,
      (a: A) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(a, requiredColumns)
      },
      config.partitions,
      getStreams(filters)
    )

  def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup
          .parTraverse(getFromRowAndCreate)
          .flatTap { _ =>
            IO {
              if (config.collectMetrics) {
                itemsCreated.inc(batchGroup.foldLeft(0: Int)(_ + _.length))
              }
            }
          }
          .unsafeRunSync()
      }
      ()
    })

  def toRow(item: A, requiredColumns: Array[String]): Row =
    if (requiredColumns.isEmpty) {
      toRow(item)
    } else {
      val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
      val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf[String](f))
      val rowOfAllFields = toRow(item)
      Row.fromSeq(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
    }

  def insert(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "abort".""")

  def upsert(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "upsert".""")

  def update(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "update".""")

  def delete(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "delete".""")
}
