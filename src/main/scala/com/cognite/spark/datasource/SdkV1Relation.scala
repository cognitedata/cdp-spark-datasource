package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.common.{Auth, Readable}
import com.softwaremill.sttp.Uri
import io.circe.Decoder
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

import scala.concurrent.ExecutionContext

abstract class SdkV1Relation[A, T <: Readable[A, IO], C: Decoder](
    config: RelationConfig,
    shortName: String)
    extends BaseRelation
    with CdpConnector
    with Serializable
    with TableScan {
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")
  @transient lazy private val itemsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.created")

  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client = new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  def schema: StructType

  def toRow(a: A): Row

  def clientToResource(client: GenericClient[IO, Nothing]): T

  def listUrl(version: String): Uri

  def getFromRowAndCreate(rows: Seq[Row]): IO[Seq[A]] =
    sys.error(s"Resource type $shortName does not support writing.")

  override def buildScan(): RDD[Row] =
    SdkV1Rdd[A](
      sqlContext.sparkContext,
      config,
      cursors(),
      (a: A) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(a)
      },
      clientToResource
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

  def cursors(): Iterator[(Option[String], Option[Int])] =
    NextCursorIterator[C](listUrl("0.6"), config, true)
}
