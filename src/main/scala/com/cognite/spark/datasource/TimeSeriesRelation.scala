package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import SparkSchemaHelper._

import scala.concurrent.ExecutionContext

case class PostTimeSeriesDataItems[A](items: Seq[A])

case class TimeSeriesItem(
    name: String,
    isString: Boolean,
    metadata: Option[Map[String, Option[String]]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Boolean,
    description: Option[String],
    // need to use Vector to avoid this error:
    // Caused by: java.io.NotSerializableException: scala.Array$$anon$2
    securityCategories: Option[Vector[Long]],
    id: Long,
    createdTime: Long,
    lastUpdatedTime: Long)

case class PostTimeSeriesItem(
    name: String,
    isString: Boolean,
    metadata: Option[Map[String, String]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Boolean,
    description: Option[String],
    securityCategories: Option[Vector[Long]])

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[TimeSeriesItem](config, "3dmodelrevisionnodes")
    with InsertableRelation
    with CdpConnector {

  @transient lazy private val timeSeriesCreated =
    metricsSource.getOrCreateCounter(s"timeseries.created")

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map(r => fromRow[PostTimeSeriesItem](r))
    post(config.apiKey, baseTimeSeriesUrl(config.project), timeSeriesItems, config.maxRetries)
      .map(item => {
        if (config.collectMetrics) {
          timeSeriesCreated.inc(rows.length)
        }
        item
      })
  }

  def baseTimeSeriesUrl(project: String): Uri =
    uri"${baseUrl(project, "0.5", config.baseUrl)}/timeseries"

  override def schema: StructType = structType[TimeSeriesItem]

  override def toRow(t: TimeSeriesItem): Row = asRow(t)

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.5/projects/${config.project}/timeseries"
}
