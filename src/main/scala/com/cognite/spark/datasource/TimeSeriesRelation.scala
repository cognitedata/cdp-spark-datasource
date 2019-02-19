package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import SparkSchemaHelper._

import scala.concurrent.ExecutionContext

case class PostTimeSeriesDataItems[A](items: Seq[A])

case class TimeSeriesItem(name: String,
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

case class PostTimeSeriesItem(name: String,
                              isString: Boolean,
                              metadata: Option[Map[String, String]],
                              unit: Option[String],
                              assetId: Option[Long],
                              isStep: Boolean,
                              description: Option[String],
                              securityCategories: Option[Vector[Long]])

class TimeSeriesRelation(config: RelationConfig)
                        (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with CdpConnector
    with Serializable {

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val timeSeriesCreated = UserMetricsSystem.counter(s"${config.metricsPrefix}timeseries.created")
  @transient lazy private val timeSeriesRead = UserMetricsSystem.counter(s"${config.metricsPrefix}timeseries.read")

  override def schema: StructType = structType[TimeSeriesItem]

  override def buildScan(): RDD[Row] = {
    val getUrl = baseTimeSeriesUrl(config.project)

    CdpRdd[TimeSeriesItem](sqlContext.sparkContext,
      (ts: TimeSeriesItem) => {
        if (config.collectMetrics) {
          timeSeriesRead.inc()
        }
        asRow(ts)
      },
      getUrl, getUrl, config.apiKey, config.project, batchSize, maxRetries, config.limit)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })
  }

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map(r => fromRow[PostTimeSeriesItem](r))
    post(config.apiKey, baseTimeSeriesUrl(config.project), timeSeriesItems, maxRetries)
      .map(item => {
        if (config.collectMetrics) {
          timeSeriesCreated.inc(rows.length)
        }
        item
      })
  }

  def baseTimeSeriesUrl(project: String): Uri = {
    uri"${baseUrl(project)}/timeseries"
  }
}
