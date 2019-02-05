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

import scala.concurrent.ExecutionContext

case class PostTimeSeriesDataItems[A](items: Seq[A])

case class TimeSeriesItem(name: String,
                          isString: Boolean,
                          metadata: Option[Map[String, String]],
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

class TimeSeriesRelation(apiKey: String,
                         project: String,
                         limit: Option[Int],
                         batchSizeOption: Option[Int],
                         metricsPrefix: String,
                         collectMetrics: Boolean)
                        (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with CdpConnector
    with Serializable {

  @transient lazy val batchSize = batchSizeOption.getOrElse(Constants.DefaultBatchSize)

  @transient lazy val timeSeriesCreated = UserMetricsSystem.counter(s"${metricsPrefix}timeseries.created")
  @transient lazy val timeSeriesRead = UserMetricsSystem.counter(s"${metricsPrefix}timeseries.read")

  override def schema: StructType = {
    StructType(Seq(
      StructField("name", StringType, false),
      StructField("isString", BooleanType, false),
      StructField("metadata", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true),
      StructField("unit", StringType, true),
      StructField("assetId", LongType, true),
      StructField("isStep", BooleanType, false),
      StructField("description", StringType, true),
      StructField("securityCategories", DataTypes.createArrayType(LongType), true),
      StructField("id", LongType, false),
      StructField("createdTime", LongType, false),
      StructField("lastUpdatedTime", LongType, false)))
  }

  override def buildScan(): RDD[Row] = {
    val getUrl = baseTimeSeriesUrl(project)

    CdpRdd[TimeSeriesItem](sqlContext.sparkContext,
      (ts: TimeSeriesItem) => {
        if (collectMetrics) {
          timeSeriesRead.inc()
        }
        Row(ts.name, ts.isString, ts.metadata, ts.unit, ts.assetId, ts.isStep, ts.description,
          ts.securityCategories, ts.id, ts.createdTime, ts.lastUpdatedTime)
      },
      getUrl, getUrl, apiKey, project, batchSize, limit)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })
  }

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map(r =>
      PostTimeSeriesItem(
        r.getString(0),
        r.getBoolean(1),
        Option(r.getAs(2)),
        Option(r.getAs(3)),
        Option(r.getAs(4)),
        r.getBoolean(5),
        Option(r.getAs(6)),
        Option(r.getAs(7))))
    post(apiKey, baseTimeSeriesUrl(project), timeSeriesItems)
      .map(item => {
        if (collectMetrics) {
          timeSeriesCreated.inc(rows.length)
        }
        item
      })
  }

  def baseTimeSeriesUrl(project: String): Uri = {
    uri"${baseUrl(project)}/timeseries"
  }
}
