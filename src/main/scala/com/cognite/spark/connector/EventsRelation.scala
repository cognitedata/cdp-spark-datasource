package com.cognite.spark.connector

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.cognite.spark.connector.CdpConnector.DataItemsWithCursor
import io.circe.generic.auto._
import okhttp3.HttpUrl
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class EventItem(id: Option[Long],
                      startTime: Option[Long],
                      endTime: Option[Long],
                      description: Option[String],
                      `type`: Option[String],
                      subtype: Option[String],
                      metadata: Option[Map[String, String]],
                      assetIds: Option[Seq[Long]],
                      source: Option[String],
                      sourceId: Option[String])

class EventsRelation(apiKey: String,
                      project: String,
                      limit: Option[Int],
                      batchSizeOption: Option[Int],
                      metricsPrefix: String,
                      collectMetrics: Boolean)
                    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {

  @transient lazy val batchSize: Int = batchSizeOption.getOrElse(10000)

  lazy private val eventsCreated = UserMetricsSystem.counter(s"${metricsPrefix}events.created")
  lazy private val eventsRead = UserMetricsSystem.counter(s"${metricsPrefix}events.read")

  override def schema: StructType = {
    StructType(Seq(
      StructField("id", LongType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("description", StringType),
      StructField("type", StringType),
      StructField("subtype", StringType),
      StructField("metadata", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("assetIds", ArrayType(LongType)),
      StructField("source", StringType),
      StructField("sourceId", StringType)))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.foreachPartition(rows => {
      val batches = rows.grouped(batchSize).toSeq
      val remainingRequests = new CountDownLatch(batches.length)
      batches.foreach(postEvent(_, remainingRequests))
      remainingRequests.await(5, TimeUnit.MINUTES)
    })
  }

  override def buildScan(): RDD[Row] = {
    val finalRows = CdpConnector.get[EventItem](apiKey, EventsRelation.baseEventsURL(project).build(), batchSize, limit,
      batchCompletedCallback = if (collectMetrics) {
        Some((items: DataItemsWithCursor[_]) => eventsRead.inc(items.data.items.length))
      } else {
        None
      })
      .map(item => Row(item.id, item.startTime, item.endTime, item.description,
        item.`type`, item.subtype, item.metadata, item.assetIds, item.source, item.sourceId))
      .toList
    sqlContext.sparkContext.parallelize(finalRows)
  }

  def postEvent(rows: Seq[Row], remainingRequests: CountDownLatch): Unit = {
    val eventItems = rows.map(r =>
      EventItem(Option(r.getAs(0)), Option(r.getAs(1)), Option(r.getAs(2)), Option(r.getString(3)),
        Option(r.getAs(4)), Option(r.getAs(5)), Option(r.getAs(6)),
        Option(r.getAs(7)), Option(r.getAs(8)), Option(r.getAs(9)))
    )
    CdpConnector.post(apiKey, EventsRelation.baseEventsURL(project).build(), eventItems, true,
      // have to specify maxRetries and retryInterval to make circe happy, for some reason
      5, 0.5,
      Some(_ => {
        if (collectMetrics) {
          eventsCreated.inc(rows.length)
        }
        remainingRequests.countDown()
      }),
      Some(_ => remainingRequests.countDown())
    )
  }
}

object EventsRelation {
  def baseEventsURL(project: String): HttpUrl.Builder = {
    CdpConnector.baseUrl(project, "0.5")
      .addPathSegment("events")
  }
}
