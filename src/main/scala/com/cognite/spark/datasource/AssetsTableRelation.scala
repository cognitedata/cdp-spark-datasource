package com.cognite.spark.datasource

import cats.Parallel
import cats.effect.{ConcurrentEffect, IO}
import cats.effect._
import cats.implicits._
import com.cognite.spark.datasource.Tap._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.generic.auto._
import okhttp3._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.concurrent.ExecutionContext

case class PostAssetsDataItems[A](items: Seq[A])

case class AssetsItem(name: String,
                      parentId: Option[Long],
                      description: Option[String],
                      metadata: Option[Map[String, String]],
                      id: Long)

case class PostAssetsItem(name: String,
                          description: String,
                          metadata: Map[String, String])

// TODO: there's obviously a lot of copy-paste going on here at the moment,
// and this should be refactored once we've identified some common patterns.
class AssetsTableRelation(apiKey: String,
                           project: String,
                           assetPath: Option[String],
                           limit: Option[Int],
                           batchSizeOption: Option[Int],
                           metricsPrefix: String,
                           collectMetrics: Boolean)
                         (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {
  // TODO: make read/write timeouts configurable
  @transient lazy val batchSize = batchSizeOption.getOrElse(10000)

  lazy private val assetsCreated = UserMetricsSystem.counter(s"${metricsPrefix}assets.created")
  lazy private val assetsRead = UserMetricsSystem.counter(s"${metricsPrefix}assets.read")

  override def schema: StructType = StructType(Seq(
    StructField("name", DataTypes.StringType),
    StructField("parentId", DataTypes.LongType),
    StructField("description", DataTypes.StringType),
    StructField("metadata", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
    StructField("id", DataTypes.LongType)))

  override def buildScan(): RDD[Row] = {
    val urlBuilder = AssetsTableRelation.baseAssetsURL(project)
    assetPath.foreach(path => urlBuilder.addQueryParameter("path", path))
    val result = CdpConnector.get[AssetsItem](apiKey, urlBuilder.build(), batchSize, limit)
      .map(item => {
        if (collectMetrics) {
          assetsRead.inc()
        }
        Row(item.name, item.parentId, item.description, item.metadata, item.id)
      })
      .toStream

    sqlContext.sparkContext.parallelize(result)
  }

  private implicit val contextShift = IO.contextShift(ExecutionContext.global)
  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })
  }

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val assetItems = rows.map(r =>
      PostAssetsItem(r.getString(0), r.getString(2), r.getAs[Map[String, String]](3)))
    CdpConnector.post(apiKey, AssetsTableRelation.baseAssetsURL(project).build(), assetItems)
      .map(item => {
        if (collectMetrics) {
          assetsCreated.inc(rows.length)
        }
        item
      })
  }
}

object AssetsTableRelation {
  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    mapper
  }

  def baseAssetsURL(project: String): HttpUrl.Builder = {
    CdpConnector.baseUrl(project, "0.5")
      .addPathSegment("assets")
  }

  val validPathComponentTypes = Seq(
    classOf[java.lang.Integer],
    classOf[java.lang.Long],
    classOf[java.lang.String])

  def isValidAssetsPath(path: String): Boolean = {
    try {
      val pathComponents = mapper.readValue(path, classOf[Seq[Any]])
      pathComponents.forall(c => validPathComponentTypes.contains(c.getClass))
    } catch {
      case _: JsonParseException => false
    }
  }
}
