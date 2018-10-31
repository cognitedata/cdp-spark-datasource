package com.cognite.spark.connector

import com.cognite.spark.connector.CdpConnector.DataItemsWithCursor
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import io.circe.generic.auto._
import org.apache.spark.groupon.metrics.UserMetricsSystem

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
    val result = CdpConnector.get[AssetsItem](apiKey, urlBuilder.build(), batchSize, limit,
      batchCompletedCallback = if (collectMetrics) {
        Some((items: DataItemsWithCursor[_]) => assetsRead.inc(items.data.items.length))
      } else {
        None
      })
      .map(item => Row(item.name, item.parentId, item.description, item.metadata, item.id))
      .toList

    sqlContext.sparkContext.parallelize(result)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      rows.grouped(batchSize).foreach(postRows)
    })
  }

  private def postRows(rows: Seq[Row]) = {
    val assetItems = rows.map(r =>
      PostAssetsItem(r.getString(0), r.getString(2), r.getAs[Map[String, String]](3)))
    CdpConnector.post(apiKey, AssetsTableRelation.baseAssetsURL(project).build(), assetItems, true,
      // have to specify maxRetries and retryInterval to make circe happy, for some reason
      5, 0.5, if (collectMetrics) {
        Some(_ => assetsCreated.inc(rows.length))
      } else {
        None
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
