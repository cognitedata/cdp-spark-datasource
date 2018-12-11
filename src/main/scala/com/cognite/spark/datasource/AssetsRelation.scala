package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.softwaremill.sttp._
import io.circe.generic.auto._
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

class AssetsRelation(apiKey: String,
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
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = batchSizeOption.getOrElse(10000)

  @transient lazy val assetsCreated = UserMetricsSystem.counter(s"${metricsPrefix}assets.created")
  @transient lazy val assetsRead = UserMetricsSystem.counter(s"${metricsPrefix}assets.read")

  override def schema: StructType = StructType(Seq(
    StructField("name", DataTypes.StringType),
    StructField("parentId", DataTypes.LongType),
    StructField("description", DataTypes.StringType),
    StructField("metadata", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
    StructField("id", DataTypes.LongType)))

  override def buildScan(): RDD[Row] = {
    val url = baseAssetsURL(project)
    val getUrl = assetPath.fold(url)(url.param("path", _))

    CdpRdd[AssetsItem](sqlContext.sparkContext,
      (a: AssetsItem) => {
        if (collectMetrics) {
          assetsRead.inc()
        }
        Row(a.name, a.parentId, a.description, a.metadata, a.id)
      },
      getUrl, getUrl, apiKey, project, batchSize, limit)
  }

  @transient private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })
  }

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val assetItems = rows.map(r =>
      PostAssetsItem(r.getString(0), r.getString(2), r.getAs[Map[String, String]](3)))
    post(apiKey, baseAssetsURL(project), assetItems)
      .map(item => {
        if (collectMetrics) {
          assetsCreated.inc(rows.length)
        }
        item
      })
  }

  def baseAssetsURL(project: String, version: String = "0.5"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/assets"
  }
}

object AssetsRelation {
  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    mapper
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
