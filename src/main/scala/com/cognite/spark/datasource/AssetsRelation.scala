package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import SparkSchemaHelper._
import org.apache.spark.datasource.MetricsSource

import scala.concurrent.ExecutionContext

case class PostAssetsDataItems[A](items: Seq[A])

case class AssetsItem(
    name: String,
    parentId: Option[Long],
    description: Option[String],
    metadata: Option[Map[String, Option[String]]],
    id: Long)

case class PostAssetsItem(name: String, description: String, metadata: Map[String, String])

class AssetsRelation(config: RelationConfig, assetPath: Option[String])(val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val metricsSource = new MetricsSource(config.metricsPrefix)
  @transient lazy private val assetsCreated = metricsSource.getOrCreateCounter(s"assets.created")
  @transient lazy private val assetsRead = metricsSource.getOrCreateCounter(s"assets.read")

  override def schema: StructType = structType[AssetsItem]

  override def buildScan(): RDD[Row] = {
    val url = baseAssetsURL(config.project)
    val getUrl = assetPath.fold(url)(url.param("path", _))

    CdpRdd[AssetsItem](
      sqlContext.sparkContext,
      (a: AssetsItem) => {
        if (config.collectMetrics) {
          assetsRead.inc()
        }
        asRow(a)
      },
      getUrl.param("onlyCursors", "true"),
      getUrl,
      config.apiKey,
      config.project,
      batchSize,
      maxRetries,
      config.limit
    )
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val assetItems = rows.map(r => fromRow[PostAssetsItem](r))
    post(config.apiKey, baseAssetsURL(config.project), assetItems, maxRetries)
      .map(item => {
        if (config.collectMetrics) {
          assetsCreated.inc(rows.length)
        }
        item
      })
  }

  def baseAssetsURL(project: String, version: String = "0.6"): Uri =
    uri"https://api.cognitedata.com/api/$version/projects/$project/assets"
}

object AssetsRelation {
  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    mapper
  }

  val validPathComponentTypes =
    Seq(classOf[java.lang.Integer], classOf[java.lang.Long], classOf[java.lang.String])

  def isValidAssetsPath(path: String): Boolean =
    try {
      val pathComponents = mapper.readValue(path, classOf[Seq[Any]])
      pathComponents.forall(c => validPathComponentTypes.contains(c.getClass))
    } catch {
      case _: JsonParseException => false
    }
}
