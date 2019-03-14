package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import com.cognite.spark.datasource.SparkSchemaHelper._

import scala.concurrent.ExecutionContext

case class PostAssetsDataItems[A](items: Seq[A])

case class AssetsItem(
    id: Long,
    path: Option[Seq[Long]],
    depth: Option[Long],
    name: Option[String],
    parentId: Option[Long],
    description: Option[String],
    metadata: Option[Map[String, Option[String]]],
    source: Option[String],
    sourceId: Option[String],
    createdTime: Long,
    lastUpdatedTime: Long)

case class PostAssetsItem(
    name: String,
    refId: Option[String],
    parentName: Option[String],
    parentRefId: Option[String],
    parentId: Option[Long],
    description: Option[String],
    source: Option[String],
    sourceId: Option[String],
    metadata: Option[Map[String, Option[String]]],
    createdTime: Long,
    lastUpdatedTime: Long
)

class AssetsRelation(config: RelationConfig, assetPath: Option[String])(val sqlContext: SQLContext)
    extends CdpRelation[AssetsItem](config, "assets")
    with InsertableRelation
    with CdpConnector {
  @transient lazy private val assetsCreated = metricsSource.getOrCreateCounter(s"assets.created")

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize).toVector
      batches.parTraverse(postRows).unsafeRunSync()
    })

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val assetItems = rows.map(r => fromRow[PostAssetsItem](r))
    post(config.apiKey, baseAssetsURL(config.project), assetItems, config.maxRetries)
      .map(item => {
        if (config.collectMetrics) {
          assetsCreated.inc(rows.length)
        }
        item
      })
  }

  def baseAssetsURL(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/assets"

  override def schema: StructType = structType[AssetsItem]

  override def toRow(t: AssetsItem): Row = asRow(t)

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/assets"

  override def cursorsUrl(relationConfig: RelationConfig): Uri =
    listUrl(relationConfig).param("onlyCursors", "true")
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
