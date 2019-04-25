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
import org.apache.spark.datasource.MetricsSource

import scala.concurrent.ExecutionContext

case class PostAssetsDataItems[A](items: Seq[A])

case class AssetsItem(
    id: Long,
    path: Option[Seq[Long]],
    depth: Option[Long],
    name: String,
    parentId: Option[Long],
    description: Option[String],
    metadata: Option[Map[String, String]],
    source: Option[String],
    sourceId: Option[String],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

case class PostAssetsItem(
    name: String,
    parentId: Option[Long],
    description: Option[String],
    source: Option[String],
    sourceId: Option[String],
    metadata: Option[Map[String, String]]
)

// This class is needed to enable partial updates
// since AssetsItem has values that are not optional
case class UpdateAssetsItemBase(
    id: Option[Long],
    name: Option[String],
    description: Option[String],
    metadata: Option[Map[String, String]],
    source: Option[String],
    sourceId: Option[String]
)

case class UpdateAssetsItem(
    id: Option[Long],
    name: Option[NonNullableSetter[String]],
    description: Option[Setter[String]],
    metadata: Option[Setter[Map[String, String]]],
    source: Option[Setter[String]],
    sourceId: Option[Setter[String]]
)
object UpdateAssetsItem {
  def apply(assetItem: UpdateAssetsItemBase): UpdateAssetsItem =
    new UpdateAssetsItem(
      assetItem.id,
      assetItem.name.map(NonNullableSetter(_)),
      Setter[String](assetItem.description),
      Setter[Map[String, String]](assetItem.metadata),
      Setter[String](assetItem.source),
      Setter[String](assetItem.sourceId)
    )
}

class AssetsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[AssetsItem](config, "assets")
    with InsertableRelation
    with CdpConnector {
  @transient lazy private val assetsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"assets.created")

  override def update(rows: Seq[Row]): IO[Unit] = {
    val updateAssetsItem = rows.map(r => UpdateAssetsItem(fromRow[UpdateAssetsItemBase](r)))

    post(
      config.auth,
      uri"${baseAssetsURL(config.project)}/update",
      updateAssetsItem,
      config.maxRetries)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup.parTraverse(postRows).unsafeRunSync()
      }
      ()
    })

  private def postRows(rows: Seq[Row]): IO[Unit] = {
    val assetItems = rows.map { r =>
      val assetItem = fromRow[PostAssetsItem](r)
      assetItem.copy(metadata = filterMetadata(assetItem.metadata))
    }
    post(config.auth, baseAssetsURL(config.project), assetItems, config.maxRetries)
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

  // TODO: As of 2019-03-22 the normal /assets endpoint doesn't work on some projects when
  // using cursors retrieved from /assets/cursors, but the cursors do work with /assets/list.
  // This is ok as long as we don't support predicate pushdown for assets, as we don't need
  // the search capabilities of /assets, but this should be removed once /assets works as expected.
  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/assets/list"

  private val cursorsUrl = uri"${config.baseUrl}/api/0.6/projects/${config.project}/assets/cursors"
  override def cursors(): Iterator[(Option[String], Option[Int])] =
    CursorsCursorIterator(cursorsUrl.param("divisions", config.partitions.toString), config)
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
