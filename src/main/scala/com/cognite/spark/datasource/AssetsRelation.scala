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
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.spark.datasource.MetricsSource

import scala.concurrent.ExecutionContext

case class Type(id: Long, name: String, fields: Seq[FieldData])
case class TypeDescription(
    id: Long,
    name: String,
    description: String,
    fields: Seq[FieldDescription]
)
case class PostType(id: Long, fields: Seq[PostField])
case class FieldData(id: Long, name: String, valueType: String, value: String)
case class FieldDescription(id: Long, name: String, description: String, valueType: String)
case class PostField(id: Long, value: String)
case class DoubleIsTooLargeForJSON(field: Long)
    extends Throwable(s"Double is too large for JSON in data type field with id $field.")

case class AssetsItem(
    id: Long,
    path: Option[Seq[Long]],
    depth: Option[Long],
    name: String,
    parentId: Option[Long],
    description: Option[String],
    types: Option[Seq[Type]],
    metadata: Option[Map[String, String]],
    source: Option[String],
    sourceId: Option[String],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long]
)

case class PostAssetsItem(
    name: String,
    parentId: Option[Long],
    description: Option[String],
    types: Option[Seq[PostType]],
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

import AssetsRelation.fieldDecoder

class AssetsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[AssetsItem](config, "assets")
    with InsertableRelation {
  @transient lazy private val assetsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"assets.created")

  private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

  override def update(rows: Seq[Row]): IO[Unit] = {
    val updateAssetsItems = rows
      .map(r => fromRow[UpdateAssetsItemBase](r))
      .map(a => UpdateAssetsItem(a))

    // Assets must have an id when using update
    if (updateAssetsItems.exists(_.id.isEmpty)) {
      throw new IllegalArgumentException("Assets must have an id when using update")
    }

    post(config, uri"${baseAssetsURL(config.project)}/update", updateAssetsItems)
  }
  //TODO: Add description as a pushdown filter once we know how spaces are handled
  override val fieldsWithPushdownFilter: Seq[String] = Seq("name", "source", "depth")

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val postAssetItems = rows.map(r => fromRow[PostAssetsItem](r))
    post(config, baseAssetsURL(config.project), postAssetItems)
  }

  override def delete(rows: Seq[Row]): IO[Unit] =
    deleteItems(config, baseAssetsURL(config.project), rows)

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
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
    post(
      config,
      baseAssetsURL(config.project),
      assetItems
    ).map(item => {
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

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/assets"

  private val cursorsUrl = uri"${config.baseUrl}/api/0.6/projects/${config.project}/assets/cursors"
  override def cursors(): Iterator[(Option[String], Option[Int])] =
    CursorsCursorIterator(
      cursorsUrl.param("divisions", config.partitions.toString),
      config
    )

  implicit val postFieldEncoder: Encoder[PostField] = new Encoder[PostField] {
    override def apply(field: PostField): Json = {
      val valueType = fieldIdToValueTypeMap.getOrElse(
        field.id,
        throw new NoSuchElementException(s"${field.id} is not a valid asset type field id")
      )
      val value: Json = valueType match {
        case "String" => Json.fromString(field.value)
        case "Long" => Json.fromLong(field.value.replace('.', ',').toLong)
        case "Double" =>
          Json
            .fromDouble(field.value.toDouble)
            .getOrElse(throw DoubleIsTooLargeForJSON(field.id))
        case "Boolean" => Json.fromBoolean(field.value.toBoolean)
        case _ =>
          throw new IllegalArgumentException(s"$valueType is not a supported type.")
      }

      Json.obj(
        ("id", Json.fromLong(field.id)),
        ("value", value)
      )
    }
  }

  private val fieldIdToValueTypeMap: Map[Long, String] = {
    val assetTypesIterator =
      get[TypeDescription](
        config,
        uri"${baseAssetsURL(config.project)}/types?"
      )
    assetTypesIterator.flatMap(_.fields).map(f => (f.id, f.valueType)).toMap
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

  implicit val fieldDecoder: Decoder[FieldData] = new Decoder[FieldData] {
    override def apply(c: HCursor): Result[FieldData] =
      for {
        id <- c.downField("id").as[Long]
        name <- c.downField("name").as[String]
        valueType <- c.downField("valueType").as[String]
        value <- valueType match {
          case "String" => c.downField("value").as[String]
          case "Long" => c.downField("value").as[Long].map(_.toString)
          case "Double" => c.downField("value").as[Double].map(_.toString)
          case "Boolean" => c.downField("value").as[Boolean].map(_.toString)
          case _ => throw new IllegalArgumentException(s"$valueType is not a supported type.")
        }
      } yield FieldData(id, name, valueType, value)
  }

  private val validPathComponentTypes =
    Seq(classOf[java.lang.Integer], classOf[java.lang.Long], classOf[java.lang.String])

  def isValidAssetsPath(path: String): Boolean =
    try {
      val pathComponents = mapper.readValue(path, classOf[Seq[Any]])
      pathComponents.forall(c => validPathComponentTypes.contains(c.getClass))
    } catch {
      case _: JsonParseException => false
    }
}
