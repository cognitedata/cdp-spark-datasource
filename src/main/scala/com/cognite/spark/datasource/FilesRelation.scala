package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.circe.generic.auto._

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.concurrent.ExecutionContext

case class FileItem(
    id: Option[Long],
    fileName: String,
    directory: Option[String],
    source: Option[String],
    sourceId: Option[String],
    fileType: Option[String],
    metadata: Option[Map[String, String]],
    assetIds: Option[Seq[Long]],
    uploaded: Option[Boolean],
    uploadedAt: Option[Long],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

case class UpdateFileItem(
    id: Option[Long],
    directory: Option[Setter[String]],
    source: Option[Setter[String]],
    sourceId: Option[Setter[String]],
    fileType: Option[Setter[String]],
    metadata: Option[Setter[Map[String, String]]],
    assetIds: Option[Map[String, Seq[Long]]]
)
object UpdateFileItem {
  def apply(fileItem: FileItem): UpdateFileItem =
    new UpdateFileItem(
      fileItem.id,
      Setter[String](fileItem.directory),
      Setter[String](fileItem.source),
      Setter[String](fileItem.sourceId),
      Setter[String](fileItem.fileType),
      Setter[Map[String, String]](fileItem.metadata),
      fileItem.assetIds.map(a => Map("set" -> a))
    )
}

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[FileItem](config, "files")
    with CdpConnector
    with InsertableRelation {

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.parTraverse(postFiles).unsafeRunSync()
      ()
    })

  def postFiles(rows: Seq[Row]): IO[Unit] = {
    val fileItems = rows.map { r =>
      val fileItem = fromRow[FileItem](r)
      fileItem.copy(metadata = filterMetadata(fileItem.metadata))
    }

    val updateFileItems = fileItems.map(f => UpdateFileItem(f))

    post(
      config.apiKey,
      uri"$listUrl/update",
      updateFileItems,
      config.maxRetries
    )
  }

  override def schema: StructType = structType[FileItem]

  override def toRow(t: FileItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/files"
}
