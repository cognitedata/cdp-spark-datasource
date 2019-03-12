package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

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

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[FileItem](config, "files") {
  override def schema: StructType = structType[FileItem]

  override def toRow(t: FileItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/files"
}
