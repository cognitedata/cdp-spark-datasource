package com.cognite.spark.datasource

import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructType}
import SparkSchemaHelper._

case class FileItem(
    id: Option[Long],
    fileName: String,
    directory: Option[String],
    source: Option[String],
    sourceId: Option[String],
    fileType: Option[String],
    metadata: Option[Map[String, Option[String]]],
    assetIds: Option[Seq[Long]],
    uploaded: Option[Boolean],
    uploadedAt: Option[Long],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[FileItem](config, "files") {
  override def schema: StructType = structType[FileItem]

  override def toRow(t: FileItem): Row = asRow(t)

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/files"
}
