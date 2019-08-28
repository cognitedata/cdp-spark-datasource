package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.{File, GenericClient}
import com.cognite.sdk.scala.v1.resources.Files
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import cats.implicits._

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
    extends SdkV1Relation[File, Files[IO], FileItem](config, "files")
    with InsertableRelation {

  override def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] = {
    val files = rows.map { r =>
      val file = fromRow[File](r)
      file.copy(metadata = filterMetadata(file.metadata))
    }
    client.files.updateFromRead(files) *> IO.unit
  }

  override def clientToResource(client: GenericClient[IO, Nothing]): Files[IO] =
    client.files

  override def schema: StructType = structType[File]

  override def toRow(t: File): Row = asRow(t)

  override def listUrl(version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/${config.project}/files"
}
