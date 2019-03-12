package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionItem(
    id: Long,
    fileId: Long,
    published: Boolean,
    rotation: Option[Seq[Double]],
    camera: Map[String, Seq[Double]],
    status: String,
    thumbnailThreedFileId: Option[Long],
    thumbnailUrl: Option[String],
    sceneThreedFiles: Option[Seq[Map[String, Long]]],
    sceneThreedFileId: Option[Long],
    assetMappingCount: Long,
    createdTime: Long)

class ThreeDModelRevisionsRelation(config: RelationConfig, modelId: Long)(
    val sqlContext: SQLContext)
    extends CdpRelation[ModelRevisionItem](config, "3dmodelrevision") {
  override def schema: StructType = structType[ModelRevisionItem]

  override def toRow(t: ModelRevisionItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/3d/models/$modelId/revisions"
}
