package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ThreeDModelRevisionSectorsItem(
    id: Int,
    parentId: Option[Int],
    path: String,
    depth: Int,
    boundingBox: Map[String, Seq[Double]],
    threedFileId: Long,
    threedFiles: Seq[Map[String, Long]])

class ThreeDModelRevisionSectorsRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends CdpRelation[ThreeDModelRevisionSectorsItem](config, "3dmodelrevisionsectors") {
  override def schema: StructType = structType[ThreeDModelRevisionSectorsItem]

  override def toRow(t: ThreeDModelRevisionSectorsItem): Row = asRow(t)

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/3d/models/$modelId/revisions/$revisionId/sectors"
}
