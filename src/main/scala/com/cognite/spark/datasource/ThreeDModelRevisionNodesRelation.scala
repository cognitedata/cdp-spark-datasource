package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionNodeItem(
    id: Long,
    treeIndex: Long,
    parentId: Option[Long],
    depth: Long,
    name: String,
    subtreeSize: Long,
    metadata: Map[String, String],
    boundingBox: Option[Map[String, Seq[Double]]],
    sectorId: Option[Long])

class ThreeDModelRevisionNodesRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends CdpRelation[ModelRevisionNodeItem](config, "3dmodelrevisionnodes") {
  override def schema: StructType = structType[ModelRevisionNodeItem]

  override def toRow(t: ModelRevisionNodeItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/3d/models/$modelId/revisions/$revisionId/nodes"
}
