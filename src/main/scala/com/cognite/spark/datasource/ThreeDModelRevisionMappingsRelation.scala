package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionMappingItem(
    nodeId: Long,
    assetId: Long,
    treeIndex: Option[Long],
    subtreeSize: Option[Long])

class ThreeDModelRevisionMappingsRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends CdpRelation[ModelRevisionMappingItem](config, "3dmodelrevisionmappings") {
  override def schema: StructType = structType[ModelRevisionMappingItem]

  override def toRow(t: ModelRevisionMappingItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/3d/models/$modelId/revisions/$revisionId/mappings"
}
