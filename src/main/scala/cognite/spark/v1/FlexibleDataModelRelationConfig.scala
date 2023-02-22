package cognite.spark.v1

import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelRelationConfig

object FlexibleDataModelRelationConfig {
  val ResourceType = "instances"

  final case class NodeOrEdgeRelationConfig(
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: Option[String])
      extends FlexibleDataModelRelationConfig

  final case class ConnectionRelationConfig(edgeSpaceExternalId: String, edgeExternalId: String)
      extends FlexibleDataModelRelationConfig

  def nodeOrEdgeRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      nodeOrEdgeRelation: NodeOrEdgeRelationConfig): FlexibleDataModelBaseRelation =
    new FlexibleDataModelNodeOrEdgeRelation(config, nodeOrEdgeRelation)(sqlContext)

  def connectionRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      connectionRelation: ConnectionRelationConfig): FlexibleDataModelBaseRelation =
    new FlexibleDataModelConnectionRelation(config, connectionRelation)(sqlContext)
}
