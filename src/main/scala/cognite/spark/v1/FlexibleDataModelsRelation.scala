package cognite.spark.v1

import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelsRelation

object FlexibleDataModelsRelation {
  val ResourceType = "instances"

  final case class NodeOrEdgeRelation(
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: Option[String])
      extends FlexibleDataModelsRelation

  final case class ConnectionRelation(edgeSpaceExternalId: String, edgeExternalId: String)
      extends FlexibleDataModelsRelation

  def nodeOrEdgeRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      nodeOrEdgeRelation: NodeOrEdgeRelation): FlexibleDataModelsNodeOrEdgeRelation =
    new FlexibleDataModelsNodeOrEdgeRelation(config, nodeOrEdgeRelation)(sqlContext)

  def connectionRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      connectionRelation: ConnectionRelation): FlexibleDataModelsConnectionRelation =
    new FlexibleDataModelsConnectionRelation(config, connectionRelation)(sqlContext)
}
