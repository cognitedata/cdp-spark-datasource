package cognite.spark.v1

import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelRelation

object FlexibleDataModelRelation {
  val ResourceType = "instances"

  final case class ViewConfig(
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: Option[String])
      extends FlexibleDataModelRelation

  final case class ConnectionConfig(edgeSpaceExternalId: String, edgeExternalId: String)
      extends FlexibleDataModelRelation

  def nodeOrEdge(
      config: RelationConfig,
      sqlContext: SQLContext,
      viewConfig: ViewConfig): FlexibleDataModelNodeOrEdgeRelation =
    new FlexibleDataModelNodeOrEdgeRelation(config, viewConfig)(sqlContext)

  def connection(
      config: RelationConfig,
      sqlContext: SQLContext,
      connectionConfig: ConnectionConfig): FlexibleDataModelConnectionRelation =
    new FlexibleDataModelConnectionRelation(config, connectionConfig)(sqlContext)
}
