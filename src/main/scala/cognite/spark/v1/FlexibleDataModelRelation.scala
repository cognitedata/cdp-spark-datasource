package cognite.spark.v1

import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelRelation

object FlexibleDataModelRelation {
  val ResourceType = "instances"

  final case class ViewCorePropertyConfig(
      viewSpace: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpace: Option[String])
      extends FlexibleDataModelRelation

  final case class ConnectionConfig(edgeSpace: String, edgeExternalId: String)
      extends FlexibleDataModelRelation

  def corePropertyRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      viewCorePropConfig: ViewCorePropertyConfig): FlexibleDataModelCorePropertyRelation =
    new FlexibleDataModelCorePropertyRelation(config, viewCorePropConfig)(sqlContext)

  def connectionRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      connectionConfig: ConnectionConfig): FlexibleDataModelConnectionRelation =
    new FlexibleDataModelConnectionRelation(config, connectionConfig)(sqlContext)
}
