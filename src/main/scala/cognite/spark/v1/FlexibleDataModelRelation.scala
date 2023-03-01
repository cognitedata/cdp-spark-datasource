package cognite.spark.v1

import com.cognite.sdk.scala.v1.fdm.instances.InstanceType
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelRelation

object FlexibleDataModelRelation {
  val ResourceType = "instances"

  final case class ViewCorePropertyConfig(
      instanceType: InstanceType,
      viewReference: Option[ViewReference],
      instanceSpace: Option[String])
      extends FlexibleDataModelRelation

  final case class ConnectionConfig(edgeTypeSpace: String, edgeTypeExternalId: String)
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
