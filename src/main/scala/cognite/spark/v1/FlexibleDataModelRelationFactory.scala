package cognite.spark.v1

import com.cognite.sdk.scala.v1.fdm.instances.InstanceType
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelRelationFactory

object FlexibleDataModelRelationFactory {
  val ResourceType = "instances"

  final case class ViewCorePropertyConfig(
      instanceType: InstanceType,
      viewReference: Option[ViewReference],
      instanceSpace: Option[String])
      extends FlexibleDataModelRelationFactory

  final case class ConnectionConfig(edgeTypeSpace: String, edgeTypeExternalId: String)
      extends FlexibleDataModelRelationFactory

  sealed trait DataModelConfig extends FlexibleDataModelRelationFactory

  final case class DataModelViewConfig(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String)
      extends DataModelConfig

  final case class DataModelConnectionConfig(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      connectionConfig: ConnectionConfig)
      extends DataModelConfig

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

  def dataModelRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      dataModelConfig: DataModelConfig
  ): FlexibleDataModelRelation = new FlexibleDataModelRelation(config, dataModelConfig)(sqlContext)
}
