package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, Usage}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ConnectionDefinition
import com.cognite.sdk.scala.v1.fdm.views.{ViewDefinition, ViewReference}
import org.apache.spark.sql.SQLContext

sealed trait FlexibleDataModelRelationFactory

object FlexibleDataModelRelationFactory {
  val ResourceType = "instances"

  final case class ViewCorePropertyConfig(
      intendedUsage: Usage,
      viewReference: Option[ViewReference],
      instanceSpace: Option[String])
      extends FlexibleDataModelRelationFactory

  final case class ConnectionConfig(
      edgeTypeSpace: String,
      edgeTypeExternalId: String,
      instanceSpace: Option[String])
      extends FlexibleDataModelRelationFactory

  sealed trait DataModelConfig extends FlexibleDataModelRelationFactory

  final case class DataModelViewConfig(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
      instanceSpace: Option[String])
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
  ): FlexibleDataModelBaseRelation =
    (dataModelConfig match {
      case vc: DataModelViewConfig => createCorePropertyRelationForDataModel(config, sqlContext, vc)
      case cc: DataModelConnectionConfig => createConnectionRelationForDataModel(config, sqlContext, cc)
    }).unsafeRunSync()

  private def createCorePropertyRelationForDataModel(
      config: RelationConfig,
      sqlContext: SQLContext,
      modelViewConfig: DataModelViewConfig): IO[FlexibleDataModelCorePropertyRelation] = {
    val client = CdpConnector.clientFromConfig(config)
    fetchInlinedDataModel(client, modelViewConfig)
      .map { models =>
        models.flatMap(_.views.getOrElse(Seq.empty)).find {
          case vc: ViewDefinition => vc.externalId == modelViewConfig.viewExternalId
          case vr: ViewReference => vr.externalId == modelViewConfig.viewExternalId
          case _ => false
        }
      }
      .flatMap {
        case Some(vc: ViewDefinition) => IO.pure(Some(vc))
        case Some(vr: ViewReference) => fetchViewWithAllProps(client, vr).map(_.headOption)
        case _ => IO.pure(None)
      }
      .flatMap {
        case Some(vc: ViewDefinition) =>
          IO.delay(
            new FlexibleDataModelCorePropertyRelation(
              config,
              corePropConfig = ViewCorePropertyConfig(
                intendedUsage = vc.usedFor,
                viewReference = Some(vc.toSourceReference),
                instanceSpace = modelViewConfig.instanceSpace
              )
            )(sqlContext)
          )
        case None =>
          IO.raiseError(
            new CdfSparkIllegalArgumentException(s"""
              |Could not find a view with externalId: '${modelViewConfig.viewExternalId}' in the specified data model
              | with (space: '${modelViewConfig.modelSpace}', externalId: '${modelViewConfig.modelExternalId}',
              | version: '${modelViewConfig.modelVersion}')
              |""".stripMargin)
          )
      }
  }

  private def createConnectionRelationForDataModel(
      config: RelationConfig,
      sqlContext: SQLContext,
      modelConnectionConfig: DataModelConnectionConfig): IO[FlexibleDataModelConnectionRelation] = {
    val client = CdpConnector.clientFromConfig(config)
    fetchInlinedDataModel(client, modelConnectionConfig)
      .flatMap {
        _.flatMap(_.views.getOrElse(Seq.empty)).toVector
          .traverse {
            case vc: ViewDefinition =>
              IO.delay(connectionDefinitionExists(vc, modelConnectionConfig.connectionConfig))
            case vr: ViewReference =>
              fetchViewWithAllProps(client, vr).map(_.headOption.exists(
                connectionDefinitionExists(_, modelConnectionConfig.connectionConfig)))
            case _ => IO.pure(false)
          }
          .map(_.contains(true))
      }
      .flatMap {
        case true =>
          IO.delay(
            new FlexibleDataModelConnectionRelation(config, modelConnectionConfig.connectionConfig)(
              sqlContext))
        case false =>
          IO.raiseError(
            new CdfSparkIllegalArgumentException(s"""
              |Could not find a connection definition with
              | (edgeTypeSpace: '${modelConnectionConfig.connectionConfig.edgeTypeSpace}',
              | edgeTypeExternalId: '${modelConnectionConfig.connectionConfig.edgeTypeExternalId}')
              | in any of the views linked with the data model (space: '${modelConnectionConfig.modelSpace}',
              | externalId: '${modelConnectionConfig.modelExternalId}', version: '${modelConnectionConfig.modelVersion}')
              |""".stripMargin)
          )
      }
  }

  private def connectionDefinitionExists(
      viewDef: ViewDefinition,
      connectionConfig: ConnectionConfig): Boolean =
    viewDef.properties.exists {
      case (_, p: ConnectionDefinition) =>
        p.`type`.space == connectionConfig.edgeTypeSpace &&
          p.`type`.externalId == connectionConfig.edgeTypeExternalId
      case _ => false
    }

  private def fetchInlinedDataModel(client: GenericClient[IO], modelViewConfig: DataModelViewConfig) =
    client.dataModelsV3
      .retrieveItems(
        Seq(
          DataModelReference(
            space = modelViewConfig.modelSpace,
            externalId = modelViewConfig.modelExternalId,
            version = Some(modelViewConfig.modelVersion))
        ),
        inlineViews = Some(true)
      )

  private def fetchInlinedDataModel(
      client: GenericClient[IO],
      modelConnectionConfig: DataModelConnectionConfig) =
    client.dataModelsV3
      .retrieveItems(
        Seq(
          DataModelReference(
            space = modelConnectionConfig.modelSpace,
            externalId = modelConnectionConfig.modelExternalId,
            version = Some(modelConnectionConfig.modelVersion))
        ),
        inlineViews = Some(true)
      )

  private def fetchViewWithAllProps(
      client: GenericClient[IO],
      vr: ViewReference): IO[Seq[ViewDefinition]] =
    client.views
      .retrieveItems(
        Seq(
          DataModelReference(
            space = vr.space,
            externalId = vr.externalId,
            version = Some(vr.version)
          )
        ),
        includeInheritedProperties = Some(true)
      )
}
