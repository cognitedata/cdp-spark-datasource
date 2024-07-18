package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, Usage}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ConnectionDefinition,
  EdgeConnection
}
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModelViewReference
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
      viewExternalId: String,
      connectionPropertyName: String,
      instanceSpace: Option[String])
      extends DataModelConfig

  def corePropertyRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      viewCorePropConfig: ViewCorePropertyConfig): FlexibleDataModelCorePropertyRelation =
    new FlexibleDataModelCorePropertyRelation(config, viewCorePropConfig)(sqlContext)

  def corePropertySyncRelation(
      cursor: String,
      config: RelationConfig,
      sqlContext: SQLContext,
      cursorName: Option[String],
      jobId: Option[String],
      syncCursorSaveCallbackUrl: Option[String],
      viewCorePropConfig: ViewCorePropertyConfig): FlexibleDataModelCorePropertySyncRelation =
    new FlexibleDataModelCorePropertySyncRelation(
      cursor,
      config,
      cursorName,
      jobId,
      syncCursorSaveCallbackUrl,
      viewCorePropConfig)(sqlContext)

  def connectionRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      connectionConfig: ConnectionConfig): FlexibleDataModelConnectionRelation =
    new FlexibleDataModelConnectionRelation(config, connectionConfig)(sqlContext)

  def dataModelRelationSync(
      cursor: String,
      cursorName: Option[String],
      jobId: Option[String],
      syncCursorSaveCallbackUrl: Option[String],
      config: RelationConfig,
      sqlContext: SQLContext,
      dataModelConfig: DataModelConfig): FlexibleDataModelBaseRelation = {
    val viewCorePropertyConfig = (dataModelConfig match {
      case vc: DataModelViewConfig => resolveViewCorePropertyConfig(config, vc).unsafeRunSync()
      case cc: DataModelConnectionConfig => ViewCorePropertyConfig(Usage.Edge, None, cc.instanceSpace)
    })
    new FlexibleDataModelCorePropertySyncRelation(
      cursor,
      config,
      cursorName,
      jobId,
      syncCursorSaveCallbackUrl,
      viewCorePropertyConfig
    )(sqlContext)
  }

  def dataModelRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      dataModelConfig: DataModelConfig
  ): FlexibleDataModelBaseRelation =
    (dataModelConfig match {
      case vc: DataModelViewConfig => createCorePropertyRelationForDataModel(config, sqlContext, vc)
      case cc: DataModelConnectionConfig => createConnectionRelationForDataModel(config, sqlContext, cc)
    }).unsafeRunSync()

  private def resolveViewCorePropertyConfig(
      config: RelationConfig,
      modelViewConfig: DataModelViewConfig): IO[ViewCorePropertyConfig] = {
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
        case Some(vc: DataModelViewReference) =>
          IO.delay(
            ViewCorePropertyConfig(
              intendedUsage = vc.usedFor,
              viewReference = Some(vc.toSourceReference),
              instanceSpace = modelViewConfig.instanceSpace
            )
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

  private def createCorePropertyRelationForDataModel(
      config: RelationConfig,
      sqlContext: SQLContext,
      modelViewConfig: DataModelViewConfig): IO[FlexibleDataModelCorePropertyRelation] =
    resolveViewCorePropertyConfig(config, modelViewConfig)
      .map(
        viewCorePropertyConfig =>
          new FlexibleDataModelCorePropertyRelation(
            config,
            corePropConfig = viewCorePropertyConfig
          )(sqlContext))

  private def createConnectionRelationForDataModel(
      config: RelationConfig,
      sqlContext: SQLContext,
      modelConnectionConfig: DataModelConnectionConfig): IO[FlexibleDataModelConnectionRelation] = {
    val client = CdpConnector.clientFromConfig(config)
    fetchInlinedDataModel(client, modelConnectionConfig)
      .flatMap {
        _.flatMap(_.views.getOrElse(Seq.empty)).toVector
          .flatTraverse {
            case vc: ViewDefinition if vc.externalId == modelConnectionConfig.viewExternalId =>
              IO.delay(
                filterConnectionDefinition(vc, modelConnectionConfig.connectionPropertyName).toVector)
            case vr: ViewReference if vr.externalId == modelConnectionConfig.viewExternalId =>
              fetchViewWithAllProps(client, vr).map(
                _.headOption
                  .flatMap(filterConnectionDefinition(_, modelConnectionConfig.connectionPropertyName))
                  .toVector)
            case _ => IO.pure(Vector.empty)
          }
          .map(_.headOption)
      }
      .flatMap {
        case Some(cDef: EdgeConnection) =>
          IO.delay(
            new FlexibleDataModelConnectionRelation(
              config,
              ConnectionConfig(
                edgeTypeSpace = cDef.`type`.space,
                edgeTypeExternalId = cDef.`type`.externalId,
                instanceSpace = modelConnectionConfig.instanceSpace)
            )(sqlContext))
        case _ =>
          IO.raiseError(
            new CdfSparkIllegalArgumentException(s"""
              |Could not find a connection definition property named: '${modelConnectionConfig.connectionPropertyName}'
              | in the data model with (space: '${modelConnectionConfig.modelSpace}',
              | externalId: '${modelConnectionConfig.modelExternalId}',
              | version: '${modelConnectionConfig.modelVersion}')
              |""".stripMargin)
          )
      }
  }

  private def filterConnectionDefinition(
      viewDef: ViewDefinition,
      connectionPropertyName: String): Option[ConnectionDefinition] =
    viewDef.properties.collectFirst {
      case (name, p: EdgeConnection) if name == connectionPropertyName => p
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
