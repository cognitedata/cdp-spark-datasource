package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, Usage}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ConnectionDefinition
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModel
import com.cognite.sdk.scala.v1.fdm.views.{ViewDefinition, ViewReference}
import natchez.Span
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

  def connectionRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      connectionConfig: ConnectionConfig): FlexibleDataModelConnectionRelation =
    new FlexibleDataModelConnectionRelation(config, connectionConfig)(sqlContext)

  def dataModelRelation(
      config: RelationConfig,
      sqlContext: SQLContext,
      dataModelConfig: DataModelConfig
  ): TracedIO[FlexibleDataModelBaseRelation] =
    (dataModelConfig match {
      case vc: DataModelViewConfig => createCorePropertyRelationForDataModel(config, sqlContext, vc)
      case cc: DataModelConnectionConfig => createConnectionRelationForDataModel(config, sqlContext, cc)
    }).map(x => x)

  private def createCorePropertyRelationForDataModel(
      config: RelationConfig,
      sqlContext: SQLContext,
      modelViewConfig: DataModelViewConfig): TracedIO[FlexibleDataModelCorePropertyRelation] = {
    val client = CdpConnector.clientFromConfig(config)
    fetchInlinedDataModel(client, modelViewConfig)
      .map { models =>
        models.flatMap(_.views.getOrElse(Seq.empty)).find {
          case vc: ViewDefinition => vc.externalId == modelViewConfig.viewExternalId
          case vr: ViewReference => vr.externalId == modelViewConfig.viewExternalId
          case _ => false
        }
      }
      .flatMap[Option[ViewDefinition], Span[IO]] {
        case Some(vc: ViewDefinition) => TracedIO.pure(Some(vc))
        case Some(vr: ViewReference) => fetchViewWithAllProps(client, vr).map(_.headOption)
        case _ => TracedIO.pure(None)
      }
      .flatMap[FlexibleDataModelCorePropertyRelation, Span[IO]] {
        case Some(vc: ViewDefinition) =>
          TracedIO.liftIO(
            IO.delay(new FlexibleDataModelCorePropertyRelation(
              config,
              corePropConfig = ViewCorePropertyConfig(
                intendedUsage = vc.usedFor,
                viewReference = Some(vc.toSourceReference),
                instanceSpace = modelViewConfig.instanceSpace
              )
            )(sqlContext)))
        case None =>
          TracedIO.liftIO(IO.raiseError(new CdfSparkIllegalArgumentException(s"""
              |Could not find a view with externalId: '${modelViewConfig.viewExternalId}' in the specified data model
              | with (space: '${modelViewConfig.modelSpace}', externalId: '${modelViewConfig.modelExternalId}',
              | version: '${modelViewConfig.modelVersion}')
              |""".stripMargin)))
      }
  }

  private def createConnectionRelationForDataModel(
      config: RelationConfig,
      sqlContext: SQLContext,
      modelConnectionConfig: DataModelConnectionConfig)
    : TracedIO[FlexibleDataModelConnectionRelation] = {
    val client = CdpConnector.clientFromConfig(config)
    val fetch: TracedIO[Seq[DataModel]] = fetchInlinedDataModel(client, modelConnectionConfig)
    fetch
      .flatMap { x =>
        x.flatMap(_.views.getOrElse(Seq.empty))
          .toVector
          .flatTraverse[TracedIO, ConnectionDefinition] {
            case vc: ViewDefinition if vc.externalId == modelConnectionConfig.viewExternalId =>
              TracedIO
                .liftIO(IO.delay(
                  filterConnectionDefinition(vc, modelConnectionConfig.connectionPropertyName).toVector))
                .map(x => x)
            case vr: ViewReference if vr.externalId == modelConnectionConfig.viewExternalId =>
              fetchViewWithAllProps(client, vr)
                .map(_.headOption
                  .flatMap(filterConnectionDefinition(_, modelConnectionConfig.connectionPropertyName))
                  .toVector)
                .map(x => x)
            case _ => TracedIO.pure(Vector.empty)
          }
          .map(_.headOption)
      }
      .flatMap {
        case Some(cDef) =>
          TracedIO.liftIO(
            IO.delay(
              new FlexibleDataModelConnectionRelation(
                config,
                ConnectionConfig(
                  edgeTypeSpace = cDef.`type`.space,
                  edgeTypeExternalId = cDef.`type`.externalId,
                  instanceSpace = modelConnectionConfig.instanceSpace)
              )(sqlContext)))
        case _ =>
          TracedIO.liftIO(
            IO.raiseError(
              new CdfSparkIllegalArgumentException(s"""
              |Could not find a connection definition property named: '${modelConnectionConfig.connectionPropertyName}'
              | in the data model with (space: '${modelConnectionConfig.modelSpace}',
              | externalId: '${modelConnectionConfig.modelExternalId}',
              | version: '${modelConnectionConfig.modelVersion}')
              |""".stripMargin)
            ))
      }
  }

  private def filterConnectionDefinition(
      viewDef: ViewDefinition,
      connectionPropertyName: String): Option[ConnectionDefinition] =
    viewDef.properties.collectFirst {
      case (name, p: ConnectionDefinition) if name == connectionPropertyName => p
    }

  private def fetchInlinedDataModel(
      client: GenericClient[TracedIO],
      modelViewConfig: DataModelViewConfig): TracedIO[Seq[DataModel]] =
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
      client: GenericClient[TracedIO],
      modelConnectionConfig: DataModelConnectionConfig): TracedIO[Seq[DataModel]] =
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
      client: GenericClient[TracedIO],
      vr: ViewReference): TracedIO[Seq[ViewDefinition]] =
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
