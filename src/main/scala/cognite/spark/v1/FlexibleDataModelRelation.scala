package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory._
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.DataModelReference
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ConnectionDefinition
import com.cognite.sdk.scala.v1.fdm.views.{ViewDefinition, ViewReference}
import fs2.Stream
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * FlexibleDataModelRelation for interacting with data models
  *
  * @param config common relation configs
  * @param dataModelConfig data model details
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelRelation(config: RelationConfig, dataModelConfig: DataModelConfig)(
    val sqlContext: SQLContext)
    extends FlexibleDataModelBaseRelation(config, sqlContext) {

  private val relation = (dataModelConfig match {
    case vc: DataModelViewConfig => createCorePropertyRelationForDataModel(vc)
    case cc: DataModelConnectionConfig => createConnectionRelationForDataModel(cc)
  }).unsafeRunSync()

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] =
    relation.getStreams(filters, selectedColumns)(client, limit, numPartitions)

  override def insert(rows: Seq[Row]): IO[Unit] = relation.insert(rows)

  override def upsert(rows: Seq[Row]): IO[Unit] = relation.upsert(rows)

  override def update(rows: Seq[Row]): IO[Unit] = relation.update(rows)

  override def delete(rows: Seq[Row]): IO[Unit] = relation.delete(rows)

  override def schema: StructType = relation.schema

  private def createCorePropertyRelationForDataModel(
      modelViewConfig: DataModelViewConfig): IO[FlexibleDataModelCorePropertyRelation] =
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
      .map { models =>
        models.flatMap(_.views.getOrElse(Seq.empty)).find {
          case vc: ViewDefinition => vc.externalId == modelViewConfig.viewExternalId
          case vr: ViewReference => vr.externalId == modelViewConfig.viewExternalId
          case _ => false
        }
      }
      .flatMap {
        case Some(vc: ViewDefinition) => IO.pure(Some(vc))
        case Some(vr: ViewReference) => fetchViewWithAllProps(vr).map(_.headOption)
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
                instanceSpace = None
              ))(sqlContext)
          )
        case None =>
          IO.raiseError(new CdfSparkIllegalArgumentException(s"""
               |Could not find a view with externalId: '${modelViewConfig.viewExternalId}' in the specified data model
               | with (space: '${modelViewConfig.modelSpace}', externalId: '${modelViewConfig.modelExternalId}', version: '${modelViewConfig.modelVersion}')
               |""".stripMargin))
      }

  private def createConnectionRelationForDataModel(
      modelConnectionConfig: DataModelConnectionConfig): IO[FlexibleDataModelConnectionRelation] =
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
      .flatMap {
        _.flatMap(_.views.getOrElse(Seq.empty)).toVector
          .traverse {
            case vc: ViewDefinition =>
              IO.delay(connectionDefinitionExists(vc, modelConnectionConfig.connectionConfig))
            case vr: ViewReference =>
              fetchViewWithAllProps(vr).map(_.headOption.exists(
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
          IO.raiseError(new CdfSparkIllegalArgumentException(s"""
               |Could not find a connection definition with
               | (edgeTypeSpace: '${modelConnectionConfig.connectionConfig.edgeTypeSpace}', edgeTypeExternalId: '${modelConnectionConfig.connectionConfig.edgeTypeExternalId}')
               | in any of the views linked with the data model (space: '${modelConnectionConfig.modelSpace}', externalId: '${modelConnectionConfig.modelExternalId}', version: '${modelConnectionConfig.modelVersion}')
               |""".stripMargin))
      }

  private def connectionDefinitionExists(
      viewDef: ViewDefinition,
      connectionConfig: ConnectionConfig): Boolean =
    viewDef.properties
      .collectFirst {
        case (_, p: ConnectionDefinition) =>
          p.`type`.space == connectionConfig.edgeTypeSpace &&
            p.`type`.externalId == connectionConfig.edgeTypeExternalId
      }
      .contains(true)

  private def fetchViewWithAllProps(vr: ViewReference): IO[Seq[ViewDefinition]] =
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
