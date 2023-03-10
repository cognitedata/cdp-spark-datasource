package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.{DataModelConfig, DataModelConnectionConfig, DataModelViewConfig}
import cognite.spark.v1.FlexibleDataModelRelationUtils.{createEdgeDeleteData, createEdges, createNodeDeleteData, createNodes}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.DataModelReference
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.ViewPropertyCreateDefinition.ConnectionDefinition
import com.cognite.sdk.scala.v1.fdm.views.{ViewCreateDefinition, ViewReference}
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

  private val schemaForDataModel = (dataModelConfig match {
    case dm: DataModelConnectionConfig =>
      deriveSchemaForDataModelConnection(dm)
    case dm: DataModelViewConfig =>
      deriveSchemaForDataModeView(dm)
  }).unsafeRunSync()

  override def schema: StructType = schemaForDataModel

  // scalastyle:off cyclomatic.complexity
  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val firstRow = rows.headOption
    (firstRow, viewReference) match {
      case (Some(fr), Some(viewRef)) =>
        upsertNodesOrEdges(
          instanceSpace,
          rows,
          fr.schema,
          viewRef,
          allProperties
        ).flatMap(results => incMetrics(itemsUpserted, results.length))
      case (Some(fr), None) =>
        upsertNodesOrEdges(
          instanceSpace,
          rows,
          fr.schema
        ).flatMap(results => incMetrics(itemsUpserted, results.length))
      case (None, _) => incMetrics(itemsUpserted, 0)
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def insert(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Create is not supported for flexible data model instances. Use upsert instead."))

  override def delete(rows: Seq[Row]): IO[Unit] =
    (rows.headOption, instanceType) match {
      case (Some(firstRow), InstanceType.Node) =>
        IO.fromEither(createNodeDeleteData(firstRow.schema, rows))
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case (Some(firstRow), InstanceType.Edge) =>
        IO.fromEither(createEdgeDeleteData(firstRow.schema, rows))
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case (None, _) => incMetrics(itemsDeleted, 0)
    }

  override def update(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException("Update is not supported for data model instances. Use upsert instead."))

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceFilter = viewReference
      .map { ref =>
        filters.toVector.traverse(
          toInstanceFilter(
            instanceType,
            _,
            space = ref.space,
            versionedExternalId = s"${ref.externalId}/${ref.version}"))
      }
      .getOrElse(filters.toVector.traverse(toNodeOrEdgeAttributeFilter(instanceType, _))) match {
      case Right(fs) if fs.isEmpty => None
      case Right(fs) if fs.length == 1 => fs.headOption
      case Right(fs) => Some(FilterDefinition.And(fs))
      case Left(err) => throw err
    }

    val filterRequest = InstanceFilterRequest(
      instanceType = Some(instanceType),
      filter = instanceFilter,
      sort = None,
      limit = limit,
      cursor = None,
      sources = viewReference.map(r => Vector(InstanceSource(r))),
      includeTyping = Some(true)
    )
    Vector(
      client.instances
        .filterStream(filterRequest, limit)
        .map(toProjectedInstance(_, selectedInstanceProps))
    )
  }

  private def upsertNodesOrEdges(
      instanceSpace: Option[String],
      rows: Seq[Row],
      schema: StructType,
      source: SourceReference,
      propDefMap: Map[String, ViewPropertyDefinition]): IO[Seq[SlimNodeOrEdge]] = {
    val nodesOrEdges = instanceType match {
      case InstanceType.Node => createNodes(instanceSpace, rows, schema, propDefMap, source)
      case InstanceType.Edge => createEdges(instanceSpace, rows, schema, propDefMap, source)
    }

    nodesOrEdges match {
      case Right(items) if items.nonEmpty =>
        val instanceCreate = InstanceCreate(
          items = items,
          replace = Some(true)
        )
        client.instances.createItems(instanceCreate)
      case Right(_) => IO.pure(Vector.empty)
      case Left(err) => IO.raiseError(err)
    }
  }

  private def upsertNodesOrEdges(
      instanceSpace: Option[String],
      rows: Seq[Row],
      schema: StructType): IO[Seq[SlimNodeOrEdge]] = {
    val nodesOrEdges = instanceType match {
      case InstanceType.Node => createNodes(instanceSpace, rows, schema)
      case InstanceType.Edge => createEdges(instanceSpace, rows, schema)
    }

    nodesOrEdges match {
      case Right(items) if items.nonEmpty =>
        val instanceCreate = InstanceCreate(
          items = items,
          replace = Some(true)
        )
        client.instances.createItems(instanceCreate)
      case Right(_) => IO.pure(Vector.empty)
      case Left(err) => IO.raiseError(err)
    }
  }

  private def deriveSchemaForDataModeView(dm: DataModelViewConfig): IO[StructType] =
    client.dataModelsV3
      .retrieveItems(
        Seq(
          DataModelReference(
            space = dm.modelSpace,
            externalId = dm.modelExternalId,
            version = Some(dm.modelVersion))
        ))
      .map { models =>
        models.flatMap(_.views.getOrElse(Seq.empty)).find {
          case vc: ViewCreateDefinition => vc.externalId == dm.viewExternalId
          case vr: ViewReference => vr.externalId == dm.viewExternalId
          case _ => false
        }
      }
      .flatMap {
        case Some(vc: ViewCreateDefinition) =>
          IO.pure(
            DataModelReference(space = vc.space, externalId = vc.externalId, version = Some(vc.version)))
        case Some(vr: ViewReference) =>
          IO.pure(
            DataModelReference(space = vr.space, externalId = vr.externalId, version = Some(vr.version)))
        case None =>
          IO.raiseError(new CdfSparkIllegalArgumentException(s"""
               |Could not find a view with externalId: '${dm.viewExternalId}' in the specified data model
               | with (space: '${dm.modelSpace}', externalId: '${dm.modelExternalId}', version: '${dm.modelVersion}')
               |""".stripMargin))
      }
      .flatMap { ref =>
        client.views
          .retrieveItems(Seq(ref), includeInheritedProperties = Some(true))
          .map(_.headOption.map(v =>
            deriveViewPropertySchemaWithUsageSpecificAttributes(v.usedFor, v.properties)))
          .flatMap {
            case Some(schema) => IO.pure(schema)
            case None =>
              IO.raiseError(new CdfSparkIllegalArgumentException(s"""
                 |Could not find a view with (space: '${ref.space}', externalId: '${ref.externalId}', version: '${ref.version}')
                 | in any of the views linked to data model (space: '${dm.modelSpace}', externalId: '${dm.modelExternalId}', version: '${dm.modelVersion}')
                 |""".stripMargin))
          }
      }

  private def deriveSchemaForDataModelConnection(dm: DataModelConnectionConfig): IO[StructType] =
    client.dataModelsV3
      .retrieveItems(
        Seq(
          DataModelReference(
            space = dm.modelSpace,
            externalId = dm.modelExternalId,
            version = Some(dm.modelVersion))
        ))
      .flatMap {
        _.flatMap(_.views.getOrElse(Seq.empty)).toVector
          .traverse {
            case vc: ViewCreateDefinition =>
              IO.delay {
                vc.properties
                  .exists {
                    case (_, p: ConnectionDefinition) =>
                      p.`type`.space == dm.connectionConfig.edgeTypeSpace &&
                        p.`type`.externalId == dm.connectionConfig.edgeTypeExternalId
                  }
              }
            case vr: ViewReference =>
              fetchView(vr)
                .map(_.headOption.exists {
                  _.properties.exists {
                    case (_, p: PropertyDefinition.ConnectionDefinition) =>
                      p.`type`.space == dm.connectionConfig.edgeTypeSpace &&
                        p.`type`.externalId == dm.connectionConfig.edgeTypeExternalId
                  }
                })
            case _ => IO.pure(false)
          }
          .map(_.contains(true))
      }
      .flatMap {
        case true => IO.delay(connectionDefinitionEdgeSchema)
        case false =>
          IO.raiseError(new CdfSparkIllegalArgumentException(s"""
               |Could not find a connection definition with
               | (edgeTypeSpace: '${dm.connectionConfig.edgeTypeSpace}', edgeTypeExternalId: '${dm.connectionConfig.edgeTypeExternalId}')
               | in any of the views linked to data model (space: '${dm.modelSpace}', externalId: '${dm.modelExternalId}', version: '${dm.modelVersion}')
               |""".stripMargin))
      }

  private def connectionDefinitionEdgeSchema =
    DataTypes.createStructType(
      Array(
        DataTypes.createStructField("space", DataTypes.StringType, false),
        DataTypes.createStructField("externalId", DataTypes.StringType, false),
        relationReferenceSchema("startNode", nullable = false),
        relationReferenceSchema("endNode", nullable = false)
      )
    )

  private def fetchView(vr: ViewReference) =
    client.views
      .retrieveItems(
        Seq(
          DataModelReference(
            space = vr.space,
            externalId = vr.externalId,
            version = Some(vr.version)
          )
        )
      )
}
