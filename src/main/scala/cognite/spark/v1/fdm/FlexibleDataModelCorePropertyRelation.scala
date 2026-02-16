package cognite.spark.v1.fdm

import cats.Apply
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.fdm.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.fdm.FlexibleDataModelQueryUtils.{generateTableExpression, sourceReference}
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory.ViewCorePropertyConfig
import cognite.spark.v1.fdm.FlexibleDataModelRelationUtils._
import cognite.spark.v1.{
  CdfSparkException,
  CdfSparkIllegalArgumentException,
  CdpConnector,
  RelationConfig
}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition.HasData
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, Usage}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import fs2.Stream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Flexible Data Model Relation for Nodes or Edges with properties
  * @param config common relation configs
  * @param corePropConfig view core property config
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelCorePropertyRelation(
    config: RelationConfig,
    corePropConfig: ViewCorePropertyConfig)(val sqlContext: SQLContext)
    extends FlexibleDataModelBaseRelation(config, sqlContext) {
  import CdpConnector._

  protected val intendedUsage: Usage = corePropConfig.intendedUsage
  protected val viewReference: Option[ViewReference] = corePropConfig.viewReference
  private val instanceSpace = corePropConfig.instanceSpace

  private val (allProperties, propertySchema) = retrieveAllViewPropsAndSchema
    .unsafeRunSync()
    .getOrElse {
      (
        Map.empty[String, ViewPropertyDefinition],
        DataTypes.createStructType(usageBasedSchemaAttributes(intendedUsage))
      )
    }

  override def schema: StructType = propertySchema

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val firstRow = rows.headOption
    firstRow match {
      case Some(fr) =>
        upsertNodesOrEdges(rows, fr.schema, viewReference, allProperties, instanceSpace)
          .flatMap(results =>
            for {
              _ <- incMetrics(itemsUpserted, results.length)
              _ <- incMetrics(itemsUpsertedNoop, results.count(!_.wasModified))
            } yield ())
      case None => incMetrics(itemsUpserted, 0)
    }
  }

  override def insert(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Create is not supported for flexible data model instances. Use upsert instead."))

  override def delete(rows: Seq[Row]): IO[Unit] =
    (rows.headOption, intendedUsage) match {
      case (Some(firstRow), Usage.Node) =>
        IO.fromEither(createNodeDeleteData(firstRow.schema, rows, instanceSpace))
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case (Some(firstRow), Usage.Edge) =>
        IO.fromEither(createEdgeDeleteData(firstRow.schema, rows, instanceSpace))
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case (Some(firstRow), Usage.All) =>
        val nodeOrEdgeDeleteData = createNodeDeleteData(firstRow.schema, rows, instanceSpace)
          .flatMap { nodes =>
            createEdgeDeleteData(firstRow.schema, rows, instanceSpace)
              .map(edges => nodes ++ edges)
          }
        IO.fromEither(nodeOrEdgeDeleteData)
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case (None, _) => incMetrics(itemsDeleted, 0)
    }

  override def update(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException("Update is not supported for data model instances. Use upsert instead."))

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceFilter = usageBasedPropertyFilter(intendedUsage, filters, viewReference) match {
      case Right(filters) => filters
      case Left(err) => throw err
    }

    def queryFilterWithHasData(
        instanceFilter: Option[FilterDefinition],
        viewReference: Option[ViewReference]): Option[FilterDefinition] =
      viewReference.map(
        ref =>
          FilterDefinition.And(
            Seq(
              instanceFilter,
              Some(HasData(Seq(ref)))
            ).flatten
        )
      )

    if (config.useQuery) {
      compatibleInstanceTypes(intendedUsage).distinct.map { instanceType =>
        val tableExpression =
          generateTableExpression(
            instanceType,
            queryFilterWithHasData(instanceFilter, viewReference),
            config.limitPerPartition)
        val selectExpression = SelectExpression(
          sources = sourceReference(instanceType, viewReference, selectedInstanceProps),
        )
        client.instances
          .queryStream(
            inputTableExpression = tableExpression,
            inputSelectExpression = selectExpression,
            limit = config.limitPerPartition,
            additionalFlags = config.enabledAdditionalFlag.map(_ -> true).toMap,
            batchSize = config.batchSize
          )
          .map(toProjectedInstance(_, None, selectedInstanceProps))
      }
    } else {
      val filterRequests = compatibleInstanceTypes(intendedUsage).map { instanceType =>
        InstanceFilterRequest(
          instanceType = Some(instanceType),
          filter = instanceFilter,
          sort = None,
          limit = config.limitPerPartition,
          cursor = None,
          sources = viewReference.map(r => Vector(InstanceSource(r))),
          includeTyping = Some(true),
          debug = optionalDebug(config.sendDebugFlag)
        )
      }
      filterRequests.distinct.map { fr =>
        client.instances
          .filterStream(fr, config.limitPerPartition)
          .map(toProjectedInstance(_, None, selectedInstanceProps))
      }
    }

  }

  private def usageBasedPropertyFilter(
      usage: Usage,
      filters: Array[Filter],
      ref: Option[ViewReference]): Either[CdfSparkException, Option[FilterDefinition]] =
    usage match {
      case Usage.Node =>
        filters.toVector
          .traverse(
            toFilter(
              InstanceType.Node,
              _,
              ref
            )
          )
          .map(toAndFilter)
      case Usage.Edge =>
        filters.toVector
          .traverse(
            toFilter(
              InstanceType.Edge,
              _,
              ref
            )
          )
          .map(toAndFilter)
      case Usage.All =>
        val nodeFilter = usageBasedPropertyFilter(Usage.Node, filters, ref)
        val edgeFilter = usageBasedPropertyFilter(Usage.Edge, filters, ref)
        nodeFilter.flatMap { nf =>
          edgeFilter.map { ef =>
            Apply[Option]
              .map2(nf, ef)((n, e) => FilterDefinition.Or(Vector(n, e)))
              .orElse(nf)
              .orElse(ef)
          }
        }
    }

  private def toAndFilter(filters: Vector[FilterDefinition]): Option[FilterDefinition] =
    if (filters.isEmpty) {
      None
    } else if (filters.length == 1) {
      filters.headOption
    } else {
      Some(FilterDefinition.And(filters))
    }

  private def retrieveAllViewPropsAndSchema
    : IO[Option[(Map[String, ViewPropertyDefinition], StructType)]] =
    corePropConfig.viewReference.flatTraverse { viewRef =>
      client.views
        .retrieveItems(
          Seq(
            DataModelReference(
              space = viewRef.space,
              externalId = viewRef.externalId,
              version = Some(viewRef.version))),
          includeInheritedProperties = Some(true))
        .map(_.headOption)
        .flatMap {
          case None =>
            IO.raiseError(new CdfSparkIllegalArgumentException(s"""
                                                                  |Could not retrieve view with (space: '${viewRef.space}', externalId: '${viewRef.externalId}', version: '${viewRef.version}').
                                                                  |Ensure that the transformation's credentials have access to the view's space.
                                                                  |""".stripMargin))
          case Some(viewDef)
              if compatibleUsageTypes(viewUsage = viewDef.usedFor, intendedUsage = intendedUsage) =>
            IO.delay(
              Some(
                (
                  viewDef.properties,
                  deriveViewPropertySchemaWithUsageSpecificAttributes(intendedUsage, viewDef.properties)
                )))
          case Some(viewDef) =>
            IO.raiseError(new CdfSparkIllegalArgumentException(s"""
                                                                  | View with (space: '${viewDef.space}', externalId: '${viewDef.externalId}', version: '${viewDef.version}')
                                                                  | is not compatible with '${intendedUsage.productPrefix}s'
                                                                  |""".stripMargin))
        }
    }

  private def upsertNodesOrEdges(
      rows: Seq[Row],
      schema: StructType,
      source: Option[SourceReference],
      propDefMap: Map[String, ViewPropertyDefinition],
      instanceSpace: Option[String]) = {
    val nodesOrEdges = intendedUsage match {
      case Usage.Node =>
        createNodes(rows, schema, propDefMap, source, instanceSpace, config.ignoreNullFields)
      case Usage.Edge =>
        createEdges(rows, schema, propDefMap, source, instanceSpace, config.ignoreNullFields)
      case Usage.All =>
        createNodesOrEdges(rows, schema, propDefMap, source, instanceSpace, config.ignoreNullFields)
    }
    nodesOrEdges match {
      case Right(items) if items.nonEmpty =>
        val instanceCreate = InstanceCreate(
          items = items,
          replace = Some(false),
          autoCreateStartNodes = Some(corePropConfig.autoCreateStartNodes),
          autoCreateEndNodes = Some(corePropConfig.autoCreateEndNodes),
          autoCreateDirectRelations = Some(corePropConfig.autoCreateDirectRelations)
        )
        client.instances.createItems(instanceCreate)
      case Right(_) => IO.pure(Vector.empty)
      case Left(err) => IO.raiseError(err)
    }
  }

  private def compatibleInstanceTypes(usage: Usage): Vector[InstanceType] =
    usage match {
      case Usage.Node => Vector(InstanceType.Node)
      case Usage.Edge => Vector(InstanceType.Edge)
      case Usage.All => Vector(InstanceType.Node, InstanceType.Edge)
    }

  private def compatibleUsageTypes(viewUsage: Usage, intendedUsage: Usage): Boolean =
    (viewUsage, intendedUsage) match {
      case (Usage.Node, Usage.Edge) => false
      case (Usage.Edge, Usage.Node) => false
      case _ => true
    }
}
