package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.ViewCorePropertyConfig
import cognite.spark.v1.FlexibleDataModelRelationUtils.{createEdgeDeleteData, createEdges, createNodeDeleteData, createNodes}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, Usage}
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * FlexibleDataModelRelation for Nodes or Edges with properties
  * @param config common relation configs
  * @param corePropConfig view core property config
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelCorePropertyRelation(
    config: RelationConfig,
    corePropConfig: ViewCorePropertyConfig)(val sqlContext: SQLContext)
    extends FlexibleDataModelBaseRelation(config, sqlContext) {
  import CdpConnector._

  private val instanceType = corePropConfig.instanceType
  private val viewReference = corePropConfig.viewReference
  private val instanceSpace = corePropConfig.instanceSpace

  private val (allProperties, propertySchema) = retrieveViewDefWithAllPropsAndSchema
    .unsafeRunSync()
    .getOrElse {
      (
        Map.empty[String, ViewPropertyDefinition],
        DataTypes.createStructType(usageBasedSchemaAttributes(toUsage(instanceType)))
      )
    }

  override def schema: StructType = propertySchema

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

  private def retrieveViewDefWithAllPropsAndSchema
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
          case None => IO.pure(None)
          case Some(viewDef) if compatibleUsageForInstanceType(viewDef.usedFor, instanceType) =>
            IO.delay(
              Some((
                viewDef.properties,
                deriveViewPropertySchemaWithUsageSpecificAttributes(viewDef.usedFor, viewDef.properties)
              )))
          case _ =>
            IO.raiseError(new CdfSparkIllegalArgumentException(s"""
               |View ${corePropConfig.viewReference} is not compatible with '${instanceType.productPrefix}' instances
               |""".stripMargin))
        }
    }

  private def compatibleUsageForInstanceType(usage: Usage, instanceType: InstanceType): Boolean =
    (usage, instanceType) match {
      case (Usage.All, _) => true
      case (Usage.Node, InstanceType.Node) => true
      case (Usage.Edge, InstanceType.Edge) => true
      case _ => false
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
}
