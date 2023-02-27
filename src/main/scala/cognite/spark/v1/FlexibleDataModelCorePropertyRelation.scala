package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelBaseRelation.{
  FlexibleDataModelInstanceDeleteModel,
  ProjectedFlexibleDataModelInstance
}
import cognite.spark.v1.FlexibleDataModelRelation.ViewCorePropertyConfig
import cognite.spark.v1.FlexibleDataModelRelationUtils.{createEdges, createNodes, createNodesOrEdges}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.{
  EdgeDeletionRequest,
  NodeDeletionRequest
}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{DataModelReference, ViewDefinition}
import fs2.Stream
import io.circe.{Decoder, Json}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.util.Locale
import scala.util.control.NonFatal

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

  private val viewSpaceExternalId = corePropConfig.viewSpace
  private val viewExternalId = corePropConfig.viewExternalId
  private val viewVersion = corePropConfig.viewVersion
  private val instanceSpace = corePropConfig.instanceSpace

  private val (viewDefinition, allViewProperties, viewSchema) = retrieveViewDefWithAllPropsAndSchema
    .unsafeRunSync()
    .getOrElse {
      throw new CdfSparkException(s"""Could not resolve schema from
           | space: $viewSpaceExternalId,
           | view externalId: $viewExternalId &
           | view version: $viewVersion
           | Correct view space, view externalId & view version should be specified.
           | """.stripMargin)
    }

  override def schema: StructType = viewSchema

  // scalastyle:off cyclomatic.complexity
  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val firstRow = rows.headOption
    (firstRow, instanceSpace) match {
      case (Some(fr), Some(instanceSpaceExtId)) =>
        upsertNodesOrEdges(
          instanceSpaceExtId,
          rows,
          fr.schema,
          viewDefinition.toSourceReference,
          allViewProperties,
          viewDefinition.usedFor
        ).flatMap(results => incMetrics(itemsUpserted, results.length))
      case (None, Some(_)) => incMetrics(itemsUpserted, 0)
      case (_, None) =>
        IO.raiseError(new CdfSparkException(s"'instanceSpace' id required to upsert data"))
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def insert(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Create is not supported for flexible data model instances. Use upsert instead."))

  override def delete(rows: Seq[Row]): IO[Unit] =
    viewDefinition.usedFor match {
      case Usage.Node => deleteNodesWithMetrics(rows)
      case Usage.Edge => deleteEdgesWithMetrics(rows)
      case Usage.All => deleteNodesOrEdgesWithMetrics(rows)
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

    val instanceType = getInstanceType(filters)

    val instanceFilter = filters.toVector.traverse(
      toInstanceFilter(
        _,
        space = viewSpaceExternalId,
        versionedExternalId = s"$viewExternalId/$viewVersion")) match {
      case Right(fs) if fs.isEmpty => None
      case Right(fs) if fs.length == 1 => fs.headOption
      case Right(fs) => Some(FilterDefinition.And(fs))
      case Left(err) => throw err
    }

    usageBasedFilterRequests(limit, instanceType, instanceFilter).map { filterRequest =>
      client.instances
        .filterStream(filterRequest, limit)
        // TODO: remove this temp fix for https://cognitedata.slack.com/archives/C031G8Y19HP/p1676890774181079
        .filter { instDef =>
          def allAvailableInstanceProps =
            instDef.properties.getOrElse(Map.empty).values.flatMap(_.values).fold(Map.empty)(_ ++ _)
          instDef.space == viewSpaceExternalId && allAvailableInstanceProps.nonEmpty
        }
        .map(toProjectedInstance(_, selectedInstanceProps))
    }
  }

  private def retrieveViewDefWithAllPropsAndSchema
    : IO[Option[(ViewDefinition, Map[String, ViewPropertyDefinition], StructType)]] =
    client.views
      .retrieveItems(
        Seq(DataModelReference(viewSpaceExternalId, viewExternalId, viewVersion)),
        includeInheritedProperties = Some(true))
      .map(_.headOption.map { viewDef =>
        (
          viewDef,
          viewDef.properties,
          deriveViewSchemaWithUsage(viewDef.usedFor, viewDef.properties)
        )
      })

  private def upsertNodesOrEdges(
      instanceSpace: String,
      rows: Seq[Row],
      schema: StructType,
      source: SourceReference,
      propDefMap: Map[String, ViewPropertyDefinition],
      usedFor: Usage): IO[Seq[SlimNodeOrEdge]] = {
    val nodesOrEdges = usedFor match {
      case Usage.Node => createNodes(instanceSpace, rows, schema, propDefMap, source)
      case Usage.Edge => createEdges(instanceSpace, rows, schema, propDefMap, source)
      case Usage.All =>
        createNodesOrEdges(instanceSpace, rows, schema, propDefMap, source)
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

  private def deleteNodesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[FlexibleDataModelInstanceDeleteModel](r))
    client.instances
      .delete(deleteCandidates.map(i =>
        NodeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  private def deleteEdgesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[FlexibleDataModelInstanceDeleteModel](r))
    client.instances
      .delete(deleteCandidates.map(i =>
        EdgeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  private def deleteNodesOrEdgesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[FlexibleDataModelInstanceDeleteModel](r))
    client.instances
      .delete(deleteCandidates.map(i =>
        NodeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
      .recoverWith {
        case NonFatal(nodeDeletionErr) =>
          client.instances
            .delete(deleteCandidates.map(i =>
              EdgeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
            .handleErrorWith { edgeDeletionErr =>
              IO.raiseError(
                new CdfSparkException(
                  s"""
                     |View with space: ${viewDefinition.space},
                     | externalId: ${viewDefinition.externalId}
                     | & version: ${viewDefinition.version}" supports both Nodes & Edges.
                     |Tried deleting as nodes and failed with: ${nodeDeletionErr.getMessage}
                     |Tried deleting as edges and failed with: ${edgeDeletionErr.getMessage}
                     |Please verify your data!
                     |""".stripMargin
                )
              )
            }
      }
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  // scalastyle:off cyclomatic.complexity
  private def deriveViewSchemaWithUsage(usage: Usage, viewProps: Map[String, ViewPropertyDefinition]) = {
    def primitivePropTypeToSparkDataType(ppt: PrimitivePropType): DataType = ppt match {
      case PrimitivePropType.Timestamp => DataTypes.TimestampType
      case PrimitivePropType.Date => DataTypes.DateType
      case PrimitivePropType.Boolean => DataTypes.BooleanType
      case PrimitivePropType.Float32 => DataTypes.FloatType
      case PrimitivePropType.Float64 => DataTypes.DoubleType
      case PrimitivePropType.Int32 => DataTypes.IntegerType
      case PrimitivePropType.Int64 => DataTypes.LongType
      case PrimitivePropType.Json => DataTypes.StringType
    }

    val fields = viewProps.map {
      case (propName, propDef) =>
        propDef match {
          case corePropDef: PropertyDefinition.ViewCorePropertyDefinition =>
            val nullable = corePropDef.nullable.getOrElse(true)
            corePropDef.`type` match {
              case _: DirectNodeRelationProperty =>
                relationReferenceSchema(propName, nullable = nullable)
              case t: TextProperty if t.isList =>
                DataTypes.createStructField(
                  propName,
                  DataTypes.createArrayType(DataTypes.StringType, nullable),
                  nullable)
              case _: TextProperty =>
                DataTypes.createStructField(propName, DataTypes.StringType, nullable)
              case p @ PrimitiveProperty(ppt, _) if p.isList =>
                DataTypes.createStructField(
                  propName,
                  DataTypes.createArrayType(primitivePropTypeToSparkDataType(ppt), nullable),
                  nullable)
              case PrimitiveProperty(ppt, _) =>
                DataTypes.createStructField(propName, primitivePropTypeToSparkDataType(ppt), nullable)
            }
          case _: PropertyDefinition.ConnectionDefinition =>
            relationReferenceSchema(propName, nullable = true)
        }
    } ++ usageBasedSchemaFields(usage)
    DataTypes.createStructType(fields.toArray)
  }
  // scalastyle:on cyclomatic.complexity

  // schema fields for relation references and node/edge identifiers
  // https://cognitedata.slack.com/archives/G012UKQJLC9/p1673627087186579
  private def usageBasedSchemaFields(usage: Usage): Array[StructField] =
    usage match {
      case Usage.Node =>
        Array(DataTypes.createStructField("externalId", DataTypes.StringType, false))
      case Usage.Edge =>
        Array(
          DataTypes.createStructField("externalId", DataTypes.StringType, false),
          relationReferenceSchema("type", nullable = false),
          relationReferenceSchema("startNode", nullable = false),
          relationReferenceSchema("endNode", nullable = false)
        )
      case Usage.All =>
        Array(
          DataTypes.createStructField("externalId", DataTypes.StringType, false),
          relationReferenceSchema("type", nullable = true),
          relationReferenceSchema("startNode", nullable = true),
          relationReferenceSchema("endNode", nullable = true)
        )
    }

  private def getInstanceType(filters: Array[Filter]) =
    (viewDefinition.usedFor match {
      case Usage.Node => Some(InstanceType.Node)
      case Usage.Edge => Some(InstanceType.Edge)
      case Usage.All => None
    }).orElse {
      filters.collectFirst {
        case EqualTo(attribute, value) if attribute.equalsIgnoreCase("instanceType") =>
          Decoder[InstanceType]
            .decodeJson(Json.fromString(String.valueOf(value).toLowerCase(Locale.US)))
            .toOption
      }.flatten
    }

  private def usageBasedFilterRequests(
      limit: Option[Int],
      instanceType: Option[InstanceType],
      instanceFilter: Option[FilterDefinition]): Seq[InstanceFilterRequest] = {
    val defaultFilterReq = InstanceFilterRequest(
      instanceType = instanceType,
      filter = instanceFilter,
      sort = None,
      limit = limit,
      cursor = None,
      sources = Some(Seq(viewDefinition.toInstanceSource)),
      includeTyping = Some(true)
    )
    viewDefinition.usedFor match {
      case Usage.Node =>
        Seq(defaultFilterReq.copy(instanceType = instanceType.orElse(Some(InstanceType.Node))))
      case Usage.Edge =>
        Seq(defaultFilterReq.copy(instanceType = instanceType.orElse(Some(InstanceType.Edge))))
      case Usage.All =>
        Seq(
          defaultFilterReq.copy(instanceType = instanceType.orElse(Some(InstanceType.Node))),
          defaultFilterReq.copy(instanceType = instanceType.orElse(Some(InstanceType.Edge)))
        ).distinct
    }
  }
}
