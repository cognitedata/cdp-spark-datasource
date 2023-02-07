package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelRelationUtils.{createEdges, createNodes, createNodesOrEdges}
import cognite.spark.v1.FlexibleDataModelsRelation._
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterValueDefinition.{
  ComparableFilterValue,
  SeqFilterValue
}
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefinition,
  PropertyType
}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.{
  EdgeDeletionRequest,
  NodeDeletionRequest
}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{DataModelReference, ViewDefinition}
import fs2.Stream
import io.circe.{Decoder, Json}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.time._
import java.util.Locale
import scala.annotation.nowarn
import scala.util.Try
import scala.util.control.NonFatal

class FlexibleDataModelsRelation(
    config: RelationConfig,
    viewSpaceExternalId: String,
    viewExternalId: String,
    viewVersion: String,
    instanceSpaceExternalId: String)(val sqlContext: SQLContext)
    extends CdfRelation(config, FlexibleDataModelsRelation.ResourceType)
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._

  private val (viewDefinition, allViewProperties, viewSchema) = retrieveViewDefWithAllPropsAndSchema
    .unsafeRunSync()
    .getOrElse {
      throw new CdfSparkException(s"""
                                       |Correct view external id & view version should be specified.
                                       |Could not resolve schema from view externalId: $viewExternalId & view version: $viewVersion
                                       |""".stripMargin)
    }

  override def schema: StructType = viewSchema

  // scalastyle:off cyclomatic.complexity
  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val firstRow = rows.headOption
    firstRow match {
      case Some(firstRow) =>
        upsertNodesOrEdges(
          instanceSpaceExternalId,
          rows,
          firstRow.schema,
          viewDefinition.toSourceReference,
          allViewProperties,
          viewDefinition.usedFor
        ).flatMap(results => incMetrics(itemsUpserted, results.length))
      case None => incMetrics(itemsUpserted, 0)
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def insert(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Create is not supported for flexible data model instances. Use upsert instead."))

  override def buildScan(selectedColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedFlexibleDataModelInstance, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedFlexibleDataModelInstance, _) => toRow(item),
      _.externalId,
      getStreams(filters, selectedColumns)
    )

  // scalastyle:off cyclomatic.complexity
  override def delete(rows: Seq[Row]): IO[Unit] =
    viewDefinition.usedFor match {
      case Usage.Node => deleteNodesWithMetrics(rows)
      case Usage.Edge => deleteEdgesWithMetrics(rows)
      case Usage.All => deleteNodesOrEdgesWithMetrics(rows)
    }
  // scalastyle:on cyclomatic.complexity

  override def update(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException("Update is not supported for data model instances. Use upsert instead."))

  private def retrieveViewDefWithAllPropsAndSchema
    : IO[Option[(ViewDefinition, Map[String, ViewPropertyDefinition], StructType)]] =
    client.views
      .retrieveItems(
        Seq(DataModelReference(viewSpaceExternalId, viewExternalId, viewVersion)),
        includeInheritedProperties = Some(true))
      .map(_.headOption)
      .flatMap {
        case Some(viewDef) =>
          viewDef.implements.traverse { v =>
            if (v.isEmpty) {
              IO.pure(
                (
                  viewDef,
                  viewDef.properties,
                  deriveViewSchemaWithUsage(viewDef.usedFor, viewDef.properties)
                )
              )
            } else {
              client.views
                .retrieveItems(v.map(vRef =>
                  DataModelReference(vRef.space, vRef.externalId, vRef.version)))
                .map { inheritingViews =>
                  // If there are properties with the same name,
                  // most recent view's property will override the old view's property.
                  // At the time of this implementation, there's no requirement on this matter.
                  val inheritingProperties = inheritingViews
                    .sortBy(_.createdTime)(Ordering[Long].reverse)
                    .map(_.properties)
                    .reduce((propMap1, propMap2) => propMap1 ++ propMap2)

                  (viewDef, inheritingProperties ++ viewDef.properties)
                }
                .map {
                  case (viewDef, viewProps) =>
                    (viewDef, viewProps, deriveViewSchemaWithUsage(viewDef.usedFor, viewProps))
                }
            }
          }
        case None => IO.pure(None)
      }

  private def upsertNodesOrEdges(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      source: SourceReference,
      propDefMap: Map[String, PropertyDefinition],
      usedFor: Usage): IO[Seq[SlimNodeOrEdge]] = {
    val nodesOrEdges = usedFor match {
      case Usage.Node => createNodes(instanceSpaceExternalId, rows, schema, propDefMap, source)
      case Usage.Edge => createEdges(instanceSpaceExternalId, rows, schema, propDefMap, source)
      case Usage.All =>
        createNodesOrEdges(instanceSpaceExternalId, rows, schema, propDefMap, source)
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

  private def convertToSparkDataType(propType: PropertyType, nullable: Boolean): DataType = {
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

    propType match {
      case TextProperty(Some(true), _) => DataTypes.createArrayType(DataTypes.StringType, nullable)
      case TextProperty(_, _) => DataTypes.StringType
      case PrimitiveProperty(ppt, Some(true)) =>
        DataTypes.createArrayType(primitivePropTypeToSparkDataType(ppt), nullable)
      case PrimitiveProperty(ppt, _) => primitivePropTypeToSparkDataType(ppt)
      case DirectNodeRelationProperty(_) =>
        DataTypes.createArrayType(DataTypes.StringType, nullable) // TODO: Verify this
    }
  }

  private def toRow(a: ProjectedFlexibleDataModelInstance): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    new GenericRow(a.properties.map {
      // For startNode, endNode & type
      case a: Array[Any] => new GenericRow(a)
      case e => e
    })
  }

  private def toComparableFilterValueDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, ComparableFilterValue] =
    toFilterValueDefinition(attribute, value) match {
      case Right(v: ComparableFilterValue) => Right(v)
      case Right(v) =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""
               |Expecting a value of type number, string, boolean, json,
               |array[number], array[string], array[boolean], array[json] for '$attribute',
               |but found ${v.getClass.getSimpleName} in ${value.toString}
               |""".stripMargin
          )
        )
      case Left(err) => Left(err)
    }

  // scalastyle:off cyclomatic.complexity
  private def toFilterValueDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, FilterValueDefinition] =
    value match {
      case v: String =>
        io.circe.parser.parse(v) match {
          case Right(json) if json.isObject => Right(FilterValueDefinition.Object(json))
          case _ => Right(FilterValueDefinition.String(v))
        }
      case v: Float => Right(FilterValueDefinition.Double(v.toDouble))
      case v: Double => Right(FilterValueDefinition.Double(v))
      case v: Int => Right(FilterValueDefinition.Integer(v.toLong))
      case v: Long => Right(FilterValueDefinition.Integer(v))
      case v: Boolean => Right(FilterValueDefinition.Boolean(v))
      case v: java.math.BigDecimal => Right(FilterValueDefinition.Double(v.doubleValue))
      case v: BigInteger => Right(FilterValueDefinition.Integer(v.longValue))
      case v: LocalDate =>
        Right(FilterValueDefinition.String(v.format(InstancePropertyValue.Date.formatter)))
      case v: LocalDateTime =>
        Right(FilterValueDefinition.String(v.format(InstancePropertyValue.Timestamp.formatter)))
      case v: Instant =>
        Right(
          FilterValueDefinition.String(
            OffsetDateTime
              .ofInstant(v, ZoneId.of("UTC"))
              .toZonedDateTime
              .format(InstancePropertyValue.Timestamp.formatter)))
      case v: ZonedDateTime =>
        Right(FilterValueDefinition.String(v.format(InstancePropertyValue.Timestamp.formatter)))
      case v: Date =>
        Right(FilterValueDefinition.String(v.toLocalDate.format(InstancePropertyValue.Date.formatter)))
      case v: Timestamp =>
        Right(
          FilterValueDefinition.String(
            OffsetDateTime
              .ofInstant(v.toInstant, ZoneId.of("UTC"))
              .toZonedDateTime
              .format(InstancePropertyValue.Timestamp.formatter)))
      case v: Array[Any] =>
        toFilterValueListDefinition(attribute, v.toVector)
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a value of type number, string, boolean or an array of them for '$attribute', but found ${v.toString}"
          )
        )
    }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity method.length
  private def toFilterValueListDefinition(
      attribute: String,
      values: Vector[Any]): Either[CdfSparkException, FilterValueDefinition] = {
    val result = values.headOption match {
      case Some(s: String) =>
        io.circe.parser.parse(s).toOption match {
          case Some(_) =>
            Try(FilterValueDefinition.ObjectList(values.flatMap(v =>
              io.circe.parser.parse(String.valueOf(v)).toOption))).toEither
          case None =>
            Try(FilterValueDefinition.StringList(values.map(_.asInstanceOf[String]))).toEither
        }
      case Some(_: Int | _: Long) =>
        Try(FilterValueDefinition.IntegerList(values.map(_.asInstanceOf[Long]))).toEither
      case Some(_: Float | _: Double) =>
        Try(FilterValueDefinition.DoubleList(values.map(_.asInstanceOf[Double]))).toEither
      case Some(_: Boolean) =>
        Try(FilterValueDefinition.BooleanList(values.map(_.asInstanceOf[Boolean]))).toEither
      case _ =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting an array of number, string, boolean or json for '$attribute', but found ${String
              .valueOf(values)}"
          )
        )
    }
    result.leftMap {
      case e: CdfSparkIllegalArgumentException => e
      case e: Throwable =>
        val arrAsStr = values.map(String.valueOf).mkString(",")
        new CdfSparkIllegalArgumentException(
          s"""Expecting an array of number, string, boolean or json for '$attribute', but found $arrAsStr
             |${e.getMessage}
             |""".stripMargin
        )
    }
  }

  private def toSeqFilterValueDefinition(
      attribute: String,
      value: Array[Any]): Either[CdfSparkException, SeqFilterValue] =
    toFilterValueDefinition(attribute, value) match {
      case Right(v: SeqFilterValue) => Right(v)
      case Right(FilterValueDefinition.Double(v)) => Right(FilterValueDefinition.DoubleList(Seq(v)))
      case Right(FilterValueDefinition.Integer(v)) => Right(FilterValueDefinition.IntegerList(Seq(v)))
      case Right(FilterValueDefinition.String(v)) => Right(FilterValueDefinition.StringList(Seq(v)))
      case Right(FilterValueDefinition.Boolean(v)) => Right(FilterValueDefinition.BooleanList(Seq(v)))
      case Right(FilterValueDefinition.Object(v)) => Right(FilterValueDefinition.ObjectList(Seq(v)))
      case Right(v) =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a SeqFilterValue for '$attribute', but found ${v.getClass.getSimpleName} as ${value
              .mkString(",")}"
          )
        )
      case Left(err) => Left(err)
    }

  // scalastyle:off cyclomatic.complexity
  private def toInstanceFilter(sparkFilter: Filter): Either[CdfSparkException, FilterDefinition] = {
    val space = viewDefinition.space
    val externalId = viewDefinition.externalId

    sparkFilter match {
      case EqualTo(attribute, value) =>
        toFilterValueDefinition(attribute, value).map(
          FilterDefinition.Equals(Seq(space, externalId, attribute), _))
      case In(attribute, values) =>
        toSeqFilterValueDefinition(attribute, values).map(
          FilterDefinition.In(Seq(space, externalId, attribute), _))
      case GreaterThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, externalId, attribute), gte = Some(f)))
      case GreaterThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, externalId, attribute), gt = Some(f)))
      case LessThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, externalId, attribute), lte = Some(f)))
      case LessThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, externalId, attribute), lt = Some(f)))
      case StringStartsWith(attribute, value) =>
        Right(
          FilterDefinition
            .Prefix(Seq(space, externalId, attribute), FilterValueDefinition.String(value)))
      case And(f1, f2) =>
        List(f1, f2).traverse(toInstanceFilter).map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        List(f1, f2).traverse(toInstanceFilter).map(FilterDefinition.Or.apply)
      case IsNotNull(attribute) =>
        Right(FilterDefinition.Exists(Seq(space, externalId, attribute)))
      case IsNull(attribute) =>
        Right(FilterDefinition.Not(FilterDefinition.Exists(Seq(space, externalId, attribute))))
      case Not(f) => toInstanceFilter(f).map(FilterDefinition.Not.apply)
      case f =>
        Left(new CdfSparkIllegalArgumentException(s"Unsupported filter '${f.getClass.getSimpleName}'"))
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      @nowarn numPartitions: Int): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceType = getInstanceType(filters)

    val instanceFilter = filters.toVector.traverse(toInstanceFilter) match {
      case Right(fs) if fs.isEmpty => None
      case Right(fs) if fs.length == 1 => fs.headOption
      case Right(fs) => Some(FilterDefinition.And(fs))
      case Left(err) => throw err
    }

    usageBasedFilterRequests(limit, instanceType, instanceFilter).map { filterRequest =>
      client.instances
        .filterStream(filterRequest, limit)
        .map(toProjectedInstance(_, selectedInstanceProps))
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
                       |View with externalId: ${viewDefinition.externalId} & version: ${viewDefinition.version}" supports both Nodes & Edges.
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
  private def extractInstancePropertyValue: InstancePropertyValue => Any = {
    case InstancePropertyValue.String(value) => value
    case InstancePropertyValue.Int32(value) => value
    case InstancePropertyValue.Int64(value) => value
    case InstancePropertyValue.Float32(value) => value
    case InstancePropertyValue.Float64(value) => value
    case InstancePropertyValue.Boolean(value) => value
    case InstancePropertyValue.Date(value) => java.sql.Date.valueOf(value)
    case InstancePropertyValue.Timestamp(value) => java.sql.Timestamp.from(value.toInstant)
    case InstancePropertyValue.Object(value) => value.noSpaces
    case InstancePropertyValue.StringList(value) => value
    case InstancePropertyValue.BooleanList(value) => value
    case InstancePropertyValue.Int32List(value) => value
    case InstancePropertyValue.Int64List(value) => value
    case InstancePropertyValue.Float32List(value) => value
    case InstancePropertyValue.Float64List(value) => value
    case InstancePropertyValue.DateList(value) => value.map(v => java.sql.Date.valueOf(v))
    case InstancePropertyValue.TimestampList(value) =>
      value.map(v => java.sql.Timestamp.from(v.toInstant))
    case InstancePropertyValue.ObjectList(value) => value.map(_.noSpaces)
  }
  // scalastyle:on cyclomatic.complexity

  private def deriveViewSchemaWithUsage(usage: Usage, viewProps: Map[String, ViewPropertyDefinition]) = {
    val fields = viewProps.map {
      case (identifier, propType) =>
        DataTypes.createStructField(
          identifier,
          convertToSparkDataType(propType.`type`, propType.nullable.getOrElse(true)),
          propType.nullable.getOrElse(true))
    } ++ usageBasedSchemaFields(usage)
    DataTypes.createStructType(fields.toArray)
  }

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

  private def relationReferenceSchema(name: String, nullable: Boolean): StructField =
    DataTypes.createStructField(
      name,
      DataTypes.createStructType(
        Array(
          DataTypes.createStructField("space", DataTypes.StringType, false),
          DataTypes.createStructField("externalId", DataTypes.StringType, false)
        )
      ),
      nullable
    )

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

  private def toProjectedInstance(
      i: InstanceDefinition,
      selectedInstanceProps: Array[String]): ProjectedFlexibleDataModelInstance = {
    // Merging all the properties without considering the space & view/container externalId
    // At the time of this impl there is no requirement to consider properties with same name
    // in different view/containers
    val allAvailablePropValues =
      i.properties.getOrElse(Map.empty).values.flatMap(_.values).fold(Map.empty)(_ ++ _)

    i match {
      case n: InstanceDefinition.NodeDefinition =>
        ProjectedFlexibleDataModelInstance(
          externalId = n.externalId,
          properties = selectedInstanceProps.map {
            case "externalId" => n.externalId
            case p => allAvailablePropValues.get(p).map(extractInstancePropertyValue).orNull
          }
        )
      case e: InstanceDefinition.EdgeDefinition =>
        ProjectedFlexibleDataModelInstance(
          externalId = e.externalId,
          properties = selectedInstanceProps.map {
            case "externalId" => e.externalId
            case "startNode" => Array(e.startNode.space, e.startNode.externalId)
            case "endNode" => Array(e.endNode.space, e.endNode.externalId)
            case "type" => Array(e.relation.space, e.relation.externalId)
            case p => allAvailablePropValues.get(p).map(extractInstancePropertyValue).orNull
          }
        )
    }
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

object FlexibleDataModelsRelation {
  val ResourceType = "instances"

  final case class ProjectedFlexibleDataModelInstance(externalId: String, properties: Array[Any])
  final case class FlexibleDataModelInstanceDeleteModel(space: Option[String], externalId: String)
}
