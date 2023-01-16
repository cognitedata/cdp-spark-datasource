package cognite.spark.v1

import cats.Apply
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstancesRelationV3._
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterValueDefinition.{
  ComparableFilterValue,
  LogicalFilterValue,
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
import com.cognite.sdk.scala.v1.fdm.common.refs.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.{
  EdgeDeletionRequest,
  NodeDeletionRequest
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{DataModelReference, ViewDefinition}
import com.cognite.sdk.scala.v1.resources.fdm.instances.Instances.instanceCreateEncoder
import fs2.Stream
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter
import scala.annotation.nowarn
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class DataModelInstancesRelationV3(
    config: RelationConfig,
    viewSpaceExternalId: String,
    viewExternalId: String,
    viewVersion: String,
    instanceSpaceExternalId: String)(val sqlContext: SQLContext)
    extends CdfRelation(config, DataModelInstancesRelationV3.ResourceType)
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._

  private val client = alphaClient
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
    println(rows.length)
    println(rows.map(_.prettyJson).mkString(System.lineSeparator()))
    firstRow match {
      case Some(firstRow) =>
        upsertNodesOrEdges(
          instanceSpaceExternalId,
          rows,
          firstRow.schema,
          viewDefinition.toViewReference,
          allViewProperties,
          viewDefinition.usedFor
        ).flatMap(results => incMetrics(itemsUpserted, results.length))
      case None if rows.isEmpty => incMetrics(itemsUpserted, 0)
    }
  }
  // scalastyle:on cyclomatic.complexity

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
            client.views
              .retrieveItems(v.map(vRef =>
                DataModelReference(vRef.space, vRef.externalId, vRef.version)))
              .map { inheritingViews =>
                val inheritingProperties = inheritingViews
                  .map(_.properties)
                  .reduce((propMap1, propMap2) => propMap1 ++ propMap2)

                (viewDef, inheritingProperties ++ viewDef.properties)
              }
              .map {
                case (viewDef, viewProps) =>
                  (viewDef, viewProps, deriveViewSchemaWithUsage(viewDef.usedFor, viewProps))
              }
          }
        case None => IO.pure(None)
      }

  private def upsertNodesOrEdges(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      destinationRef: SourceReference,
      propDefMap: Map[String, PropertyDefinition],
      usedFor: Usage): IO[Seq[SlimNodeOrEdge]] = {
    val nodesOrEdges = usedFor match {
      case Usage.Node => createNodes(instanceSpaceExternalId, rows, schema, propDefMap, destinationRef)
      case Usage.Edge => createEdges(instanceSpaceExternalId, rows, schema, propDefMap, destinationRef)
      case Usage.All =>
        createNodesOrEdges(instanceSpaceExternalId, rows, schema, destinationRef, propDefMap)
    }

    nodesOrEdges match {
      case Left(err) => IO.raiseError(err)
      case Right(items) =>
        val instanceCreate = InstanceCreate(
          items = items,
          autoCreateStartNodes = Some(true),
          autoCreateEndNodes = Some(true),
          replace = Some(false) // TODO: verify this
        )

        println(instanceCreate.asJson.noSpaces)
        client.instances.createItems(instanceCreate)
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

  def insert(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Create (abort) is not supported for data model instances. Use upsert instead."))

  def toRow(a: ProjectedDataModelInstanceV3): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    new GenericRow(a.properties)
  }

  def toComparableFilterValueDefinition(
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

  def toLogicalFilterValueDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, LogicalFilterValue] =
    toFilterValueDefinition(attribute, value) match {
      case Right(v: LogicalFilterValue) => Right(v)
      case Right(v) =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a boolean value for '$attribute', but found ${v.getClass.getSimpleName} in ${value.toString}"
          )
        )
      case Left(err) => Left(err)
    }

  // scalastyle:off cyclomatic.complexity
  def toFilterValueDefinition(
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
      case v: Array[_] => toFilterValueListDefinition(attribute, v)
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a value of type number, string, boolean or an array of them for '$attribute', but found ${v.toString}"
          )
        )
    }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity method.length
  def toFilterValueListDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, FilterValueDefinition] =
    value match {
      case v: Array[String] =>
        v.headOption.flatMap(io.circe.parser.parse(_).toOption) match {
          case Some(_) =>
            Right(FilterValueDefinition.ObjectList(v.flatMap(io.circe.parser.parse(_).toOption)))
          case None => Right(FilterValueDefinition.StringList(v))
        }
      case v: Array[Float] => Right(FilterValueDefinition.DoubleList(v.map(_.toDouble)))
      case v: Array[Double] => Right(FilterValueDefinition.DoubleList(v))
      case v: Array[Int] => Right(FilterValueDefinition.IntegerList(v.map(_.toLong)))
      case v: Array[Long] => Right(FilterValueDefinition.IntegerList(v))
      case v: Array[Boolean] => Right(FilterValueDefinition.BooleanList(v))
      case v: Array[java.math.BigDecimal] =>
        Right(FilterValueDefinition.DoubleList(v.map(_.doubleValue)))
      case v: Array[BigInteger] => Right(FilterValueDefinition.IntegerList(v.map(_.longValue)))
      case v: Array[LocalDate] =>
        Right(FilterValueDefinition.StringList(v.map(_.format(InstancePropertyValue.Date.formatter))))
      case v: Array[LocalDateTime] =>
        Right(
          FilterValueDefinition.StringList(v.map(_.format(InstancePropertyValue.Timestamp.formatter))))
      case v: Array[Instant] =>
        Right(
          FilterValueDefinition.StringList(
            v.map(
              OffsetDateTime
                .ofInstant(_, ZoneId.of("UTC"))
                .toZonedDateTime
                .format(InstancePropertyValue.Timestamp.formatter))))
      case v: Array[ZonedDateTime] =>
        Right(
          FilterValueDefinition.StringList(v.map(_.format(InstancePropertyValue.Timestamp.formatter))))
      case v: Array[Date] =>
        Right(
          FilterValueDefinition.StringList(
            v.map(_.toLocalDate.format(InstancePropertyValue.Date.formatter))))
      case v: Array[Timestamp] =>
        Right(
          FilterValueDefinition.StringList(
            v.map(
              ts =>
                OffsetDateTime
                  .ofInstant(ts.toInstant, ZoneId.of("UTC"))
                  .toZonedDateTime
                  .format(InstancePropertyValue.Timestamp.formatter))))
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a value of type number, string, boolean or an array of them for '$attribute', but found ${v.toString}"
          )
        )
    }
  // scalastyle:on cyclomatic.complexity method.length

  def toSeqFilterValueDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, SeqFilterValue] =
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
            s"Expecting a SeqFilterValue for '$attribute', but found ${v.getClass.getSimpleName} as ${value.toString}"
          )
        )
      case Left(err) => Left(err)
    }

  // scalastyle:off cyclomatic.complexity
  def toInstanceFilter(sparkFilter: Filter): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) =>
        toFilterValueDefinition(attribute, value).map(FilterDefinition.Equals(Seq(attribute), _))
      case In(attribute, values) =>
        toSeqFilterValueDefinition(attribute, values).map(FilterDefinition.In(Seq(attribute), _))
      case GreaterThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), gte = Some(f)))
      case GreaterThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), gt = Some(f)))
      case LessThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), lte = Some(f)))
      case LessThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), lt = Some(f)))
      case And(f1, f2) =>
        List(f1, f2).traverse(toInstanceFilter).map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        List(f1, f2).traverse(toInstanceFilter).map(FilterDefinition.Or.apply)
      case IsNotNull(attribute) => Right(FilterDefinition.Exists(Seq(attribute)))
      case IsNull(attribute) => Right(FilterDefinition.Not(FilterDefinition.Exists(Seq(attribute))))
      case Not(f) => toInstanceFilter(f).map(FilterDefinition.Not.apply)
      case f =>
        Left(new CdfSparkIllegalArgumentException(s"Unsupported filter '${f.getClass.getSimpleName}'"))
    }
  // scalastyle:on cyclomatic.complexity

  private def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      @nowarn client: GenericClient[IO],
      limit: Option[Int],
      @nowarn numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstanceV3]] = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceType = {
      (viewDefinition.usedFor match {
        case Usage.Node => Some(InstanceType.Node)
        case Usage.Edge => Some(InstanceType.Edge)
        case Usage.All => None
      }).orElse {
        filters.collectFirst {
          case EqualTo("instanceType", value) =>
            Decoder[InstanceType].decodeJson(Json.fromString(String.valueOf(value))).toOption
        }.flatten
      }
    }

    val instanceFilter = filters.toVector.traverse(toInstanceFilter) match {
      case Right(fs) if fs.isEmpty => None
      case Right(fs) => Some(FilterDefinition.And(fs))
      case Left(err) => throw err
    }

    val filterRequest = InstanceFilterRequest(
      instanceType = instanceType,
      filter = instanceFilter,
      sort = None, // Some(Seq(PropertyFilterV3))
      limit = limit,
      cursor = None,
      sources = Some(Seq(viewDefinition.toViewReference))
    )

    Seq(
      client.instances
        .filterStream(filterRequest, limit)
        .map(toProjectedInstance(_, selectedInstanceProps)))
  }

  override def buildScan(selectedColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedDataModelInstanceV3, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedDataModelInstanceV3, _) => toRow(item),
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

  private def deleteNodesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceV3DeleteModel](r))
    client.instances
      .delete(deleteCandidates.map(i =>
        NodeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  private def deleteEdgesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceV3DeleteModel](r))
    client.instances
      .delete(deleteCandidates.map(i =>
        EdgeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  private def deleteNodesOrEdgesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceV3DeleteModel](r))
    client.instances
      .delete(deleteCandidates.map(i =>
        NodeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
      .recoverWith {
        case NonFatal(nodeDeletionErr) =>
          client.instances
            .delete(deleteCandidates.map(i =>
              EdgeDeletionRequest(i.space.getOrElse(viewSpaceExternalId), i.externalId)))
            .handleErrorWith {
              case NonFatal(edgeDeletionErr) =>
                IO.raiseError(
                  new CdfSparkException(
                    s"""
                       |View with externalId: ${viewDefinition.externalId} & version: ${viewDefinition.version}" supports both Nodes & Edges.
                       |Tried deleting as nodes and failed: ${nodeDeletionErr.getMessage}
                       |Tried deleting as edges and failed: ${edgeDeletionErr.getMessage}
                       |Please verify your data
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
        Array(DataTypes.createStructField("instanceExternalId", DataTypes.StringType, false))
      case Usage.Edge =>
        Array(
          DataTypes.createStructField("instanceExternalId", DataTypes.StringType, false),
          relationReferenceSchema("type", nullable = false),
          relationReferenceSchema("startNode", nullable = false),
          relationReferenceSchema("endNode", nullable = false)
        )
      case Usage.All =>
        Array(
          DataTypes.createStructField("instanceExternalId", DataTypes.StringType, false),
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

  private def toProjectedInstance(
      i: InstanceDefinition,
      selectedInstanceProps: Array[String]): ProjectedDataModelInstanceV3 = {
    val (externalId, properties) = i match {
      case n: InstanceDefinition.NodeDefinition => (n.externalId, n.properties)
      case e: InstanceDefinition.EdgeDefinition => (e.externalId, e.properties)
    }

    val allAvailablePropValues =
      properties.getOrElse(Map.empty).values.flatMap(_.values).fold(Map.empty)(_ ++ _)

    ProjectedDataModelInstanceV3(
      externalId = externalId,
      properties = selectedInstanceProps.map { prop =>
        allAvailablePropValues.get(prop).map(extractInstancePropertyValue).orNull
      }
    )
  }
}

object DataModelInstancesRelationV3 {
  val ResourceType = "datamodelinstancesV3"

  final case class ProjectedDataModelInstanceV3(externalId: String, properties: Array[Any])
  final case class DataModelInstanceV3DeleteModel(space: Option[String], externalId: String)

  private[spark] def createNodes(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      destinationRef: SourceReference): Either[CdfSparkException, Vector[NodeWrite]] =
    validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap) *> createNodeWriteData(
      instanceSpaceExternalId,
      schema,
      destinationRef,
      propertyDefMap,
      rows)

  def createEdges(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      destinationRef: SourceReference): Either[CdfSparkException, Vector[EdgeWrite]] =
    validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap) *> createEdgeWriteData(
      instanceSpaceExternalId,
      schema,
      destinationRef,
      propertyDefMap,
      rows)

  def createNodesOrEdges(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      destinationRef: SourceReference,
      propertyDefMap: Map[String, PropertyDefinition],
  ): Either[CdfSparkException, Vector[NodeOrEdgeCreate]] =
    rows.toVector.traverse { row =>
      for {
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(propertyDefMap, schema, row)
        writeData <- createNodeOrEdgeWriteData(
          externalId = externalId,
          instanceSpaceExternalId = instanceSpaceExternalId,
          destinationRef,
          edgeNodeTypeRelation = extractEdgeTypeDirectRelation(schema, row).toOption,
          startNodeRelation = extractEdgeStartNodeDirectRelation(schema, row).toOption,
          endNodeRelation = extractEdgeEndNodeDirectRelation(schema, row).toOption,
          props,
          row
        )
      } yield writeData
    }

  private def createEdgeWriteData(
      instanceSpaceExternalId: String,
      schema: StructType,
      destinationRef: SourceReference,
      propertyDefMap: Map[String, PropertyDefinition],
      rows: Seq[Row]): Either[CdfSparkException, Vector[EdgeWrite]] =
    rows.toVector.traverse { row =>
      for {
        extId <- extractExternalId(schema, row)
        edgeType <- extractEdgeTypeDirectRelation(schema, row)
        startNode <- extractEdgeStartNodeDirectRelation(schema, row)
        endNode <- extractEdgeEndNodeDirectRelation(schema, row)
        props <- extractInstancePropertyValues(propertyDefMap, schema, row)
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = instanceSpaceExternalId,
          externalId = extId,
          startNode = startNode,
          endNode = endNode,
          sources = Seq(
            EdgeOrNodeData(
              source = destinationRef,
              properties = Some(props.toMap)
            )
          )
        )
    }

  private def createNodeOrEdgeWriteData(
      externalId: String,
      instanceSpaceExternalId: String,
      destinationRef: SourceReference,
      edgeNodeTypeRelation: Option[DirectRelationReference],
      startNodeRelation: Option[DirectRelationReference],
      endNodeRelation: Option[DirectRelationReference],
      props: Vector[(String, InstancePropertyValue)],
      row: Row): Either[CdfSparkException, NodeOrEdgeCreate] =
    (edgeNodeTypeRelation, startNodeRelation, endNodeRelation) match {
      case (Some(edgeType), Some(startNode), Some(endNode)) =>
        Right(
          EdgeWrite(
            `type` = edgeType,
            space = instanceSpaceExternalId,
            externalId = externalId,
            startNode = startNode,
            endNode = endNode,
            sources = Seq(
              EdgeOrNodeData(
                source = destinationRef,
                properties = Some(props.toMap)
              )
            )
          ))
      case (None, None, None) =>
        Right(
          NodeWrite(
            space = instanceSpaceExternalId,
            externalId = externalId,
            sources = Seq(
              EdgeOrNodeData(
                source = destinationRef,
                properties = Some(props.toMap)
              )
            )
          ))
      case _ =>
        Left(new CdfSparkException(s"""
                                      |Fields 'type', 'externalId', 'startNode' & 'endNode' fields must be present to create an Edge.
                                      |Field 'externalId' is required to create a Node
                                      |Only found: 'externalId', ${Vector(
                                        edgeNodeTypeRelation.map(_ => "'type'"),
                                        startNodeRelation.map(_ => "'startNode'"),
                                        endNodeRelation.map(_ => "'endNode'")).flatten.mkString(", ")}
                                      |data row: ${row.mkString(",")}
                                      |""".stripMargin))
    }

  private def createNodeWriteData(
      instanceSpaceExternalId: String,
      schema: StructType,
      destinationRef: SourceReference,
      propertyDefMap: Map[String, PropertyDefinition],
      rows: Seq[Row]): Either[CdfSparkException, Vector[NodeWrite]] =
    rows.toVector.traverse { row =>
      for {
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(propertyDefMap, schema, row)
      } yield
        NodeWrite(
          space = instanceSpaceExternalId,
          externalId = externalId,
          sources = Seq(
            EdgeOrNodeData(
              source = destinationRef,
              properties = Some(props.toMap)
            )
          )
        )
    }

  private def extractExternalId(schema: StructType, row: Row): Either[CdfSparkException, String] =
    Try {
      Option(row.getString(schema.fieldIndex("externalId")))
    } match {
      case Success(Some(relation)) => Right(relation)
      case Success(None) =>
        Left(
          new CdfSparkException(
            s"""
               |'externalId' shouldn't be null
               |data row: ${row.mkString(",")}
               |""".stripMargin
          ))
      case Failure(err) =>
        Left(new CdfSparkException(s"""
                                      |Couldn't find required string property 'externalId': ${err.getMessage}
                                      |data row: ${row.mkString(",")}
                                      |""".stripMargin))
    }

  private def extractEdgeTypeDirectRelation(
      schema: StructType,
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("type", "Edge type", schema, row)

  private def extractEdgeStartNodeDirectRelation(
      schema: StructType,
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("startNode", "Edge start node", schema, row)

  private def extractEdgeEndNodeDirectRelation(
      schema: StructType,
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("endNode", "Edge end node", schema, row)

  private def extractDirectRelation(
      propertyName: String,
      descriptiveName: String,
      schema: StructType,
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    Try {
      val edgeTypeRow = row.getStruct(schema.fieldIndex(propertyName))
      val space = Option(edgeTypeRow.getAs[String]("space"))
      val externalId = Option(edgeTypeRow.getAs[String]("externalId"))
      Apply[Option].map2(space, externalId)(DirectRelationReference.apply)
    } match {
      case Success(Some(relation)) => Right(relation)
      case Success(None) =>
        Left(
          new CdfSparkException(
            s"""
               |'$propertyName' ($descriptiveName) shouldn't contain null values.
               |Please verify that 'space' & 'externalId' values are not null for '$propertyName'
               |data row: ${row.mkString(",")}
               |""".stripMargin
          ))
      case Failure(err) =>
        Left(new CdfSparkException(s"""
                                      |Couldn't find required property '$propertyName'
                                      |'$propertyName' ($descriptiveName) should be a 'StructType' which consists 'space' & 'externalId' : ${err.getMessage}
                                      |data row: ${row.mkString(",")}
                                      |""".stripMargin))
    }

  private def extractInstancePropertyValues(
      propertyDefMap: Map[String, PropertyDefinition],
      schema: StructType,
      row: Row): Either[CdfSparkException, Vector[(String, InstancePropertyValue)]] =
    propertyDefMap.toVector.flatTraverse {
      case (propName, propDef) =>
        propertyDefinitionToInstancePropertyValue(row, schema, propName, propDef).map {
          case Some(t) => Vector(propName -> t)
          case None => Vector.empty
        }
    }

  private def validateRowFieldsWithPropertyDefinitions(
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition]): Either[CdfSparkException, Boolean] = {

    val (propsExistsInSchema @ _, propsMissingInSchema) = propertyDefMap.partition {
      case (propName, _) => Try(schema.fieldIndex(propName)).isSuccess
    }
    val (nullablePropsMissingInSchema @ _, nonNullablePropsMissingInSchema) =
      propsMissingInSchema.partition { case (_, prop) => prop.nullable.getOrElse(true) }

    val (falselyNullableFieldsInSchema, trulyNullableOrNonNullableFieldsInSchema @ _) =
      propsExistsInSchema.partition {
        case (propName, prop) => prop.nullable.contains(false) && schema(propName).nullable
      }

    if (nonNullablePropsMissingInSchema.nonEmpty) {
      Left(
        new CdfSparkException(
          s"""Can't find required properties: '${nonNullablePropsMissingInSchema.values
               .map(p =>
                 s"${p.name.getOrElse("Unnamed property")}-${p.description.getOrElse("Non described property")}")
               .mkString("', '")}'""".stripMargin
        )
      )
    } else if (falselyNullableFieldsInSchema.nonEmpty) {
      Left(
        new CdfSparkException(
          s"""Properties ['${falselyNullableFieldsInSchema.keys
               .mkString("', '")}'] cannot be nullable""".stripMargin
        )
      )
    } else {
      Right(true)
    }
  }

  private def propertyDefinitionToInstancePropertyValue(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: PropertyDefinition): Either[CdfSparkException, Option[InstancePropertyValue]] = {
    val instancePropertyValueResult = propDef.`type` match {
      case DirectNodeRelationProperty(_) => // TODO: Verify this
        row
          .getSeq[String](schema.fieldIndex(propertyName))
          .toVector
          .traverse(io.circe.parser.parse)
          .map(l => Some(InstancePropertyValue.ObjectList(l)))
          .leftMap(e =>
            new CdfSparkException(
              s"Error parsing value of field '$propertyName' as a list of json objects: ${e.getMessage}"))
      case t if t.isList => toInstantPropertyValueOfList(row, schema, propertyName, propDef)
      case _ => toInstantPropertyValueOfNonList(row, schema, propertyName, propDef)
    }

    instancePropertyValueResult.leftMap {
      case e: CdfSparkException =>
        new CdfSparkException(
          s"""
             |${e.getMessage}
             |table row: ${row.mkString(",")}
             |""".stripMargin
        )
      case e: Throwable =>
        new CdfSparkException(
          s"""
             |Error parsing value of field '$propertyName': ${e.getMessage}
             |table row: ${row.mkString(",")}
             |""".stripMargin
        )
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstantPropertyValueOfList(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: PropertyDefinition): Either[Throwable, Option[InstancePropertyValue]] = {
    val nullable = propDef.nullable.getOrElse(true)
    val fieldIndex = Try(schema.fieldIndex(propertyName))
    val nullAtIndex = fieldIndex.map(row.isNullAt).getOrElse(true)
    if (nullable && nullAtIndex) {
      Right(None)
    } else if (!nullable && nullAtIndex) {
      Left(new CdfSparkException(s"'$propertyName' cannot be null"))
    } else {
      val propVal = fieldIndex.toOption.traverse { i =>
        propDef.`type` match {
          case TextProperty(Some(true), _) =>
            Try(InstancePropertyValue.StringList(row.getSeq[String](i))).toEither
          case PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
            Try(InstancePropertyValue.BooleanList(row.getSeq[Boolean](i))).toEither
          case PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
            Try(
              InstancePropertyValue.Float32List(
                row
                  .getSeq[Any](i)
                  .map(_.asInstanceOf[Number].floatValue))).toEither
          case PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
            Try(
              InstancePropertyValue.Float64List(
                row
                  .getSeq[Any](i)
                  .map(_.asInstanceOf[Number].doubleValue))).toEither
          case PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
            Try(
              InstancePropertyValue.Int32List(
                row
                  .getSeq[Any](i)
                  .map(_.asInstanceOf[Number].intValue))).toEither
          case PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
            Try(
              InstancePropertyValue.Int64List(
                row
                  .getSeq[Any](i)
                  .map(_.asInstanceOf[Number].longValue))).toEither
          case PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
            Try(
              InstancePropertyValue.TimestampList(
                row
                  .getSeq[String](i)
                  .map(ZonedDateTime.parse(_, DateTimeFormatter.ISO_ZONED_DATE_TIME)))).toEither
              .leftMap(e => new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as a list of ISO formatted zoned timestamps: ${e.getMessage}
                                                     |Expected timestamp format is: ${DateTimeFormatter.ISO_ZONED_DATE_TIME.toString}
                                                     |""".stripMargin))
          case PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
            Try(
              InstancePropertyValue.DateList(
                row
                  .getSeq[String](i)
                  .map(LocalDate.parse(_, DateTimeFormatter.ISO_LOCAL_DATE)))).toEither
              .leftMap(e => new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as a list of ISO formatted dates: ${e.getMessage}
                                                     |Expected date format is: ${DateTimeFormatter.ISO_LOCAL_DATE.toString}
                                                     |""".stripMargin))
          case PrimitiveProperty(PrimitivePropType.Json, Some(true)) | DirectNodeRelationProperty(_) =>
            row
              .getSeq[String](i)
              .toVector
              .traverse(io.circe.parser.parse)
              .map(InstancePropertyValue.ObjectList.apply)
              .leftMap(e =>
                new CdfSparkException(
                  s"Error parsing value of field '$propertyName' as a list of json objects: ${e.getMessage}"))

          case t => Left(new CdfSparkException(s"Unhandled list type: ${t.toString}"))
        }
      }
      propVal
    }
  }
  // scalastyle:on cyclomatic.complexity method.length

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstantPropertyValueOfNonList(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: PropertyDefinition): Either[Throwable, Option[InstancePropertyValue]] = {
    val nullable = propDef.nullable.getOrElse(true)
    val fieldIndex = Try(schema.fieldIndex(propertyName))
    val nullAtIndex = fieldIndex.map(row.isNullAt).getOrElse(true)
    if (nullable && nullAtIndex) {
      Right[CdfSparkException, Option[InstancePropertyValue]](None)
    } else if (!nullable && nullAtIndex) {
      Left[CdfSparkException, Option[InstancePropertyValue]](
        new CdfSparkException(s"'$propertyName' cannot be null")
      )
    } else {
      val propVal = fieldIndex.toOption.traverse { i =>
        propDef.`type` match {
          case TextProperty(None | Some(false), _) =>
            Try(InstancePropertyValue.String(row.getString(i))).toEither
          case PrimitiveProperty(PrimitivePropType.Boolean, None | Some(false)) =>
            Try(InstancePropertyValue.Boolean(row.getBoolean(i))).toEither
          case PrimitiveProperty(PrimitivePropType.Float32, None | Some(false)) =>
            Try(
              InstancePropertyValue
                .Float32(row.get(i).asInstanceOf[Number].floatValue)).toEither
          case PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
            Try(InstancePropertyValue.Float64(row.get(i).asInstanceOf[Number].doubleValue)).toEither
          case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) |
              PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
            Try(
              InstancePropertyValue
                .Int64(row.get(i).asInstanceOf[Number].longValue())).toEither
          case PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
            Try(
              InstancePropertyValue.Timestamp(ZonedDateTime
                .parse(row.getString(i), DateTimeFormatter.ISO_ZONED_DATE_TIME))).toEither
              .leftMap(e => new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as a list of ISO formatted zoned timestamps: ${e.getMessage}
                                                     |Expected timestamp format is: ${DateTimeFormatter.ISO_ZONED_DATE_TIME.toString}
                                                     |""".stripMargin))
          case PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
            Try(
              InstancePropertyValue.Date(
                LocalDate.parse(row.getString(i), DateTimeFormatter.ISO_LOCAL_DATE))).toEither
              .leftMap(e => new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as a list of ISO formatted dates: ${e.getMessage}
                                                     |Expected date format is: ${DateTimeFormatter.ISO_LOCAL_DATE.toString}
                                                     |""".stripMargin))
          case PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
            io.circe.parser
              .parse(row
                .getString(i))
              .map(InstancePropertyValue.Object.apply)
              .leftMap(e =>
                new CdfSparkException(
                  s"Error parsing value of field '$propertyName' as a list of json objects: ${e.getMessage}"))

          case t => Left(new CdfSparkException(s"Unhandled non-list type: ${t.toString}"))
        }
      }
      propVal
    }
  }
  // scalastyle:on cyclomatic.complexity method.length
}
