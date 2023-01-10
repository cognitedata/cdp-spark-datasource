package cognite.spark.v1

import cats.Apply
import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterValueDefinition.ComparableFilterValue
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefinition,
  PropertyType
}
import com.cognite.sdk.scala.v1.fdm.common.refs.SourceReference
import com.cognite.sdk.scala.v1.fdm.containers._
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.{
  EdgeDeletionRequest,
  NodeDeletionRequest
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{DataModelReference, ViewDefinition, ViewReference}
import com.cognite.sdk.scala.v1.resources.fdm.instances.Instances.instanceCreateEncoder
import io.circe.syntax.EncoderOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time._
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// scalastyle:off number.of.methods
class DataModelInstancesRelationV3(
    config: RelationConfig,
    space: String,
    viewExternalIdAndVersion: Option[(String, String)],
    containerExternalId: Option[String])(val sqlContext: SQLContext)
    extends CdfRelation(config, DataModelInstancesRelationV3.ResourceType)
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._

  private val viewDefinitionAndProperties
    : Option[(ViewDefinition, Map[String, ViewPropertyDefinition])] =
    viewExternalIdAndVersion
      .flatTraverse {
        case (viewExtId, viewVersion) =>
          val allProperties = alphaClient.views
            .retrieveItems(
              Seq(DataModelReference(space, viewExtId, viewVersion)),
              includeInheritedProperties = Some(true))
            .map(_.headOption)
            .flatMap {
              case Some(viewDef) =>
                viewDef.implements.traverse { v =>
                  alphaClient.views
                    .retrieveItems(v.map(vRef =>
                      DataModelReference(vRef.space, vRef.externalId, vRef.version)))
                    .map { inheritingViews =>
                      val inheritingProperties = inheritingViews
                        .map(_.properties)
                        .reduce((propMap1, propMap2) => propMap1 ++ propMap2)

                      (viewDef, inheritingProperties ++ viewDef.properties)
                    }
                }
              case None => IO.pure(None)
            }
          allProperties
      }
      .unsafeRunSync()

  private val containerDefinition: Option[ContainerDefinition] =
    containerExternalId
      .flatTraverse { extId =>
        alphaClient.containers
          .retrieveByExternalIds(Seq(ContainerId(space, extId)))
          .map(_.headOption)
      }
      .unsafeRunSync()

  override def schema: StructType = {
    val fields = viewDefinitionAndProperties
      .map {
        case (_, fieldsMap) =>
          fieldsMap.map {
            case (identifier, propType) =>
              DataTypes.createStructField(
                identifier,
                convertToSparkDataType(propType.`type`, propType.nullable.getOrElse(true)),
                propType.nullable.getOrElse(true))
          }
      }
      .orElse(containerDefinition.map { containerDef =>
        containerDef.properties.map {
          case (identifier, propType) =>
            DataTypes.createStructField(
              identifier,
              convertToSparkDataType(propType.`type`, propType.nullable.getOrElse(true)),
              propType.nullable.getOrElse(true))
        }
      })
      .getOrElse {
        throw new CdfSparkException(s"""
                                       |Correct (view external id, view version) pair or container external id should be specified.
                                       |Could not resolve schema from view external id: $viewExternalIdAndVersion or container external id: $containerExternalId
                                       |""".stripMargin)
      }
    DataTypes.createStructType(fields.toArray)
  }

  // scalastyle:off cyclomatic.complexity
  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val viewDefAndPropsWithFirstRow = Apply[Option]
      .map2(rows.headOption, viewDefinitionAndProperties) {
        case (firstRow, (viewDef, viewProps)) => Tuple3(firstRow, viewDef, viewProps)
      }
    val containerDefWithFirstRow = Apply[Option]
      .map2(rows.headOption, containerDefinition)(Tuple2.apply)

    println(rows.length)
    println(rows.map(_.prettyJson).mkString(System.lineSeparator()))
    (containerDefWithFirstRow, viewDefAndPropsWithFirstRow) match {
      case (Some((firstRow, containerDef)), None) =>
        createNodesOrEdges(
          rows,
          firstRow.schema,
          containerDef.toContainerReference,
          containerDef.properties,
          containerDef.usedFor).flatMap(results => incMetrics(itemsUpserted, results.length))
      case (None, Some((firstRow, viewDef, viewProps))) =>
        createNodesOrEdges(
          rows,
          firstRow.schema,
          ViewReference(space, viewDef.externalId, viewDef.version),
          viewProps,
          viewDef.usedFor).flatMap(results => incMetrics(itemsUpserted, results.length))
      case (Some(_), Some(_)) =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"Either a correct (view external id, view version) pair or a container external id should be specified, not both"
          ))
      case (None, None) if rows.isEmpty => incMetrics(itemsUpserted, 0)
      case (None, None) if containerExternalId.nonEmpty =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"""|Either a correct (view external id, view version) pair or a container external id should be specified.
                |Could not resolve container external id: $containerExternalId
                |""".stripMargin
          ))
      case (None, None) if viewExternalIdAndVersion.nonEmpty =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"""|Either a correct (view external id, view version) pair or a container external id should be specified.
                |Could not resolve (view external id, view version) : $viewExternalIdAndVersion
                |""".stripMargin
          ))
      case (None, None) =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"Either a correct (view external id, view version) pair or a container external id should be specified"
          ))
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def createNodesOrEdges(
      rows: Seq[Row],
      schema: StructType,
      sourceReference: SourceReference,
      propDefMap: Map[String, PropertyDefinition],
      usedFor: Usage): IO[Seq[SlimNodeOrEdge]] = usedFor match {
    case Usage.Node => createNodes(rows, schema, propDefMap, sourceReference)
    case Usage.Edge => createEdges(rows, schema, propDefMap, sourceReference)
    case Usage.All =>
      verifyPrerequisitesForEdges(schema) match {
        case Failure(errForEdge) =>
          verifyPrerequisitesForNodes(schema) match {
            case Failure(errForNode) =>
              IO.raiseError[Seq[SlimNodeOrEdge]](
                new CdfSparkException(s"""|Cannot be considered as Edges or Nodes.
                                          |Fields 'type', 'externalId', 'startNode' & 'endNode' fields must be present to create an Edge: ${errForEdge.getMessage}"
                                          |Field 'externalId' is required to create a Node: ${errForNode.getMessage}
                                          |""".stripMargin)
              )
            case Success(_) =>
              createNodes(rows, schema, propDefMap, sourceReference)
          }
        case Success(_) =>
          createEdges(rows, schema, propDefMap, sourceReference)
      }
      createNodes(rows, schema, propDefMap, sourceReference)
  }

  private def createNodes(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      destinationRef: SourceReference): IO[Seq[SlimNodeOrEdge]] =
    verifyPrerequisitesForNodes(schema) *> validateRowFieldsWithPropertyDefinitions(
      schema,
      propertyDefMap) match {
      case Success(_) =>
        createNodeWriteData(schema, destinationRef, propertyDefMap, rows) match {
          case Left(err) => IO.raiseError(err)
          case Right(items) =>
            val instanceCreate = InstanceCreate(
              items = items,
              autoCreateStartNodes = Some(true),
              autoCreateEndNodes = Some(true),
              replace = Some(false) // TODO: verify this
            )

            println(instanceCreate.asJson.noSpaces)
            alphaClient.instances.createItems(instanceCreate)
        }
      case Failure(err) =>
        IO.raiseError[Seq[SlimNodeOrEdge]](
          new CdfSparkException(s"Field 'externalId' is required to create a Node: ${err.getMessage}"))
    }

  private def createEdges(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      destinationRef: SourceReference): IO[Seq[SlimNodeOrEdge]] =
    verifyPrerequisitesForEdges(schema) *> validateRowFieldsWithPropertyDefinitions(
      schema,
      propertyDefMap) match {
      case Success(_) =>
        createEdgeWriteData(schema, destinationRef, propertyDefMap, rows) match {
          case Left(err) => IO.raiseError(err)
          case Right(items) =>
            val instanceCreate = InstanceCreate(
              items = items,
              autoCreateStartNodes = Some(true),
              autoCreateEndNodes = Some(true),
              replace = Some(false) // TODO: verify this
            )
            println(instanceCreate.asJson.noSpaces)
            alphaClient.instances.createItems(instanceCreate)
        }
      case Failure(err) =>
        IO.raiseError[Seq[SlimNodeOrEdge]](new CdfSparkException(
          s"Fields 'type', 'externalId', 'startNode' & 'endNode' fields must be present to create an Edge: ${err.getMessage}"))
    }

  private def verifyPrerequisitesForEdges(schema: StructType): Try[Boolean] =
    Try {
      (
        schema.fieldIndex("type"),
        schema.fieldIndex("externalId"),
        schema.fieldIndex("startNode"),
        schema.fieldIndex("endNode")
      )
    } *> Success(true)

  private def verifyPrerequisitesForNodes(schema: StructType): Try[Boolean] =
    Try(schema.fieldIndex("externalId")) *> Success(true)

  private def validateRowFieldsWithPropertyDefinitions(
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition]): Try[Boolean] = {

    val (existsInPropDef @ _, missingInPropDef) = schema.fields.partition { field =>
      propertyDefMap.contains(field.name)
    }

    val (nonNullableMissingFields, nullableMissingFields @ _) = missingInPropDef.partition { field =>
      propertyDefMap.get(field.name).flatMap(_.nullable).contains(false)
    }

    if (nonNullableMissingFields.nonEmpty) {
      Failure(
        new CdfSparkException(
          s"""Can't find required properties: ${nonNullableMissingFields
               .map(_.name)
               .mkString(", ")}""".stripMargin
        )
      )
    } else {
      Success(true)
    }
  }

  private def createNodeWriteData(
      schema: StructType,
      destinationRef: SourceReference,
      propertyDefMap: Map[String, PropertyDefinition],
      rows: Seq[Row]): Either[CdfSparkException, List[NodeWrite]] =
    rows.toList.traverse { row =>
      val properties = schema.fields.toList.flatTraverse { field =>
        propertyDefMap.get(field.name).toList.flatTraverse { propDef =>
          propertyDefinitionToInstancePropertyValue(row, field, schema, propDef).map {
            case Some(t) => List(field.name -> t)
            case None => List.empty
          }
        }
      }
      properties.map { props =>
        NodeWrite(
          space = space,
          externalId = row.getString(schema.fieldIndex("externalId")),
          sources = Seq(
            EdgeOrNodeData(
              source = destinationRef,
              properties = Some(props.toMap)
            )
          )
        )
      }
    }

  private def createEdgeWriteData(
      schema: StructType,
      destinationRef: SourceReference,
      propertyDefMap: Map[String, PropertyDefinition],
      rows: Seq[Row]): Either[CdfSparkException, List[EdgeWrite]] =
    rows.toList.traverse { row =>
      val edgeNodeTypeRelation = extractEdgeTypeDirectRelation(row)
      val startNodeRelation = extractEdgeStartNodeDirectRelation(row)
      val endNodeRelation = extractEdgeEndNodeDirectRelation(row)
      val properties = schema.fields.toList.flatTraverse { field =>
        propertyDefMap.get(field.name).toList.flatTraverse { propDef =>
          propertyDefinitionToInstancePropertyValue(row, field, schema, propDef).map {
            case Some(t) => List(field.name -> t)
            case None => List.empty
          }
        }
      }

      for {
        edgeType <- edgeNodeTypeRelation
        startNode <- startNodeRelation
        endNode <- endNodeRelation
        props <- properties
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = space,
          externalId = row.getString(schema.fieldIndex("externalId")),
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

  private def extractEdgeTypeDirectRelation(
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("type", "Edge type", row)

  private def extractEdgeStartNodeDirectRelation(
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("startNode", "Edge start node", row)

  private def extractEdgeEndNodeDirectRelation(
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("endNode", "Edge end node", row)

  private def extractDirectRelation(
      propertyName: String,
      descriptiveName: String,
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
             |data row: ${row.json}
             |""".stripMargin
          ))
      case Failure(err) =>
        Left(new CdfSparkException(s"""
             |Couldn't find '$propertyName'. '$propertyName' ($descriptiveName) should be a 'StructType' which consists 'space' & 'externalId' : ${err.getMessage}
             |data row: ${row.json}
             |""".stripMargin))
    }

  private def propertyDefinitionToInstancePropertyValue(
      row: Row,
      field: StructField,
      schema: StructType,
      propDef: PropertyDefinition): Either[CdfSparkException, Option[InstancePropertyValue]] = {
    val instancePropertyValueResult = propDef.`type` match {
      case DirectNodeRelationProperty(_) => // TODO: Verify this
        row
          .getSeq[String](schema.fieldIndex(field.name))
          .toList
          .traverse(io.circe.parser.parse)
          .map(l => Some(InstancePropertyValue.ObjectList(l)))
          .leftMap(e =>
            new CdfSparkException(
              s"Error parsing value of field '${field.name}' as a list of json objects: ${e.getMessage}"))
      case t if t.isList => toInstantPropertyValueOfList(row, field, schema, propDef)
      case _ => toInstantPropertyValueOfNonList(row, field, schema, propDef)
    }

    instancePropertyValueResult.leftMap {
      case e: CdfSparkException =>
        new CdfSparkException(
          s"""
             |${e.getMessage}
             |table row: ${row.json}
             |""".stripMargin
        )
      case e: Throwable =>
        new CdfSparkException(
          s"""
             |Error parsing value of field '${field.name}': ${e.getMessage}
             |table row: ${row.json}
             |""".stripMargin
        )
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstantPropertyValueOfList(
      row: Row,
      field: StructField,
      schema: StructType,
      propDef: PropertyDefinition): Either[Throwable, Option[InstancePropertyValue]] = {
    val nullable = propDef.nullable.getOrElse(true)
    val fieldIndex = schema.fieldIndex(field.name)
    if (nullable && row.isNullAt(fieldIndex)) {
      Right(None)
    } else if (!nullable && row.isNullAt(fieldIndex)) {
      Left(
        new CdfSparkException(
          s"'${field.name}' cannot be null. Consider replacing null with a default value"))
    } else {
      val propVal = propDef.`type` match {
        case TextProperty(Some(true), _) =>
          Try(InstancePropertyValue.StringList(row.getSeq[String](schema.fieldIndex(field.name)))).toEither
        case PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
          Try(InstancePropertyValue.BooleanList(row.getSeq[Boolean](schema.fieldIndex(field.name)))).toEither
        case PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
          Try(
            InstancePropertyValue.Float32List(
              row
                .getSeq[Any](schema.fieldIndex(field.name))
                .map(_.asInstanceOf[java.lang.Number].floatValue))).toEither
        case PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
          Try(
            InstancePropertyValue.Float64List(
              row
                .getSeq[Any](schema.fieldIndex(field.name))
                .map(_.asInstanceOf[java.lang.Number].doubleValue))).toEither
        case PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
          Try(
            InstancePropertyValue.Int32List(
              row
                .getSeq[Any](schema.fieldIndex(field.name))
                .map(_.asInstanceOf[java.lang.Number].intValue))).toEither
        case PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
          Try(
            InstancePropertyValue.Int64List(
              row
                .getSeq[Any](schema.fieldIndex(field.name))
                .map(_.asInstanceOf[java.lang.Number].longValue))).toEither
        case PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
          Try(
            InstancePropertyValue.TimestampList(
              row
                .getSeq[String](schema.fieldIndex(field.name))
                .map(ZonedDateTime.parse(_, DateTimeFormatter.ISO_ZONED_DATE_TIME)))).toEither
            .leftMap(e => new CdfSparkException(s"""
                                                   |Error parsing value of field '${field.name}' as a list of ISO formatted zoned timestamps: ${e.getMessage}
                                                   |Expected timestamp format is: ${DateTimeFormatter.ISO_ZONED_DATE_TIME.toString}
                                                   |""".stripMargin))
        case PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
          Try(
            InstancePropertyValue.DateList(
              row
                .getSeq[String](schema.fieldIndex(field.name))
                .map(LocalDate.parse(_, DateTimeFormatter.ISO_LOCAL_DATE)))).toEither
            .leftMap(e => new CdfSparkException(s"""
                                                   |Error parsing value of field '${field.name}' as a list of ISO formatted dates: ${e.getMessage}
                                                   |Expected date format is: ${DateTimeFormatter.ISO_LOCAL_DATE.toString}
                                                   |""".stripMargin))
        case PrimitiveProperty(PrimitivePropType.Json, Some(true)) | DirectNodeRelationProperty(_) =>
          row
            .getSeq[String](schema.fieldIndex(field.name))
            .toList
            .traverse(io.circe.parser.parse)
            .map(InstancePropertyValue.ObjectList.apply)
            .leftMap(e =>
              new CdfSparkException(
                s"Error parsing value of field '${field.name}' as a list of json objects: ${e.getMessage}"))

        case t => Left(new CdfSparkException(s"Unhandled list type: ${t.toString}"))
      }
      propVal.map(Some(_))
    }
  }
  // scalastyle:on cyclomatic.complexity method.length

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstantPropertyValueOfNonList(
      row: Row,
      field: StructField,
      schema: StructType,
      propDef: PropertyDefinition): Either[Throwable, Option[InstancePropertyValue]] = {
    val nullable = propDef.nullable.getOrElse(true)
    val fieldIndex = schema.fieldIndex(field.name)
    if (nullable && row.isNullAt(fieldIndex)) {
      Right[CdfSparkException, Option[InstancePropertyValue]](None)
    } else if (!nullable && row.isNullAt(fieldIndex)) {
      Left[CdfSparkException, Option[InstancePropertyValue]](
        new CdfSparkException(
          s"'${field.name}' cannot be null. Consider replacing null with a default value")
      )
    } else {
      val propVal = propDef.`type` match {
        case TextProperty(None | Some(false), _) =>
          Try(InstancePropertyValue.String(row.getString(fieldIndex))).toEither
        case PrimitiveProperty(PrimitivePropType.Boolean, None | Some(false)) =>
          Try(InstancePropertyValue.Boolean(row.getBoolean(fieldIndex))).toEither
        case PrimitiveProperty(PrimitivePropType.Float32, None | Some(false)) =>
          Try(
            InstancePropertyValue
              .Float32(row.get(fieldIndex).asInstanceOf[java.lang.Number].floatValue)).toEither
        case PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
          Try(
            InstancePropertyValue.Float64(
              row.get(fieldIndex).asInstanceOf[java.lang.Number].doubleValue)).toEither
        case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) |
            PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
          Try(
            InstancePropertyValue
              .Int64(row.get(fieldIndex).asInstanceOf[java.lang.Number].longValue())).toEither
        case PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
          Try(
            InstancePropertyValue.Timestamp(ZonedDateTime
              .parse(row.getString(fieldIndex), DateTimeFormatter.ISO_ZONED_DATE_TIME))).toEither
            .leftMap(e => new CdfSparkException(s"""
                                                   |Error parsing value of field '${field.name}' as a list of ISO formatted zoned timestamps: ${e.getMessage}
                                                   |Expected timestamp format is: ${DateTimeFormatter.ISO_ZONED_DATE_TIME.toString}
                                                   |""".stripMargin))
        case PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
          Try(
            InstancePropertyValue.Date(
              LocalDate.parse(row.getString(fieldIndex), DateTimeFormatter.ISO_LOCAL_DATE))).toEither
            .leftMap(e => new CdfSparkException(s"""
                                                   |Error parsing value of field '${field.name}' as a list of ISO formatted dates: ${e.getMessage}
                                                   |Expected date format is: ${DateTimeFormatter.ISO_LOCAL_DATE.toString}
                                                   |""".stripMargin))
        case PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
          io.circe.parser
            .parse(row
              .getString(fieldIndex))
            .map(InstancePropertyValue.Object.apply)
            .leftMap(e =>
              new CdfSparkException(
                s"Error parsing value of field '${field.name}' as a list of json objects: ${e.getMessage}"))

        case t => Left(new CdfSparkException(s"Unhandled non-list type: ${t.toString}"))
      }
      propVal.map(Some(_))
    }
  }
  // scalastyle:on cyclomatic.complexity method.length

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

  def valueToFilterValueDef(
      attribute: String,
      value: Any): Either[CdfSparkException, FilterValueDefinition] =
    value match {
//      case v: String =>
//        io.circe.parser.parse(v) match {
//          case Right(json) if json.isObject => Some(FilterValueDefinition.Object(json))
//          case _ => Some(FilterValueDefinition.String(v))
//        }
      case v: String => Right(FilterValueDefinition.String(v))
      case v: scala.Double => Right(FilterValueDefinition.Number(v))
      case v: scala.Int => Right(FilterValueDefinition.Integer(v.toLong))
      case v: scala.Long => Right(FilterValueDefinition.Integer(v))
      case v: scala.Boolean => Right(FilterValueDefinition.Boolean(v))
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a String, Number, Boolean or Object for '$attribute', but found ${v.toString}"
          )
        )
    }

  def valueToComparableFilterValueDef(
      attribute: String,
      value: Any): Either[CdfSparkException, ComparableFilterValue] =
    value match {
      case v: String => Right(FilterValueDefinition.String(v))
      case v: scala.Double => Right(FilterValueDefinition.Number(v))
      case v: scala.Int => Right(FilterValueDefinition.Integer(v.toLong))
      case v: scala.Long => Right(FilterValueDefinition.Integer(v))
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a comparable value(Number or String) for '$attribute', but found ${v.toString}"
          )
        )
    }

  def valueToFilterValueDefSeq(
      attribute: String,
      value: Any): Either[CdfSparkException, Seq[FilterValueDefinition]] =
    value match {
      //      case v: Array[String] =>
      //        v.headOption.flatMap(s => io.circe.parser.parse(s).toOption) match {
      //          case Some(_) => Some(v.map(FilterValueDefinition.Object.apply))
      //          case None => Some(v.map(FilterValueDefinition.String.apply))
      //        }
      case v: Array[Double] => Right(v.map(FilterValueDefinition.Number.apply))
      case v: Array[Float] => Right(v.map(f => FilterValueDefinition.Number(f.toDouble)))
      case v: Array[Int] => Right(v.map(i => FilterValueDefinition.Integer.apply(i.toLong)))
      case v: Array[Long] => Right(v.map(FilterValueDefinition.Integer.apply))
      case v: Array[String] => Right(v.map(FilterValueDefinition.String.apply))
      case v: Array[Boolean] => Right(v.map(FilterValueDefinition.Boolean.apply))
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting an Array of String, Number, Boolean or Object for '$attribute', but found ${v.toString}"
          )
        )
    }

  // scalastyle:off cyclomatic.complexity
  def getInstanceFilter(sparkFilter: sql.sources.Filter): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) =>
        valueToFilterValueDef(attribute, value).map(FilterDefinition.Equals(Seq(attribute), _))
      case In(attribute, values) =>
        valueToFilterValueDefSeq(attribute, values).map(FilterDefinition.In(Seq(attribute), _))
      case StringStartsWith(attribute, value) =>
        Right(FilterDefinition.Prefix(Seq(attribute), FilterValueDefinition.String(value)))
      case GreaterThanOrEqual(attribute, value) =>
        valueToComparableFilterValueDef(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), gte = Some(f)))
      case GreaterThan(attribute, value) =>
        valueToComparableFilterValueDef(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), gt = Some(f)))
      case LessThanOrEqual(attribute, value) =>
        valueToComparableFilterValueDef(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), lte = Some(f)))
      case LessThan(attribute, value) =>
        valueToComparableFilterValueDef(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(attribute), lt = Some(f)))
      case And(f1, f2) =>
        List(f1, f2).traverse(getInstanceFilter).map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        List(f1, f2).traverse(getInstanceFilter).map(FilterDefinition.Or.apply)
      case IsNotNull(attribute) => Right(FilterDefinition.Exists(Seq(attribute)))
      case Not(f) => getInstanceFilter(f).map(FilterDefinition.Not.apply)
      case f => Left(new CdfSparkIllegalArgumentException(s"Unsupported filter '${f.toString}'"))
    }
  // scalastyle:on cyclomatic.complexity

//  def getStreams(filters: Array[sql.sources.Filter], selectedColumns: Array[String])(
//      @nowarn client: GenericClient[IO],
//      limit: Option[Int],
//      @nowarn numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstanceV3]] = {
//    val selectedPropsArray = if (selectedColumns.isEmpty) {
//      schema.fieldNames
//    } else {
//      selectedColumns
//    }
//
//    val filter = filters.toList.traverse(getInstanceFilter)
//
//    val spaceFilters = (filters.map {
//      case EqualTo("space", value) => value.toString
//    } ++ Array(space)).distinct
//
//    val dmiQuery = InstanceFilterRequest(
//      model = DataModelIdentifier(space = Some(space), model = modelExternalId),
//      spaces = Some(spaceFilters),
//      filter = filter,
//      sort = None,
//      limit = limit,
//      cursor = None,
//      instanceType = None
//    )
//
//    if (modelType == NodeType) {
//      Seq(
//        client.nodes
//          .queryStream(dmiQuery, limit)
//          .map(r => toProjectedInstance(r, selectedPropsArray)))
//    } else {
//      Seq(
//        client.edges
//          .queryStream(dmiQuery, limit)
//          .map(r => toProjectedInstance(r, selectedPropsArray)))
//    }
//  }

  override def buildScan(selectedColumns: Array[String], filters: Array[sql.sources.Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedDataModelInstanceV3, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedDataModelInstanceV3, _) => toRow(item),
      _.externalId,
      (_, _, _) => Seq.empty //getStreams(filters, selectedColumns)
    )

  // scalastyle:off cyclomatic.complexity
  override def delete(rows: Seq[Row]): IO[Unit] =
    (containerDefinition, viewDefinitionAndProperties) match {
      case (Some(containerDef), None) if containerDef.usedFor == Usage.Edge =>
        deleteEdgesWithMetrics(rows)

      case (Some(containerDef), None) if containerDef.usedFor == Usage.Node =>
        deleteNodesWithMetrics(rows)

      case (Some(containerDef), None) if containerDef.usedFor == Usage.All =>
        deleteNodesOrEdgesWithMetrics(rows, s"Container with externalId: ${containerDef.externalId}")

      case (None, Some((viewDef, _))) if viewDef.usedFor == Usage.Edge => deleteEdgesWithMetrics(rows)

      case (None, Some((viewDef, _))) if viewDef.usedFor == Usage.Node => deleteNodesWithMetrics(rows)

      case (None, Some((viewDef, _))) if viewDef.usedFor == Usage.All =>
        deleteNodesOrEdgesWithMetrics(
          rows,
          s"View with externalId: ${viewDef.externalId} & version: ${viewDef.version}")

      case (Some(_), Some(_)) =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"Either a correct (view external id, view version) pair or a container external id should be specified, not both"
          ))
      case (None, None) if rows.isEmpty => incMetrics(itemsDeleted, 0)
      case (None, None) if containerExternalId.nonEmpty =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"""|Either a correct (view external id, view version) pair or a container external id should be specified.
                |Could not resolve container external id: $containerExternalId
                |""".stripMargin
          ))
      case (None, None) if viewExternalIdAndVersion.nonEmpty =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"""|Either a correct (view external id, view version) pair or a container external id should be specified.
                |Could not resolve (view external id, view version) : $viewExternalIdAndVersion
                |""".stripMargin
          ))
      case (None, None) =>
        IO.raiseError[Unit](
          new CdfSparkException(
            s"Either a correct (view external id, view version) pair or a container external id should be specified"
          ))
    }
  // scalastyle:on cyclomatic.complexity

  private def deleteNodesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceV3DeleteModel](r))
    alphaClient.instances
      .delete(deleteCandidates.map(i => NodeDeletionRequest(i.space.getOrElse(space), i.externalId)))
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  private def deleteEdgesWithMetrics(rows: Seq[Row]): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceV3DeleteModel](r))
    alphaClient.instances
      .delete(deleteCandidates.map(i => EdgeDeletionRequest(i.space.getOrElse(space), i.externalId)))
      .flatMap(results => incMetrics(itemsDeleted, results.length))
  }

  private def deleteNodesOrEdgesWithMetrics(rows: Seq[Row], errorMsgPrefix: String): IO[Unit] = {
    val deleteCandidates =
      rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceV3DeleteModel](r))
    alphaClient.instances
      .delete(deleteCandidates.map(i => NodeDeletionRequest(i.space.getOrElse(space), i.externalId)))
      .recoverWith {
        case NonFatal(nodeDeletionErr) =>
          alphaClient.instances
            .delete(deleteCandidates.map(i =>
              EdgeDeletionRequest(i.space.getOrElse(space), i.externalId)))
            .handleErrorWith {
              case NonFatal(edgeDeletionErr) =>
                IO.raiseError(
                  new CdfSparkException(
                    s"""
                       |$errorMsgPrefix supports both Nodes & Edges
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

  def update(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException("Update is not supported for data model instances. Use upsert instead."))

}

object DataModelInstancesRelationV3 {
  val ResourceType = "datamodelinstancesV3"
}

final case class ProjectedDataModelInstanceV3(externalId: String, properties: Array[Any])
final case class DataModelInstanceV3DeleteModel(space: Option[String], externalId: String)
