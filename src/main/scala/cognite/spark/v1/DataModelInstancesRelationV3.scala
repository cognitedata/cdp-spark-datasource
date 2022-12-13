package cognite.spark.v1

import cats.Apply
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstancesHelper._
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1
import com.cognite.sdk.scala.v1.DataModelType.NodeType
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefinition,
  PropertyType
}
import com.cognite.sdk.scala.v1.fdm.common.refs.SourceReference
import com.cognite.sdk.scala.v1.fdm.containers._
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{DataModelReference, ViewDefinition, ViewReference}
import com.cognite.sdk.scala.v1.{
  DataModel,
  DataModelIdentifier,
  DataModelInstanceQuery,
  DataModelProperty,
  DataModelPropertyDefinition,
  DataModelType,
  GenericClient,
  PropertyMap
}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time._
import java.time.format.DateTimeFormatter
import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

class DataModelInstancesRelationV3(
    config: RelationConfig,
    space: String,
    viewExternalIdAndVersion: Option[(String, String)],
    containerExternalId: Option[String])(val sqlContext: SQLContext)
    extends CdfRelation(config, "datamodelinstancesV3")
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

  val modelExternalId = ""
  private val model: DataModel = alphaClient.dataModels
    .retrieveByExternalIds(Seq(modelExternalId), spaceExternalId = space)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(
          s"Could not resolve schema of data model $modelExternalId. " +
            s"Got an exception from the CDF API: ${e.message} (code: ${e.code})",
          e)
    }
    .unsafeRunSync()
    .headOption
    // TODO Missing model externalId used to result in CdpApiException, now it returns empty list
    //  Check with dms team
    .getOrElse(throw new CdfSparkException(
      s"Could not resolve schema of data model $modelExternalId. Please check if the model exists."))

  private val modelType = model.dataModelType

  val modelInfo: Map[String, DataModelPropertyDefinition] = {
    val modelProps = model.properties.getOrElse(Map())

    if (modelType == DataModelType.EdgeType) {
      modelProps ++ Map(
        "externalId" -> DataModelPropertyDefinition(v1.PropertyType.Text, false),
        "type" -> DataModelPropertyDefinition(v1.PropertyType.Text, false),
        "startNode" -> DataModelPropertyDefinition(v1.PropertyType.Text, false),
        "endNode" -> DataModelPropertyDefinition(v1.PropertyType.Text, false)
      )
    } else {
      modelProps ++ Map(
        "externalId" -> DataModelPropertyDefinition(v1.PropertyType.Text, false)
      )
    }
  }

  override def schema: StructType = {
    val fields = viewDefinitionAndProperties
      .map {
        case (_, fieldsMap) =>
          fieldsMap.map {
            case (identifier, propType) =>
              StructField(
                identifier,
                convertToSparkDataType(propType.`type`),
                nullable = propType.nullable.getOrElse(true))
          }
      }
      .orElse(containerDefinition.map { container =>
        container.properties.map {
          case (identifier, propType) =>
            DataTypes.createStructField(
              identifier,
              convertToSparkDataType(propType.`type`),
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

    (containerDefWithFirstRow, viewDefAndPropsWithFirstRow) match {
      case (Some((firstRow, container)), None) =>
        createNodesOrEdges(
          rows,
          firstRow.schema,
          ContainerReference(space, container.externalId),
          container.properties,
          container.usedFor).flatMap(results => incMetrics(itemsUpserted, results.length))
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
      case (None, None) if rows.isEmpty => IO.unit
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
      usedFor: ContainerUsage): IO[Seq[SlimNodeOrEdge]] = usedFor match {
    case ContainerUsage.Node => createNodes(rows, schema, propDefMap, sourceReference)
    case ContainerUsage.Edge => createEdges(rows, schema, propDefMap, sourceReference)
    case ContainerUsage.All =>
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
          .map(l => Some(InstancePropertyValue.ObjectsList(l)))
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
          Try(InstancePropertyValue.DoubleList(row.getSeq[Double](schema.fieldIndex(field.name)))).toEither
        case PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
          Try(InstancePropertyValue.DoubleList(row.getSeq[Double](schema.fieldIndex(field.name)))).toEither
        case PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
          Try(InstancePropertyValue.IntegerList(row.getSeq[Long](schema.fieldIndex(field.name)))).toEither
        case PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
          Try(InstancePropertyValue.IntegerList(row.getSeq[Long](schema.fieldIndex(field.name)))).toEither
        case PrimitiveProperty(PrimitivePropType.Numeric, Some(true)) =>
          Try(InstancePropertyValue.DoubleList(row.getSeq[Double](schema.fieldIndex(field.name)))).toEither
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
            .map(InstancePropertyValue.ObjectsList.apply)
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
          Try(InstancePropertyValue.Double(row.getDouble(fieldIndex))).toEither
        case PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
          Try(InstancePropertyValue.Double(row.getDouble(fieldIndex))).toEither
        case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
          Try(InstancePropertyValue.Integer(row.getLong(fieldIndex))).toEither
        case PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
          Try(InstancePropertyValue.Integer(row.getLong(fieldIndex))).toEither
        case PrimitiveProperty(PrimitivePropType.Numeric, None | Some(false)) =>
          Try(InstancePropertyValue.Double(row.getDouble(fieldIndex))).toEither
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

  private def convertToSparkDataType(propType: PropertyType): DataType = {
    def primitivePropTypeToSparkDataType(ppt: PrimitivePropType): DataType = ppt match {
      case PrimitivePropType.Timestamp => DataTypes.TimestampType
      case PrimitivePropType.Date => DataTypes.DateType
      case PrimitivePropType.Boolean => DataTypes.BooleanType
      case PrimitivePropType.Float32 => DataTypes.FloatType
      case PrimitivePropType.Float64 => DataTypes.DoubleType
      case PrimitivePropType.Int32 => DataTypes.IntegerType
      case PrimitivePropType.Int64 => DataTypes.LongType
      case PrimitivePropType.Numeric => DataTypes.DoubleType
      case PrimitivePropType.Json => DataTypes.StringType
    }

    propType match {
      case TextProperty(Some(true), _) => DataTypes.StringType
      case TextProperty(_, _) => DataTypes.createArrayType(DataTypes.StringType)
      case PrimitiveProperty(ppt, Some(true)) =>
        DataTypes.createArrayType(primitivePropTypeToSparkDataType(ppt))
      case PrimitiveProperty(ppt, _) => primitivePropTypeToSparkDataType(ppt)
      case DirectNodeRelationProperty(_) =>
        DataTypes.createArrayType(DataTypes.StringType) // TODO: Verify this
    }
  }

  def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException(
      "Create (abort) is not supported for data model instances. Use upsert instead.")

  def toRow(a: ProjectedDataModelInstance2): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    new GenericRow(a.properties)
  }

  def uniqueId(a: ProjectedDataModelInstance2): String = a.externalId

  // scalastyle:off cyclomatic.complexity
  private def getValue(propName: String, prop: DataModelProperty[_]): Any =
    prop.value match {
      case x: Iterable[_] if (x.size == 2) && (Seq("startNode", "endNode", "type") contains propName) =>
        x.mkString(":")
      case x: java.math.BigDecimal =>
        x.doubleValue
      case x: java.math.BigInteger => x.longValue
      case x: Array[java.math.BigDecimal] =>
        x.toVector.map(i => i.doubleValue)
      case x: Array[java.math.BigInteger] =>
        x.toVector.map(i => i.longValue)
      case x: BigDecimal =>
        x.doubleValue
      case x: BigInt => x.longValue
      case x: Array[BigDecimal] =>
        x.toVector.map(i => i.doubleValue)
      case x: Array[BigInt] =>
        x.toVector.map(i => i.longValue)
      case x: Array[LocalDate] =>
        x.toVector.map(i => java.sql.Date.valueOf(i))
      case x: java.time.LocalDate =>
        java.sql.Date.valueOf(x)
      case x: java.time.ZonedDateTime =>
        java.sql.Timestamp.from(x.toInstant)
      case x: Array[java.time.ZonedDateTime] =>
        x.toVector.map(i => java.sql.Timestamp.from(i.toInstant))
      case _ => prop.value
    }
  // scalastyle:on cyclomatic.complexity

  def toProjectedInstance(
      pmap: PropertyMap,
      requiredPropsArray: Array[String]): ProjectedDataModelInstance2 = {
    val dmiProperties = pmap.allProperties
    ProjectedDataModelInstance2(
      externalId = pmap.externalId,
      properties = requiredPropsArray.map { name: String =>
        dmiProperties.get(name).map(getValue(name, _)).orNull
      }
    )
  }

  // scalastyle:off method.length cyclomatic.complexity
  def getInstanceFilter(sparkFilter: sql.sources.Filter): Option[DomainSpecificLanguageFilter] =
    sparkFilter match {
      case EqualTo(left, right) =>
        Some(DSLEqualsFilter(Seq(space, modelExternalId, left), parsePropertyValue(left, right)))
      case In(attribute, values) =>
        if (modelInfo(attribute).`type`.code.endsWith("[]")) {
          None
        } else {
          val setValues = values.filter(_ != null)
          Some(
            DSLInFilter(
              Seq(space, modelExternalId, attribute),
              setValues.map(parsePropertyValue(attribute, _)).toIndexedSeq))
        }
      case StringStartsWith(attribute, value) =>
        Some(
          DSLPrefixFilter(Seq(space, modelExternalId, attribute), parsePropertyValue(attribute, value)))
      case GreaterThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            gte = Some(parsePropertyValue(attribute, value))))
      case GreaterThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            gt = Some(parsePropertyValue(attribute, value))))
      case LessThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            lte = Some(parsePropertyValue(attribute, value))))
      case LessThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            lt = Some(parsePropertyValue(attribute, value))))
      case And(f1, f2) =>
        (getInstanceFilter(f1) ++ getInstanceFilter(f2)).reduceLeftOption((sf1, sf2) =>
          DSLAndFilter(Seq(sf1, sf2)))
      case Or(f1, f2) =>
        (getInstanceFilter(f1), getInstanceFilter(f2)) match {
          case (Some(sf1), Some(sf2)) =>
            Some(DSLOrFilter(Seq(sf1, sf2)))
          case _ =>
            None
        }
      case IsNotNull(attribute) =>
        Some(DSLExistsFilter(Seq(space, modelExternalId, attribute)))
      case Not(f) =>
        getInstanceFilter(f).map(DSLNotFilter)
      case _ => None
    }
  // scalastyle:on method.length cyclomatic.complexity

  def getStreams(filters: Array[sql.sources.Filter], selectedColumns: Array[String])(
      @nowarn client: GenericClient[IO],
      limit: Option[Int],
      @nowarn numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance2]] = {
    val selectedPropsArray: Array[String] = if (selectedColumns.isEmpty) {
      schema.fields.map(_.name)
    } else {
      selectedColumns
    }

    val filter: DomainSpecificLanguageFilter = {
      val andFilters = filters.toVector.flatMap(getInstanceFilter)
      if (andFilters.isEmpty) EmptyFilter else DSLAndFilter(andFilters)
    }

    // only take the first spaceExternalId and ignore the rest, maybe we can throw an error instead in case there are
    // more than one
    val instanceSpaceExternalIdFilter = filters
      .collectFirst {
        case EqualTo("spaceExternalId", value) => value.toString()
      }
      .getOrElse(space)

    val dmiQuery = DataModelInstanceQuery(
      model = DataModelIdentifier(space = Some(space), model = modelExternalId),
      spaceExternalId = instanceSpaceExternalIdFilter,
      filter = filter,
      sort = None,
      limit = limit,
      cursor = None
    )

    if (modelType == NodeType) {
      Seq(
        alphaClient.nodes
          .queryStream(dmiQuery, limit)
          .map(r => toProjectedInstance(r, selectedPropsArray)))
    } else {
      Seq(
        alphaClient.edges
          .queryStream(dmiQuery, limit)
          .map(r => toProjectedInstance(r, selectedPropsArray)))
    }
  }

  override def buildScan(selectedColumns: Array[String], filters: Array[sql.sources.Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedDataModelInstance2, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedDataModelInstance2, _) => toRow(item),
      uniqueId,
      getStreams(filters, selectedColumns)
    )

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceDeleteSchema](r))
    if (modelType == DataModelType.NodeType) {
      alphaClient.nodes
        .deleteItems(deletes.map(_.externalId), space)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    } else {
      alphaClient.edges
        .deleteItems(deletes.map(_.externalId), space)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    }
  }

  def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for data model instances. Use upsert instead.")

}

final case class ProjectedDataModelInstance2(externalId: String, properties: Array[Any])
final case class DataModelInstanceDeleteSchema2(spaceExternalId: Option[String], externalId: String)
