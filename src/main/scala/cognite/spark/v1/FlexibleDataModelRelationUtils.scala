package cognite.spark.v1

import cats.Apply
import cats.implicits._
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{
  DirectNodeRelationProperty,
  PrimitiveProperty,
  TextProperty
}
import com.cognite.sdk.scala.v1.fdm.common.refs.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.{
  DirectRelationReference,
  EdgeOrNodeData,
  InstancePropertyValue,
  NodeOrEdgeCreate
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DecimalType.{
  is32BitDecimalType,
  is64BitDecimalType,
  isByteArrayDecimalType
}
import org.apache.spark.sql.types.{Decimal, DecimalType, StructField, StructType}

import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

object FlexibleDataModelRelationUtils {
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
      propertyDefMap: Map[String, PropertyDefinition],
      destinationRef: SourceReference
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

  // scalastyle:off method.length
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
          )
        )
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
          )
        )
      case _ =>
        val relationRefNames = Vector(
          edgeNodeTypeRelation.map(_ => "'type'"),
          startNodeRelation.map(_ => "'startNode'"),
          endNodeRelation.map(_ => "'endNode'")
        ).flatten
        Left(new CdfSparkException(s"""
                                      |Fields 'type', 'externalId', 'startNode' & 'endNode' fields must be present to create an Edge.
                                      |Field 'externalId' is required to create a Node
                                      |Only found: 'externalId', ${relationRefNames.mkString(", ")}
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
        case (propName, prop) => (prop.nullable contains false) && schema(propName).nullable
      }
// TODO: Verify this

    if (nonNullablePropsMissingInSchema.nonEmpty) {
      val propsAsStr = nonNullablePropsMissingInSchema.keys.mkString(", ")
      Left(new CdfSparkException(s"Can't find required properties: [$propsAsStr]"))
    } else if (falselyNullableFieldsInSchema.nonEmpty) {
      val propsAsStr = falselyNullableFieldsInSchema.keys.mkString(", ")
      Left(
        new CdfSparkException(
          s"""Properties [$propsAsStr] cannot be nullable""".stripMargin
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
            tryAsFloatSeq(row.getSeq[Decimal](i), propertyName).map(InstancePropertyValue.Float32List)
          case PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
            tryAsDoubleSeq(row.getSeq[Decimal](i), propertyName).map(InstancePropertyValue.Float64List)
          case PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
            tryAsIntSeq(row.getSeq[Decimal](i), propertyName).map(InstancePropertyValue.Int32List)
          case PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
            tryAsLongSeq(row.getSeq[Decimal](i), propertyName).map(InstancePropertyValue.Int64List)
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
            tryAsFloat(row.getDecimal(i), propertyName).map(InstancePropertyValue.Float32)
          case PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
            tryAsDouble(row.getDecimal(i), propertyName).map(InstancePropertyValue.Float64)
          case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
            tryAsInt(row.getDecimal(i), propertyName).map(InstancePropertyValue.Int32)
          case PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
            tryAsLong(row.getDecimal(i), propertyName).map(InstancePropertyValue.Int64)
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

  private def tryAsLong(bd: BigDecimal, propertyName: String): Either[CdfSparkException, Long] =
    if (bd.isValidLong) {
      Right(bd.longValue())
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
           |Expecting a Long but found '${bd.toDouble}'
           |""".stripMargin))
    }

  private def tryAsLongSeq(
      decimals: Seq[Decimal],
      propertyName: String): Either[CdfSparkException, Seq[Long]] =
    Try {
      decimals.map { d =>
        val bd = d.toBigDecimal
        if (bd.isValidLong) {
          bd.longValue()
        } else {
          throw new IllegalArgumentException()
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(_) =>
        val seqAsStr = decimals.map(_.toString()).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                                       |Expecting an Array[Long] but found '[$seqAsStr]'
                                                       |""".stripMargin))
    }

  private def tryAsInt(bd: BigDecimal, propertyName: String): Either[CdfSparkException, Int] =
    if (bd.isValidInt) {
      Right(bd.intValue())
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
           |Expecting an Int but found '${bd.toDouble}'
           |""".stripMargin))
    }

  private def tryAsIntSeq(
      decimals: Seq[Decimal],
      propertyName: String): Either[CdfSparkException, Seq[Int]] =
    Try {
      decimals.map { d =>
        val bd = d.toBigDecimal
        if (bd.isValidInt) {
          bd.intValue()
        } else {
          throw new IllegalArgumentException()
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(_) =>
        val seqAsStr = decimals.map(_.toString()).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                      |Expecting an Array[Int] but found '[$seqAsStr]'
                                      |""".stripMargin))
    }

  private def tryAsFloat(bd: BigDecimal, propertyName: String): Either[CdfSparkException, Float] =
    if (bd.isDecimalFloat) {
      Right(bd.floatValue())
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
           |Expecting a Float but found '${bd.toDouble}'
           |""".stripMargin))
    }

  private def tryAsFloatSeq(
      decimals: Seq[Decimal],
      propertyName: String): Either[CdfSparkException, Seq[Float]] =
    Try {
      decimals.map { d =>
        val bd = d.toBigDecimal
        if (bd.isDecimalFloat) {
          bd.floatValue()
        } else {
          throw new IllegalArgumentException()
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(_) =>
        val seqAsStr = decimals.map(_.toString()).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                      |Expecting an Array[Float] but found '[$seqAsStr]'
                                      |""".stripMargin))
    }

  private def tryAsDouble(bd: BigDecimal, propertyName: String): Either[CdfSparkException, Double] =
    if (bd.isDecimalDouble) {
      Right(bd.doubleValue())
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
           |Expecting a Double but found '${bd.toString()}'
           |""".stripMargin))
    }

  private def tryAsDoubleSeq(
      decimals: Seq[Decimal],
      propertyName: String): Either[CdfSparkException, Seq[Double]] =
    Try {
      decimals.map { d =>
        val bd = d.toBigDecimal
        if (bd.isDecimalDouble) {
          bd.doubleValue()
        } else {
          throw new IllegalArgumentException()
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(_) =>
        val seqAsStr = decimals.map(_.toString()).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                      |Expecting an Array[Double] but found '[$seqAsStr]'
                                      |""".stripMargin))
    }
}
