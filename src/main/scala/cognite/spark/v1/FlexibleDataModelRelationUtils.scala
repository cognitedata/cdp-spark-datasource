package cognite.spark.v1

import cats.Apply
import cats.implicits._
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{
  DirectNodeRelationProperty,
  PrimitiveProperty,
  TextProperty
}
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{
  DirectRelationReference,
  EdgeOrNodeData,
  InstancePropertyValue,
  NodeOrEdgeCreate
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.time.{LocalDate, ZonedDateTime}
import scala.util.{Failure, Success, Try}

object FlexibleDataModelRelationUtils {
  private[spark] def createNodes(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      source: SourceReference): Either[CdfSparkException, Vector[NodeWrite]] =
    validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap) *> createNodeWriteData(
      instanceSpaceExternalId,
      schema,
      source,
      propertyDefMap,
      rows)

  private[spark] def createEdges(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      source: SourceReference): Either[CdfSparkException, Vector[EdgeWrite]] =
    validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap) *> createEdgeWriteData(
      instanceSpaceExternalId,
      schema,
      source,
      propertyDefMap,
      rows)

  private[spark] def createNodesOrEdges(
      instanceSpaceExternalId: String,
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      source: SourceReference
  ): Either[CdfSparkException, Vector[NodeOrEdgeCreate]] =
    rows.toVector.traverse { row =>
      for {
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(propertyDefMap, schema, row)
        writeData <- createNodeOrEdgeWriteData(
          externalId = externalId,
          instanceSpaceExternalId = instanceSpaceExternalId,
          source,
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
      source: SourceReference,
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
              source = source,
              properties = Some(props.toMap)
            )
          )
        )
    }

  // scalastyle:off method.length
  private def createNodeOrEdgeWriteData(
      externalId: String,
      instanceSpaceExternalId: String,
      source: SourceReference,
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
                source = source,
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
                source = source,
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
                                      |Fields 'type', 'externalId', 'startNode' & 'endNode' fields are required to create an Edge.
                                      |Field 'externalId' is required to create a Node
                                      |Only found: 'externalId', ${relationRefNames.mkString(", ")}
                                      |in data row: ${rowToString(row)}
                                      |""".stripMargin))
    }

  private def createNodeWriteData(
      instanceSpaceExternalId: String,
      schema: StructType,
      source: SourceReference,
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
              source = source,
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
               |'externalId' cannot be null
               |in data row: ${rowToString(row)}
               |""".stripMargin
          ))
      case Failure(err) =>
        Left(new CdfSparkException(s"""
                                      |Couldn't find required string property 'externalId': ${err.getMessage}
                                      |in data row: ${rowToString(row)}
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
               |'$propertyName' ($descriptiveName) cannot contain null values.
               |Please verify that 'space' & 'externalId' values are not null for '$propertyName'
               |in data row: ${rowToString(row)}
               |""".stripMargin
          ))
      case Failure(err) =>
        Left(new CdfSparkException(s"""
                                      |Could not find required property '$propertyName'
                                      |'$propertyName' ($descriptiveName) should be a 'StructType' with 'space' & 'externalId' properties: ${err.getMessage}
                                      |in data row: ${rowToString(row)}
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

    if (nonNullablePropsMissingInSchema.nonEmpty) {
      val propsAsStr = nonNullablePropsMissingInSchema.keys.mkString(", ")
      Left(new CdfSparkException(s"Could not find required properties: [$propsAsStr]"))
    } else if (falselyNullableFieldsInSchema.nonEmpty) {
      val propsAsStr = falselyNullableFieldsInSchema.keys.mkString(", ")
      Left(
        new CdfSparkException(
          s"""Properties [$propsAsStr] cannot contain null values
             |Please verify your data!
             |""".stripMargin
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
        skipNulls(row.getSeq[String](schema.fieldIndex(propertyName))).toVector
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
             |for data row: ${rowToString(row)}
             |""".stripMargin
        )
      case e: Throwable =>
        new CdfSparkException(
          s"""
             |Error parsing value of field '$propertyName': ${e.getMessage}
             |for data row: ${rowToString(row)}
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
            Try(InstancePropertyValue.StringList(skipNulls(row.getSeq[String](i)))).toEither
          case PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
            Try(InstancePropertyValue.BooleanList(skipNulls(row.getSeq[Boolean](i)))).toEither
          case PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
            val floatSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
            tryAsFloatSeq(floatSeq, propertyName)
              .map(InstancePropertyValue.Float32List)
          case PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
            val doubleSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
            tryAsDoubleSeq(doubleSeq, propertyName)
              .map(InstancePropertyValue.Float64List)
          case PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
            val intSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
            tryAsIntSeq(intSeq, propertyName)
              .map(InstancePropertyValue.Int32List)
          case PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
            val longSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
            tryAsLongSeq(longSeq, propertyName)
              .map(InstancePropertyValue.Int64List)
          case PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
            val formatter = InstancePropertyValue.Timestamp.formatter
            val strSeq = Try(row.getSeq[String](i)).getOrElse(row.getAs[Array[String]](i).toSeq)
            Try(
              InstancePropertyValue.TimestampList(skipNulls(strSeq)
                .map(ZonedDateTime.parse(_, formatter)))).toEither
              .leftMap { e =>
                val exampleTimestamps = Vector(
                  "2023-01-17T20:39:57Z",
                  "2023-01-17T20:39:57+01:00",
                  "2023-01-17T20:39:57.234Z",
                  "2023-01-17T20:39:57.234+01:00"
                )
                new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as an array of timestamps: ${e.getMessage}
                                                     |Expected timestamp format is: ${formatter.toString}
                                                     |Eg: ${exampleTimestamps.mkString(",")}
                                                     |""".stripMargin)
              }
          case PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
            val formatter = InstancePropertyValue.Date.formatter
            val strSeq = Try(row.getSeq[String](i)).getOrElse(row.getAs[Array[String]](i).toSeq)
            Try(
              InstancePropertyValue.DateList(
                skipNulls(strSeq)
                  .map(LocalDate.parse(_, formatter))
              )).toEither
              .leftMap(e => new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as an array of dates: ${e.getMessage}
                                                     |Expected date format is: ${formatter.toString}
                                                     |""".stripMargin))
          case PrimitiveProperty(PrimitivePropType.Json, Some(true)) | DirectNodeRelationProperty(_) =>
            val strSeq = Try(row.getSeq[String](i)).getOrElse(row.getAs[Array[String]](i).toSeq)
            skipNulls(strSeq).toVector
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
            tryAsFloat(row.get(i), propertyName).map(InstancePropertyValue.Float32)
          case PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
            tryAsDouble(row.get(i), propertyName).map(InstancePropertyValue.Float64)
          case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
            tryAsInt(row.get(i), propertyName).map(InstancePropertyValue.Int32)
          case PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
            tryAsLong(row.get(i), propertyName).map(InstancePropertyValue.Int64)
          case PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
            val formatter = InstancePropertyValue.Timestamp.formatter
            Try(
              InstancePropertyValue.Timestamp(ZonedDateTime
                .parse(row.getString(i), formatter))).toEither
              .leftMap { e =>
                val exampleTimestamps = Vector(
                  "2023-01-17T20:39:57Z",
                  "2023-01-17T20:39:57+01:00",
                  "2023-01-17T20:39:57.234Z",
                  "2023-01-17T20:39:57.234+01:00"
                )
                new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as an array of timestamps: ${e.getMessage}
                                                     |Expected timestamp format is: ${formatter.toString}
                                                     |Eg: ${exampleTimestamps.mkString(",")}
                                                     |""".stripMargin)
              }
          case PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
            val formatter = InstancePropertyValue.Date.formatter
            Try(InstancePropertyValue.Date(LocalDate.parse(row.getString(i), formatter))).toEither
              .leftMap(e => new CdfSparkException(s"""
                                                     |Error parsing value of field '$propertyName' as an array of dates: ${e.getMessage}
                                                     |Expected date format is: ${formatter.toString}
                                                     |""".stripMargin))
          case PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
            io.circe.parser
              .parse(row
                .getString(i))
              .map(InstancePropertyValue.Object.apply)
              .leftMap(e =>
                new CdfSparkException(
                  s"Error parsing value of field '$propertyName' as an array of json objects: ${e.getMessage}"))

          case t => Left(new CdfSparkException(s"Unhandled non-list type: ${t.toString}"))
        }
      }
      propVal
    }
  }
  // scalastyle:on cyclomatic.complexity method.length

  private def tryAsLong(n: Any, propertyName: String): Either[CdfSparkException, Long] = {
    val nAsStr = String.valueOf(n)
    val bd = BigDecimal(nAsStr)
    if (bd.isValidLong) {
      Right(bd.longValue)
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                    |Expecting a Long but found '$nAsStr'
                                    |""".stripMargin))
    }
  }

  private def tryAsLongSeq(ns: Seq[Any], propertyName: String): Either[CdfSparkException, Seq[Long]] =
    Try {
      skipNulls(ns).map { n =>
        val bd = BigDecimal(String.valueOf(n))
        if (bd.isValidLong) {
          bd.longValue
        } else {
          throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Long")
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(e) =>
        val seqAsStr = ns.map(String.valueOf).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                       |Expecting an Array[Long] but found '[$seqAsStr]' where
                                       |${e.getMessage}
                                       |""".stripMargin))
    }

  private def tryAsInt(n: Any, propertyName: String): Either[CdfSparkException, Int] = {
    val nAsStr = String.valueOf(n)
    val bd = BigDecimal(nAsStr)
    if (bd.isValidInt) {
      Right(bd.intValue)
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                    |Expecting an Int but found '$nAsStr'
                                    |""".stripMargin))
    }
  }

  private def tryAsIntSeq(ns: Seq[Any], propertyName: String): Either[CdfSparkException, Seq[Int]] =
    Try {
      skipNulls(ns).map { n =>
        val bd = BigDecimal(String.valueOf(n))
        if (bd.isValidInt) {
          bd.intValue
        } else {
          throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Int")
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(e) =>
        val seqAsStr = ns.map(String.valueOf).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                      |Expecting an Array[Int] but found '[$seqAsStr]' where
                                      |${e.getMessage}
                                      |""".stripMargin))
    }

  private def tryAsFloat(n: Any, propertyName: String): Either[CdfSparkException, Float] = {
    val nAsStr = String.valueOf(n)
    val bd = BigDecimal(nAsStr)
    if (bd.isDecimalFloat) {
      Right(bd.floatValue)
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                    |Expecting a Float but found '$nAsStr'
                                    |""".stripMargin))
    }
  }

  private def tryAsFloatSeq(ns: Seq[Any], propertyName: String): Either[CdfSparkException, Seq[Float]] =
    Try {
      skipNulls(ns).map { n =>
        val bd = BigDecimal(String.valueOf(n))
        if (bd.isDecimalFloat) {
          bd.floatValue
        } else {
          throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Float")
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(e) =>
        val seqAsStr = ns.map(String.valueOf).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                      |Expecting an Array[Float] but found '[$seqAsStr]'
                                      |${e.getMessage}
                                      |""".stripMargin))
    }

  private def tryAsDouble(n: Any, propertyName: String): Either[CdfSparkException, Double] = {
    val nAsStr = String.valueOf(n)
    val bd = BigDecimal(nAsStr)
    if (bd.isDecimalDouble) {
      Right(bd.doubleValue)
    } else {
      Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                    |Expecting a Double but found '$nAsStr'
                                    |""".stripMargin))
    }
  }

  private def tryAsDoubleSeq(
      ns: Seq[Any],
      propertyName: String): Either[CdfSparkException, Seq[Double]] =
    Try {
      skipNulls(ns).map { n =>
        val bd = BigDecimal(String.valueOf(n))
        if (bd.isDecimalDouble) {
          bd.doubleValue
        } else {
          throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Double")
        }
      }
    } match {
      case Success(value) => Right(value)
      case Failure(e) =>
        val seqAsStr = ns.map(String.valueOf).mkString(",")
        Left(new CdfSparkException(s"""Error parsing value for field '$propertyName'.
                                      |Expecting an Array[Double] but found '[$seqAsStr]' where
                                      |${e.getMessage}
                                      |""".stripMargin))
    }

  private def skipNulls[T](seq: Seq[T]): Seq[T] =
    seq.filter(_ != null)

  private def rowToString(row: Row): String =
    Try(row.json).getOrElse(row.mkString(", "))
}
