package cognite.spark.v1

import cats.implicits._
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition._
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{
  DirectNodeRelationProperty,
  FileReference,
  PrimitiveProperty,
  SequenceReference,
  TextProperty,
  TimeSeriesReference
}
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.{
  EdgeDeletionRequest,
  NodeDeletionRequest
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{
  EdgeOrNodeData,
  InstanceDeletionRequest,
  InstancePropertyValue,
  NodeOrEdgeCreate
}
import io.circe.syntax.EncoderOps
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.time._
import scala.util.{Failure, Success, Try}

// scalastyle:off
//TODO put back scalastyle rules
object FlexibleDataModelRelationUtils {
  private[spark] def createNodes(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      source: SourceReference,
      instanceSpace: Option[String],
      ignoreNullFields: Boolean = true) =
    validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap) *> createNodeWriteData(
      schema,
      source,
      propertyDefMap,
      rows,
      instanceSpace,
      ignoreNullFields)

  private[spark] def createNodes(rows: Seq[Row], schema: StructType, instanceSpace: Option[String]) =
    createNodeWriteData(instanceSpace, schema, rows)

  private[spark] def createEdges(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      source: SourceReference,
      instanceSpace: Option[String],
      ignoreNullFields: Boolean = true): Either[CdfSparkException, Vector[EdgeWrite]] =
    validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap) *> createEdgeWriteData(
      schema,
      source,
      propertyDefMap,
      rows,
      instanceSpace,
      ignoreNullFields)

  private[spark] def createEdges(
      rows: Seq[Row],
      schema: StructType,
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[EdgeWrite]] =
    createEdgeWriteData(schema, rows, instanceSpace)

  private[spark] def createNodesOrEdges(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      source: SourceReference,
      instanceSpace: Option[String],
      ignoreNullFields: Boolean = true
  ): Either[CdfSparkException, Vector[NodeOrEdgeCreate]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(
          propertyDefMap,
          schema,
          instanceSpace.orElse(Some(space)),
          ignoreNullFields,
          row)
        writeData <- createNodeOrEdgeWriteData(
          externalId = externalId,
          instanceSpace = space,
          source,
          edgeNodeTypeRelation =
            extractEdgeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          startNodeRelation =
            extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          endNodeRelation =
            extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          props,
          row
        )
      } yield writeData
    }

  private[spark] def createNodesOrEdges(
      rows: Seq[Row],
      schema: StructType,
      instanceSpace: Option[String]
  ): Either[CdfSparkException, Vector[NodeOrEdgeCreate]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        externalId <- extractExternalId(schema, row)
        writeData <- createNodeOrEdgeWriteData(
          externalId = externalId,
          instanceSpace = space,
          edgeNodeTypeRelation =
            extractEdgeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          startNodeRelation =
            extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          endNodeRelation =
            extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          row
        )
      } yield writeData
    }

  private[spark] def createConnectionInstances(
      edgeType: DirectRelationReference,
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[EdgeWrite]] =
    createConnectionInstanceWriteData(
      schema,
      edgeType,
      rows,
      instanceSpace
    )

  private[spark] def createNodeDeleteData(
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[InstanceDeletionRequest]] =
    rows.toVector.traverse { row =>
      for {
        spaceExtId <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
      } yield NodeDeletionRequest(space = spaceExtId, externalId = extId)
    }

  private[spark] def createEdgeDeleteData(
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[InstanceDeletionRequest]] =
    rows.toVector.traverse { row =>
      for {
        spaceExtId <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
      } yield
        EdgeDeletionRequest(
          space = spaceExtId,
          externalId = extId
        )
    }

  private def createEdgeWriteData(
      schema: StructType,
      source: SourceReference,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      rows: Seq[Row],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean): Either[CdfSparkException, Vector[EdgeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
        edgeType <- extractEdgeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        startNode <- extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        endNode <- extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        props <- extractInstancePropertyValues(
          propertyDefMap,
          schema,
          instanceSpace.orElse(Some(space)),
          ignoreNullFields,
          row)
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = space,
          externalId = extId,
          startNode = startNode,
          endNode = endNode,
          sources = Some(
            Seq(
              EdgeOrNodeData(
                source = source,
                properties = Some(props.toMap)
              )
            )
          )
        )
    }

  private def createEdgeWriteData(
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[EdgeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
        edgeType <- extractEdgeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        startNode <- extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        endNode <- extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = space,
          externalId = extId,
          startNode = startNode,
          endNode = endNode,
          sources = None
        )
    }

  // scalastyle:off method.length
  private def createNodeOrEdgeWriteData(
      externalId: String,
      instanceSpace: String,
      source: SourceReference,
      edgeNodeTypeRelation: Option[DirectRelationReference],
      startNodeRelation: Option[DirectRelationReference],
      endNodeRelation: Option[DirectRelationReference],
      props: Vector[(String, Option[InstancePropertyValue])],
      row: Row): Either[CdfSparkException, NodeOrEdgeCreate] =
    (edgeNodeTypeRelation, startNodeRelation, endNodeRelation) match {
      case (Some(edgeType), Some(startNode), Some(endNode)) =>
        Right(
          EdgeWrite(
            `type` = edgeType,
            space = instanceSpace,
            externalId = externalId,
            startNode = startNode,
            endNode = endNode,
            sources = Some(
              Seq(
                EdgeOrNodeData(
                  source = source,
                  properties = Some(props.toMap)
                )
              )
            )
          )
        )
      case (None, None, None) =>
        Right(
          NodeWrite(
            space = instanceSpace,
            externalId = externalId,
            sources = Some(
              Seq(
                EdgeOrNodeData(
                  source = source,
                  properties = Some(props.toMap)
                )
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

  private def createNodeOrEdgeWriteData(
      externalId: String,
      instanceSpace: String,
      edgeNodeTypeRelation: Option[DirectRelationReference],
      startNodeRelation: Option[DirectRelationReference],
      endNodeRelation: Option[DirectRelationReference],
      row: Row): Either[CdfSparkException, NodeOrEdgeCreate] =
    (edgeNodeTypeRelation, startNodeRelation, endNodeRelation) match {
      case (Some(edgeType), Some(startNode), Some(endNode)) =>
        Right(
          EdgeWrite(
            `type` = edgeType,
            space = instanceSpace,
            externalId = externalId,
            startNode = startNode,
            endNode = endNode,
            sources = None
          )
        )
      case (None, None, None) =>
        Right(
          NodeWrite(
            space = instanceSpace,
            externalId = externalId,
            sources = None
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
      schema: StructType,
      source: SourceReference,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      rows: Seq[Row],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean): Either[CdfSparkException, Vector[NodeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(
          propertyDefMap,
          schema,
          instanceSpace.orElse(Some(space)),
          ignoreNullFields,
          row)
      } yield
        NodeWrite(
          space = space,
          externalId = externalId,
          sources = Some(
            Seq(
              EdgeOrNodeData(
                source = source,
                properties = Some(props.toMap)
              )
            )
          )
        )
    }

  private def createNodeWriteData(
      instanceSpace: Option[String],
      schema: StructType,
      rows: Seq[Row]): Either[CdfSparkException, Vector[NodeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        externalId <- extractExternalId(schema, row)
      } yield
        NodeWrite(
          space = space,
          externalId = externalId,
          sources = None
        )
    }

  private def createConnectionInstanceWriteData(
      schema: StructType,
      edgeType: DirectRelationReference,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[EdgeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
        startNode <- extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        endNode <- extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = space,
          externalId = extId,
          startNode = startNode,
          endNode = endNode,
          sources = None
        )
    }

  private def extractExternalId(schema: StructType, row: Row): Either[CdfSparkException, String] =
    Try {
      Option(row.get(schema.fieldIndex("externalId")))
    } match {
      case Success(Some(externalId)) => Right(String.valueOf(externalId))
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

  private def extractSpace(schema: StructType, row: Row): Either[CdfSparkException, String] =
    Try {
      Option(row.get(schema.fieldIndex("space")))
    } match {
      case Success(Some(space)) => Right(String.valueOf(space))
      case Success(None) =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""
               |'space' cannot be null
               |in data row: ${rowToString(row)}
               |""".stripMargin
          ))
      case Failure(err) =>
        Left(new CdfSparkException(s"""
                                      |Couldn't find required string property 'space': ${err.getMessage}
                                      |in data row: ${rowToString(row)}
                                      |""".stripMargin))
    }

  private def extractEdgeTypeDirectRelation(
      schema: StructType,
      instanceSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("type", "Edge type", schema, instanceSpace, row)

  private def extractEdgeStartNodeDirectRelation(
      schema: StructType,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("startNode", "Edge start node", schema, defaultSpace, row)

  private def extractEdgeEndNodeDirectRelation(
      schema: StructType,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("endNode", "Edge end node", schema, defaultSpace, row)

  private def extractDirectRelationReferenceFromStruct(
      propertyName: String,
      descriptiveName: String,
      defaultSpace: Option[String],
      struct: Row): Either[CdfSparkException, DirectRelationReference] = {
    val space = Try(Option(struct.getAs[Any]("space")))
      .orElse(Try(Option(struct.getAs[Any]("spaceExternalId")))) // just in case of fdm v2 utils are used
      .getOrElse(None)
      .orElse(defaultSpace)
    val externalId = Option(struct.getAs[Any]("externalId"))
    (space, externalId) match {
      case (Some(s), Some(e)) =>
        Right(DirectRelationReference(space = String.valueOf(s), externalId = String.valueOf(e)))
      case (_, None) => Left(new CdfSparkException(s"""
        |'$propertyName' ($descriptiveName) cannot contain null values.
        |Please verify that 'externalId' values are not null for '$propertyName'
        |in data row: ${rowToString(struct)}
        |""".stripMargin))
      case (None, _) => Left(new CdfSparkException(s"""
        |'$propertyName' ($descriptiveName) cannot contain null values.
        |Please verify that 'space' values are not null for '$propertyName'
        |in data row: ${rowToString(struct)}
        |""".stripMargin))
    }
  }

  private def extractDirectRelation(
      propertyName: String,
      descriptiveName: String,
      schema: StructType,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    Try {
      val struct = row.getStruct(schema.fieldIndex(propertyName))
      extractDirectRelationReferenceFromStruct(propertyName, descriptiveName, defaultSpace, struct)
    } match {
      case Success(relation) => relation
      case Failure(err) =>
        Left(new CdfSparkException(s"""
            |Could not find required property '$propertyName'
            |'$propertyName' ($descriptiveName) should be a
            | 'StructType' with 'space' & 'externalId' properties: ${err.getMessage}
            |in data row: ${rowToString(row)}
            |""".stripMargin))
    }

  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  private[spark] def extractInstancePropertyValue(
      propType: DataType,
      value: InstancePropertyValue): Any =
    (propType, value) match {
      case (StringType, InstancePropertyValue.Date(v)) => v.toString
      case (StringType, InstancePropertyValue.Timestamp(v)) => v.toString
      case (ArrayType(StringType, _), InstancePropertyValue.TimestampList(v)) => v.map(_.toString)
      case (ArrayType(StringType, _), InstancePropertyValue.DateList(v)) => v.map(_.toString)
      case (IntegerType, InstancePropertyValue.Float64(v)) => v.toInt
      case (IntegerType, InstancePropertyValue.Int64(v)) => v.toInt
      case (IntegerType, InstancePropertyValue.Float32(v)) => v.toInt
      case (LongType, InstancePropertyValue.Int32(v)) => v.toLong
      case (LongType, InstancePropertyValue.Float32(v)) => v.toLong
      case (LongType, InstancePropertyValue.Float64(v)) => v.toLong
      case (DoubleType, InstancePropertyValue.Int32(v)) => v.toDouble
      case (DoubleType, InstancePropertyValue.Int64(v)) => v.toDouble
      case (DoubleType, InstancePropertyValue.Float32(v)) => v.toDouble
      case (FloatType, InstancePropertyValue.Float64(v)) => v.toFloat
      case (FloatType, InstancePropertyValue.Int32(v)) => v.toFloat
      case (FloatType, InstancePropertyValue.Int64(v)) => v.toFloat
      case (ArrayType(IntegerType, _), InstancePropertyValue.Float64List(v)) => v.map(_.toInt)
      case (ArrayType(IntegerType, _), InstancePropertyValue.Int64List(v)) => v.map(_.toInt)
      case (ArrayType(IntegerType, _), InstancePropertyValue.Float32List(v)) => v.map(_.toInt)
      case (ArrayType(LongType, _), InstancePropertyValue.Int32List(v)) => v.map(_.toLong)
      case (ArrayType(LongType, _), InstancePropertyValue.Float32List(v)) => v.map(_.toLong)
      case (ArrayType(LongType, _), InstancePropertyValue.Float64List(v)) => v.map(_.toLong)
      case (ArrayType(DoubleType, _), InstancePropertyValue.Int32List(v)) => v.map(_.toDouble)
      case (ArrayType(DoubleType, _), InstancePropertyValue.Int64List(v)) => v.map(_.toDouble)
      case (ArrayType(DoubleType, _), InstancePropertyValue.Float32List(v)) => v.map(_.toDouble)
      case (ArrayType(FloatType, _), InstancePropertyValue.Float64List(v)) => v.map(_.toFloat)
      case (ArrayType(FloatType, _), InstancePropertyValue.Int32List(v)) => v.map(_.toFloat)
      case (ArrayType(FloatType, _), InstancePropertyValue.Int64List(v)) => v.map(_.toFloat)
      case (_, InstancePropertyValue.Int64(v)) => v
      case (_, InstancePropertyValue.Float64(v)) => v
      case (_, InstancePropertyValue.Float32(v)) => v
      case (_, InstancePropertyValue.Int32(value)) => value
      case (_, InstancePropertyValue.Int32List(value)) => value
      case (_, InstancePropertyValue.Int64List(value)) => value
      case (_, InstancePropertyValue.Float32List(value)) => value
      case (_, InstancePropertyValue.Float64List(value)) => value
      case (_, InstancePropertyValue.String(value)) => value
      case (_, InstancePropertyValue.Boolean(value)) => value
      case (_, InstancePropertyValue.Date(value)) => java.sql.Date.valueOf(value)
      case (_, InstancePropertyValue.Timestamp(value)) => java.sql.Timestamp.from(value.toInstant)
      case (_, InstancePropertyValue.Object(value)) => value.noSpaces
      case (_, InstancePropertyValue.ViewDirectNodeRelation(value)) =>
        value.map(r => Array(r.space, r.externalId)).orNull
      case (_, InstancePropertyValue.StringList(value)) => value
      case (_, InstancePropertyValue.BooleanList(value)) => value
      case (_, InstancePropertyValue.DateList(value)) => value.map(v => java.sql.Date.valueOf(v))
      case (_, InstancePropertyValue.TimestampList(value)) =>
        value.map(v => java.sql.Timestamp.from(v.toInstant))
      case (_, InstancePropertyValue.ObjectList(value)) => value.map(_.noSpaces)
      case (_, InstancePropertyValue.TimeSeriesReference(value)) => value
      case (_, InstancePropertyValue.FileReference(value)) => value
      case (_, InstancePropertyValue.SequenceReference(value)) => value
      case (_, InstancePropertyValue.TimeSeriesReferenceList(value)) => value
      case (_, InstancePropertyValue.FileReferenceList(value)) => value
      case (_, InstancePropertyValue.SequenceReferenceList(value)) => value
      case (_, InstancePropertyValue.ViewDirectNodeRelationList(value)) =>
        value.map(r => Array(r.space, r.externalId))
    }

  private def extractInstancePropertyValues(
      propertyDefMap: Map[String, ViewPropertyDefinition],
      schema: StructType,
      instanceSpace: Option[String],
      ignoreNullFields: Boolean,
      row: Row): Either[CdfSparkException, Vector[(String, Option[InstancePropertyValue])]] =
    propertyDefMap.toVector.flatTraverse {
      case (propName, propDef) =>
        propertyDefinitionToInstancePropertyValue(row, schema, propName, propDef, instanceSpace).map {
          case FieldSpecified(t) => Vector(propName -> Some(t))
          case FieldNull =>
            if (ignoreNullFields) {
              Vector.empty
            } else {
              Vector(propName -> None)
            }
          case FieldNotSpecified => Vector.empty
        }
    }

  private def validateRowFieldsWithPropertyDefinitions(
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition]): Either[CdfSparkException, Boolean] = {

    val (propsExistsInSchema @ _, propsMissingInSchema) = propertyDefMap.partition {
      case (propName, _) => Try(schema.fieldIndex(propName)).isSuccess
    }
    val (nullablePropsMissingInSchema @ _, nonNullablePropsMissingInSchema) =
      propsMissingInSchema.partition {
        case (_, corePropDef: ViewCorePropertyDefinition) => corePropDef.nullable.getOrElse(true)
        case (_, _: ConnectionDefinition) => true
      }

    if (nonNullablePropsMissingInSchema.nonEmpty) {
      val propsAsStr = nonNullablePropsMissingInSchema.keys.mkString(", ")
      Left(new CdfSparkException(s"Could not find required properties: [$propsAsStr]"))
    } else {
      Right(true)
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
  private def propertyDefinitionToInstancePropertyValue(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: ViewPropertyDefinition,
      instanceSpace: Option[String]): Either[CdfSparkException, OptionalField[InstancePropertyValue]] = {
    val instancePropertyValueResult = propDef match {
      case corePropDef: PropertyDefinition.ViewCorePropertyDefinition =>
        corePropDef.`type` match {
          case t if t.isList =>
            toInstancePropertyValueOfList(row, schema, propertyName, corePropDef, instanceSpace)
          case _ =>
            toInstancePropertyValueOfNonList(row, schema, propertyName, corePropDef, instanceSpace)
        }
      case _: PropertyDefinition.ConnectionDefinition =>
        lookupFieldInRow(row, schema, propertyName, nullable = true) { _ =>
          extractDirectRelation(propertyName, "Connection Reference", schema, instanceSpace, row)
            .map(_.asJson)
            .map(InstancePropertyValue.Object)
        }
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

  private val timezoneId: ZoneId = ZoneId.of("UTC")

  private def lookupFieldInRow(row: Row, schema: StructType, propertyName: String, nullable: Boolean)(
      get: => Int => Either[Throwable, InstancePropertyValue])
    : Either[Throwable, OptionalField[InstancePropertyValue]] =
    Try(schema.fieldIndex(propertyName)) match {
      case Failure(_) => Right(FieldNotSpecified)
      case Success(i) if i >= row.length || i < 0 => Right(FieldNotSpecified)
      case Success(i) if row.isNullAt(i) =>
        if (nullable) {
          Right(FieldNull)
        } else {
          Left(new CdfSparkException(s"'$propertyName' cannot be null"))
        }
      case Success(i) => get(i).map(FieldSpecified(_))
    }

  private def getListPropAsSeq[T](propertyName: String, row: Row, index: Integer): Seq[T] = {
    Try(row.getSeq[T](index)) match {
      case Success(x) => x
      case Failure(_) =>
        Try(row.getAs[Array[T]](index).toSeq) match {
          case Success(x) => x
          case Failure(err) =>
            throw new CdfSparkException(
              f"""Could not deserialize property $propertyName as an array of the expected type""",
              err)
        }
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstancePropertyValueOfList(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: CorePropertyDefinition,
      instanceSpace: Option[String]
  ): Either[Throwable, OptionalField[InstancePropertyValue]] =
    lookupFieldInRow(row, schema, propertyName, propDef.nullable.getOrElse(true)) { i =>
      propDef.`type` match {
        case _: DirectNodeRelationProperty =>
          val structSeq = getListPropAsSeq[Row](propertyName, row, i)
          tryAsDirectNodeRelationList(structSeq, propertyName, instanceSpace)
            .map(InstancePropertyValue.ViewDirectNodeRelationList)
        case _: TextProperty =>
          Try({
            val strSeq = getListPropAsSeq[String](propertyName, row, i)
            InstancePropertyValue.StringList(skipNulls(strSeq))
          }).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Boolean, _) =>
          Try({
            val boolSeq = getListPropAsSeq[Boolean](propertyName, row, i)
            InstancePropertyValue.BooleanList(boolSeq)
          }).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Float32, _) =>
          val floatSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(floatSeq, propertyName, Float.toString, safeConvertToFloat)
            .map(InstancePropertyValue.Float32List)
        case _ @PrimitiveProperty(PrimitivePropType.Float64, _) =>
          val doubleSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(doubleSeq, propertyName, Double.toString, safeConvertToDouble)
            .map(InstancePropertyValue.Float64List)
        case _ @PrimitiveProperty(PrimitivePropType.Int32, _) =>
          val intSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(intSeq, propertyName, Int.toString, safeConvertToInt)
            .map(InstancePropertyValue.Int32List)
        case _ @PrimitiveProperty(PrimitivePropType.Int64, _) =>
          val longSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(longSeq, propertyName, Long.toString, safeConvertToLong)
            .map(InstancePropertyValue.Int64List)
        case _ @PrimitiveProperty(PrimitivePropType.Timestamp, _) =>
          val tsSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryAsTimestamps(tsSeq, propertyName).map(InstancePropertyValue.TimestampList)
        case _ @PrimitiveProperty(PrimitivePropType.Date, _) =>
          val dateSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryAsDates(dateSeq, propertyName).map(InstancePropertyValue.DateList)
        case _ @PrimitiveProperty(PrimitivePropType.Json, _) =>
          val strSeq = getListPropAsSeq[String](propertyName, row, i)
          skipNulls(strSeq).toVector
            .traverse(io.circe.parser.parse)
            .map(InstancePropertyValue.ObjectList.apply)
            .leftMap(e =>
              new CdfSparkException(
                s"Error parsing value of field '$propertyName' as a list of json objects: ${e.getMessage}"))
        case _: TimeSeriesReference =>
          val strSeq = getListPropAsSeq[Any](propertyName, row, i)
          Try(InstancePropertyValue.TimeSeriesReferenceList(skipNulls(strSeq).map(String.valueOf))).toEither
        case _: FileReference =>
          val strSeq = getListPropAsSeq[Any](propertyName, row, i)
          Try(InstancePropertyValue.FileReferenceList(skipNulls(strSeq).map(String.valueOf))).toEither
        case _: SequenceReference =>
          val strSeq = getListPropAsSeq[Any](propertyName, row, i)
          Try(InstancePropertyValue.SequenceReferenceList(skipNulls(strSeq).map(String.valueOf))).toEither
        case t => Left(new CdfSparkException(s"Unhandled list type: ${t.toString}"))
      }
    }

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstancePropertyValueOfNonList(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: CorePropertyDefinition,
      instanceSpace: Option[String]): Either[Throwable, OptionalField[InstancePropertyValue]] =
    lookupFieldInRow(row, schema, propertyName, propDef.nullable.getOrElse(true)) { i =>
      propDef.`type` match {
        case _: DirectNodeRelationProperty =>
          extractDirectRelation(propertyName, "Direct Node Relation", schema, instanceSpace, row)
            .map(directRelationReference =>
              InstancePropertyValue.ViewDirectNodeRelation(Some(directRelationReference)))
        case _: TextProperty =>
          Try(InstancePropertyValue.String(String.valueOf(row.get(i)))).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Boolean, _) =>
          Try(InstancePropertyValue.Boolean(row.getBoolean(i))).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Float32, _) =>
          tryConvertNumber(row.get(i), propertyName, Float.toString, safeConvertToFloat)
            .map(InstancePropertyValue.Float32)
        case _ @PrimitiveProperty(PrimitivePropType.Float64, _) =>
          tryConvertNumber(row.get(i), propertyName, Double.toString, safeConvertToDouble)
            .map(InstancePropertyValue.Float64)
        case _ @PrimitiveProperty(PrimitivePropType.Int32, _) =>
          tryConvertNumber(row.get(i), propertyName, Int.toString, safeConvertToInt)
            .map(InstancePropertyValue.Int32)
        case _ @PrimitiveProperty(PrimitivePropType.Int64, _) =>
          tryConvertNumber(row.get(i), propertyName, Long.toString, safeConvertToLong)
            .map(InstancePropertyValue.Int64)
        case _ @PrimitiveProperty(PrimitivePropType.Timestamp, _) =>
          tryAsTimestamp(row.get(i), propertyName).map(InstancePropertyValue.Timestamp.apply)
        case _ @PrimitiveProperty(PrimitivePropType.Date, _) =>
          tryAsDate(row.get(i), propertyName).map(InstancePropertyValue.Date.apply)
        case _ @PrimitiveProperty(PrimitivePropType.Json, _) =>
          io.circe.parser
            .parse(row
              .getString(i))
            .map(InstancePropertyValue.Object.apply)
            .leftMap(e =>
              new CdfSparkException(
                s"Error parsing value of field '$propertyName' as a json object: ${e.getMessage}"))
        case _: TimeSeriesReference =>
          Try(InstancePropertyValue.TimeSeriesReference(String.valueOf(row.get(i)))).toEither
        case _: FileReference =>
          Try(InstancePropertyValue.FileReference(String.valueOf(row.get(i)))).toEither
        case _: SequenceReference =>
          Try(InstancePropertyValue.SequenceReference(String.valueOf(row.get(i)))).toEither
        case t => Left(new CdfSparkException(s"Unhandled non-list type: ${t.toString}"))
      }
    }
  // scalastyle:on cyclomatic.complexity method.length

  private def safeConvertToLong(n: BigDecimal): Long =
    if (n.isValidLong) {
      n.longValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Long")
    }
  private def safeConvertToInt(n: BigDecimal): Int =
    if (n.isValidInt) {
      n.intValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Int")
    }
  private def safeConvertToDouble(n: BigDecimal): Double =
    if (n.isDecimalDouble) {
      n.doubleValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Double")
    }
  private def safeConvertToFloat(n: BigDecimal): Float =
    if (n.isDecimalFloat) {
      n.floatValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Float")
    }

  private def tryConvertNumber[T](
      n: Any,
      propertyName: String,
      expectedType: String,
      safeConvert: (BigDecimal) => T
  ): Either[CdfSparkException, T] =
    Try {
      val asBigDecimal = BigDecimal(String.valueOf(n))
      safeConvert(asBigDecimal)
    }.toEither
      .leftMap(exception =>
        new CdfSparkException(
          s"""Error parsing value for field '$propertyName'.
                                 |Expecting a $expectedType but found '${String.valueOf(n)}'
                                 |""".stripMargin,
          exception
      ))

  private def tryConvertNumberSeq[T](
      ns: Seq[Any],
      propertyName: String,
      expectedType: String,
      safeConvert: (BigDecimal) => T): Either[CdfSparkException, Seq[T]] =
    Try {
      skipNulls(ns).map { n =>
        val asBigDecimal = BigDecimal(String.valueOf(n))
        safeConvert(asBigDecimal)
      }
    }.toEither
      .leftMap(exception => {
        val seqAsStr = ns.map(String.valueOf).mkString(",")
        new CdfSparkException(
          s"""Error parsing value for field '$propertyName'.
                                 |Expecting a $expectedType but found '$seqAsStr'
                                 |""".stripMargin,
          exception
        )
      })

  private def tryAsDirectNodeRelationList(
      value: Seq[Row],
      propertyName: String,
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[DirectRelationReference]] =
    skipNulls(value).toVector
      .traverse(extractDirectRelations(propertyName, "Direct Node Relation", instanceSpace, _))

  private def extractDirectRelations(
      propertyName: String,
      descriptiveName: String,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    Try {
      extractDirectRelationReferenceFromStruct(propertyName, descriptiveName, defaultSpace, row)
    } match {
      case Success(relation) => relation
      case Failure(err) =>
        Left(new CdfSparkException(s"""
                                      |Could not find required property '$propertyName'
                                      |'$propertyName' ($descriptiveName) should be a
                                      | 'StructType' with 'space' & 'externalId' properties: ${err.getMessage}
                                      |in data row: ${rowToString(row)}
                                      |""".stripMargin))
    }

  private def tryAsDate(value: Any, propertyName: String): Either[CdfSparkException, LocalDate] =
    Try(value.asInstanceOf[java.sql.Date].toLocalDate)
      .orElse(Try(LocalDate.parse(String.valueOf(value))))
      .toEither
      .orElse(tryAsTimestamp(value, propertyName).map(_.toLocalDate))
      .leftMap { e =>
        new CdfSparkException(
          s"""Error parsing value of field '$propertyName' as a date: ${e.getMessage}""".stripMargin)
      }

  private def tryAsDates(
      value: Seq[Any],
      propertyName: String): Either[CdfSparkException, Vector[LocalDate]] =
    skipNulls(value).toVector.traverse(tryAsDate(_, propertyName)).leftMap { e =>
      new CdfSparkException(
        s"""Error parsing value of field '$propertyName' as an array of dates: ${e.getMessage}""".stripMargin)
    }

  private def tryAsTimestamp(
      value: Any,
      propertyName: String): Either[CdfSparkException, ZonedDateTime] =
    Try(
      ZonedDateTime
        .ofLocal(value.asInstanceOf[java.sql.Timestamp].toLocalDateTime, timezoneId, ZoneOffset.UTC))
      .orElse(Try(ZonedDateTime.parse(String.valueOf(value))))
      .orElse(Try(LocalDateTime.parse(String.valueOf(value)).atZone(timezoneId)))
      .toEither
      .leftMap { e =>
        new CdfSparkException(
          s"""Error parsing value of field '$propertyName' as a timestamp: ${e.getMessage}""".stripMargin)
      }

  private def tryAsTimestamps(
      value: Seq[Any],
      propertyName: String): Either[CdfSparkException, Vector[ZonedDateTime]] =
    skipNulls(value).toVector.traverse(tryAsTimestamp(_, propertyName)).leftMap { e =>
      new CdfSparkException(
        s"""Error parsing value of field '$propertyName' as an array of timestamps: ${e.getMessage}""".stripMargin)
    }

  private def extractSpaceOrDefault(schema: StructType, row: Row, defaultSpace: Option[String]) =
    defaultSpace.map(Right(_)).getOrElse(extractSpace(schema, row)).leftMap { e =>
      new CdfSparkIllegalArgumentException(
        s"""
           |There's no 'instanceSpace' specified to be used as default space and could not extract 'space' from data.
           | If the intention is to not use a default space then please make sure the data is correct.
           | ${e.getMessage}
           |""".stripMargin
      )
    }

  private def skipNulls[T](seq: Seq[T]): Seq[T] =
    seq.filter(_ != null)

  private def rowToString(row: Row): String =
    Try(row.json).getOrElse(row.mkString(", "))
}
// scalastyle:on number.of.methods file.size.limit
