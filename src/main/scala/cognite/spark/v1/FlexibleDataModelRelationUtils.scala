package cognite.spark.v1

import cats.Apply
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
import org.apache.spark.sql.types.StructType

import java.time._
import scala.util.{Failure, Success, Try}

// scalastyle:off number.of.methods file.size.limit
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

  private def extractDirectRelation(
      propertyName: String,
      descriptiveName: String,
      schema: StructType,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    Try {
      val struct = row.getStruct(schema.fieldIndex(propertyName))
      val space = Try(Option(struct.getAs[Any]("space")))
        .orElse(Try(Option(struct.getAs[Any]("spaceExternalId")))) // just in case of fdm v2 utils are used
        .getOrElse(None)
        .orElse(defaultSpace)
      val externalId = Option(struct.getAs[Any]("externalId"))
      Apply[Option].map2(space, externalId) {
        case (s, e) => DirectRelationReference(space = String.valueOf(s), externalId = String.valueOf(e))
      }
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
          case _: DirectNodeRelationProperty =>
            directNodeRelationToInstancePropertyValue(
              row,
              schema,
              propertyName,
              corePropDef,
              instanceSpace)
          case t if t.isList => toInstancePropertyValueOfList(row, schema, propertyName, corePropDef)
          case _ => toInstancePropertyValueOfNonList(row, schema, propertyName, corePropDef)
        }
      case _: PropertyDefinition.ConnectionDefinition =>
        lookupFieldInRow(row, schema, propertyName, true) { _ =>
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
      case Success(i) if i >= row.length => Right(FieldNotSpecified)
      case Success(i) if row.isNullAt(i) =>
        if (nullable) {
          Right(FieldNull)
        } else {
          Left(new CdfSparkException(s"'$propertyName' cannot be null"))
        }
      case Success(i) => get(i).map(FieldSpecified(_))
    }

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstancePropertyValueOfList(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: CorePropertyDefinition): Either[Throwable, OptionalField[InstancePropertyValue]] =
    lookupFieldInRow(row, schema, propertyName, propDef.nullable.getOrElse(true)) { i =>
      propDef.`type` match {
        case p: TextProperty if p.isList =>
          val strSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          Try(InstancePropertyValue.StringList(skipNulls(strSeq).map(String.valueOf))).toEither
        case p @ PrimitiveProperty(PrimitivePropType.Boolean, _) if p.isList =>
          val boolSeq = Try(row.getSeq[Boolean](i)).getOrElse(row.getAs[Array[Boolean]](i).toSeq)
          Try(InstancePropertyValue.BooleanList(boolSeq)).toEither
        case p @ PrimitiveProperty(PrimitivePropType.Float32, _) if p.isList =>
          val floatSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          tryAsFloatSeq(floatSeq, propertyName)
            .map(InstancePropertyValue.Float32List)
        case p @ PrimitiveProperty(PrimitivePropType.Float64, _) if p.isList =>
          val doubleSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          tryAsDoubleSeq(doubleSeq, propertyName)
            .map(InstancePropertyValue.Float64List)
        case p @ PrimitiveProperty(PrimitivePropType.Int32, _) if p.isList =>
          val intSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          tryAsIntSeq(intSeq, propertyName)
            .map(InstancePropertyValue.Int32List)
        case p @ PrimitiveProperty(PrimitivePropType.Int64, _) if p.isList =>
          val longSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          tryAsLongSeq(longSeq, propertyName)
            .map(InstancePropertyValue.Int64List)
        case p @ PrimitiveProperty(PrimitivePropType.Timestamp, _) if p.isList =>
          val tsSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          tryAsTimestamps(tsSeq, propertyName).map(InstancePropertyValue.TimestampList)
        case p @ PrimitiveProperty(PrimitivePropType.Date, _) if p.isList =>
          val dateSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          tryAsDates(dateSeq, propertyName).map(InstancePropertyValue.DateList)
        case p @ PrimitiveProperty(PrimitivePropType.Json, _) if p.isList =>
          val strSeq = Try(row.getSeq[String](i)).getOrElse(row.getAs[Array[String]](i).toSeq)
          skipNulls(strSeq).toVector
            .traverse(io.circe.parser.parse)
            .map(InstancePropertyValue.ObjectList.apply)
            .leftMap(e =>
              new CdfSparkException(
                s"Error parsing value of field '$propertyName' as a list of json objects: ${e.getMessage}"))
        case p: TimeSeriesReference if p.isList =>
          val strSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          Try(InstancePropertyValue.TimeSeriesReferenceList(skipNulls(strSeq).map(String.valueOf))).toEither
        case p: FileReference if p.isList =>
          val strSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          Try(InstancePropertyValue.FileReferenceList(skipNulls(strSeq).map(String.valueOf))).toEither
        case p: SequenceReference if p.isList =>
          val strSeq = Try(row.getSeq[Any](i)).getOrElse(row.getAs[Array[Any]](i).toSeq)
          Try(InstancePropertyValue.SequenceReferenceList(skipNulls(strSeq).map(String.valueOf))).toEither
        case t => Left(new CdfSparkException(s"Unhandled list type: ${t.toString}"))
      }
    }

  // scalastyle:off cyclomatic.complexity method.length
  private def toInstancePropertyValueOfNonList(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: CorePropertyDefinition): Either[Throwable, OptionalField[InstancePropertyValue]] =
    lookupFieldInRow(row, schema, propertyName, propDef.nullable.getOrElse(true)) { i =>
      propDef.`type` match {
        case p: TextProperty if !p.isList =>
          Try(InstancePropertyValue.String(String.valueOf(row.get(i)))).toEither
        case p @ PrimitiveProperty(PrimitivePropType.Boolean, _) if !p.isList =>
          Try(InstancePropertyValue.Boolean(row.getBoolean(i))).toEither
        case p @ PrimitiveProperty(PrimitivePropType.Float32, _) if !p.isList =>
          tryAsFloat(row.get(i), propertyName).map(InstancePropertyValue.Float32)
        case p @ PrimitiveProperty(PrimitivePropType.Float64, _) if !p.isList =>
          tryAsDouble(row.get(i), propertyName).map(InstancePropertyValue.Float64)
        case p @ PrimitiveProperty(PrimitivePropType.Int32, _) if !p.isList =>
          tryAsInt(row.get(i), propertyName).map(InstancePropertyValue.Int32)
        case p @ PrimitiveProperty(PrimitivePropType.Int64, _) if !p.isList =>
          tryAsLong(row.get(i), propertyName).map(InstancePropertyValue.Int64)
        case p @ PrimitiveProperty(PrimitivePropType.Timestamp, _) if !p.isList =>
          tryAsTimestamp(row.get(i), propertyName).map(InstancePropertyValue.Timestamp.apply)
        case p @ PrimitiveProperty(PrimitivePropType.Date, _) if !p.isList =>
          tryAsDate(row.get(i), propertyName).map(InstancePropertyValue.Date.apply)
        case p @ PrimitiveProperty(PrimitivePropType.Json, _) if !p.isList =>
          io.circe.parser
            .parse(row
              .getString(i))
            .map(InstancePropertyValue.Object.apply)
            .leftMap(e =>
              new CdfSparkException(
                s"Error parsing value of field '$propertyName' as a json object: ${e.getMessage}"))
        case p: TimeSeriesReference if !p.isList =>
          Try(InstancePropertyValue.TimeSeriesReference(String.valueOf(row.get(i)))).toEither
        case p: FileReference if !p.isList =>
          Try(InstancePropertyValue.FileReference(String.valueOf(row.get(i)))).toEither
        case p: SequenceReference if !p.isList =>
          Try(InstancePropertyValue.SequenceReference(String.valueOf(row.get(i)))).toEither
        case t => Left(new CdfSparkException(s"Unhandled non-list type: ${t.toString}"))
      }
    }
  // scalastyle:on cyclomatic.complexity method.length

  private def directNodeRelationToInstancePropertyValue(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: CorePropertyDefinition,
      defaultSpace: Option[String]): Either[Throwable, OptionalField[InstancePropertyValue]] = {
    val nullable = propDef.nullable.getOrElse(true)
    lookupFieldInRow(row, schema, propertyName, nullable) { _ =>
      extractDirectRelation(propertyName, "Direct Node Relation", schema, defaultSpace, row)
        .map(_.asJson)
        .map(InstancePropertyValue.Object)
    }
  }

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
