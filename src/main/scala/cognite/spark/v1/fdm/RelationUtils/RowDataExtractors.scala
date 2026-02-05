package cognite.spark.v1.fdm.RelationUtils

import cats.implicits._
import cognite.spark.v1.fdm.RelationUtils.Convertors._
import cognite.spark.v1.{
  CdfSparkException,
  CdfSparkIllegalArgumentException,
  FieldNotSpecified,
  FieldNull,
  FieldSpecified,
  OptionalField
}
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition._
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{
  DirectNodeRelationProperty,
  EnumProperty,
  FileReference,
  PrimitiveProperty,
  SequenceReference,
  TextProperty,
  TimeSeriesReference
}
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  ListablePropertyType,
  PrimitivePropType,
  PropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.instances.InstancePropertyValue
import io.circe.syntax.EncoderOps
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

object RowDataExtractors {

  def extractExternalId(schema: StructType, row: Row): Either[CdfSparkException, String] =
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

  def extractNodeTypeDirectRelation(
      schema: StructType,
      instanceSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("_type", "Node type", schema, instanceSpace, row)

  //For edge we support using "type" as an alias for "_type" for legacy reasons
  def extractEdgeTypeDirectRelation(
      schema: StructType,
      instanceSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("_type", "Edge type", schema, instanceSpace, row) match {
      case right @ Right(_) => right
      case _ => extractDirectRelation("type", "Edge type", schema, instanceSpace, row)
    }

  def extractEdgeStartNodeDirectRelation(
      schema: StructType,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("startNode", "Edge start node", schema, defaultSpace, row)

  def extractEdgeEndNodeDirectRelation(
      schema: StructType,
      defaultSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    extractDirectRelation("endNode", "Edge end node", schema, defaultSpace, row)

  private def extractDirectRelationReferenceFromPropertyStruct(
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
      case (_, None) =>
        Left(new CdfSparkException(s"""
                                                      |'$propertyName' ($descriptiveName) cannot contain null values.
                                                      |Please verify that 'externalId' values are not null for '$propertyName'
                                                      |in data row: ${rowToString(struct)}
                                                      |""".stripMargin))
      case (None, _) =>
        Left(new CdfSparkException(s"""
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
      extractDirectRelationReferenceFromPropertyStruct(
        propertyName,
        descriptiveName,
        defaultSpace,
        struct)
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

  def extractInstancePropertyValue(propType: DataType, value: InstancePropertyValue): Any =
    (propType, value) match {
      case (StringType, InstancePropertyValue.Date(v)) => v.toString
      case (StringType, InstancePropertyValue.Timestamp(v)) => v.toString
      case (StringType, InstancePropertyValue.Enum(v)) => v
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
      case (_, InstancePropertyValue.Enum(value)) => value
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
        value.map(r => Array(r.space, r.externalId)).toArray
    }

  def extractInstancePropertyValues(
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

  def propertyDefinitionToInstancePropertyValue(
      row: Row,
      schema: StructType,
      propertyName: String,
      propDef: ViewPropertyDefinition,
      instanceSpace: Option[String]): Either[CdfSparkException, OptionalField[InstancePropertyValue]] = {
    val instancePropertyValueResult = propDef match {
      case corePropDef: PropertyDefinition.ViewCorePropertyDefinition =>
        corePropDef.`type` match {
          case t: ListablePropertyType if t.isList =>
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

  def toInstancePropertyValueOfList(
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
            val strSeq = getListPropAsSeq[Any](propertyName, row, i).map(String.valueOf)
            InstancePropertyValue.StringList(skipNulls(strSeq))
          }).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Boolean, _) =>
          Try({
            val boolSeq = getListPropAsSeq[Boolean](propertyName, row, i)
            InstancePropertyValue.BooleanList(boolSeq)
          }).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Float32, _) =>
          val floatSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(floatSeq, propertyName, "Float", safeConvertToFloat)
            .map(InstancePropertyValue.Float32List)
        case _ @PrimitiveProperty(PrimitivePropType.Float64, _) =>
          val doubleSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(doubleSeq, propertyName, "Double", safeConvertToDouble)
            .map(InstancePropertyValue.Float64List)
        case _ @PrimitiveProperty(PrimitivePropType.Int32, _) =>
          val intSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(intSeq, propertyName, "Int", safeConvertToInt)
            .map(InstancePropertyValue.Int32List)
        case _ @PrimitiveProperty(PrimitivePropType.Int64, _) =>
          val longSeq = getListPropAsSeq[Any](propertyName, row, i)
          tryConvertNumberSeq(longSeq, propertyName, "Long", safeConvertToLong)
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

  def toInstancePropertyValueOfNonList(
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
        case _: EnumProperty =>
          Try(InstancePropertyValue.Enum(String.valueOf(row.get(i)))).toEither
        case _: TextProperty =>
          Try(InstancePropertyValue.String(String.valueOf(row.get(i)))).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Boolean, _) =>
          Try(InstancePropertyValue.Boolean(row.getBoolean(i))).toEither
        case _ @PrimitiveProperty(PrimitivePropType.Float32, _) =>
          tryConvertNumber(row.get(i), propertyName, "Float", safeConvertToFloat)
            .map(InstancePropertyValue.Float32)
        case _ @PrimitiveProperty(PrimitivePropType.Float64, _) =>
          tryConvertNumber(row.get(i), propertyName, "Double", safeConvertToDouble)
            .map(InstancePropertyValue.Float64)
        case _ @PrimitiveProperty(PrimitivePropType.Int32, _) =>
          tryConvertNumber(row.get(i), propertyName, "Int", safeConvertToInt)
            .map(InstancePropertyValue.Int32)
        case _ @PrimitiveProperty(PrimitivePropType.Int64, _) =>
          tryConvertNumber(row.get(i), propertyName, "Long", safeConvertToLong)
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

  def extractSpaceOrDefault(
      schema: StructType,
      row: Row,
      defaultSpace: Option[String]): Either[CdfSparkIllegalArgumentException, String] =
    defaultSpace.map(Right(_)).getOrElse(extractSpace(schema, row)).leftMap { e =>
      new CdfSparkIllegalArgumentException(
        s"""
           |There's no 'instanceSpace' specified to be used as default space and could not extract 'space' from data.
           | If the intention is to not use a default space then please make sure the data is correct.
           | ${e.getMessage}
           |""".stripMargin
      )
    }

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

  private def getListPropAsSeq[T](propertyName: String, row: Row, index: Integer): Seq[T] =
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

  private def tryAsDirectNodeRelationList(
      value: Seq[Row],
      propertyName: String,
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[DirectRelationReference]] =
    skipNulls(value).toVector
      .traverse(extractDirectRelations(propertyName, "Direct Node Relation", instanceSpace, _))

  private def extractDirectRelations(
      propertyName: String,
      descriptiveName: String,
      instanceSpace: Option[String],
      row: Row): Either[CdfSparkException, DirectRelationReference] =
    Try {
      extractDirectRelationReferenceFromPropertyStruct(propertyName, descriptiveName, instanceSpace, row)
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

  def rowToString(row: Row): String =
    Try(row.json).getOrElse(row.mkString(", "))

}
