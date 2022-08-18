package cognite.spark.v1

import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}

import com.cognite.sdk.scala.v1.DataModelType.NodeType
import com.cognite.sdk.scala.v1.{DataModelProperty, DataModelType, PropertyType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

// scalastyle:off cyclomatic.complexity
object AlphaDataModelInstancesHelper {
  private def unknownPropertyTypeMessage(a: Any) = s"Unknown property type $a."

  private def notValidPropertyTypeMessage(
      a: Any,
      propertyType: String,
      sparkSqlType: Option[String] = None) = {
    val sparkSqlTypeMessage = sparkSqlType
      .map(
        tname =>
          s" Try to cast the value to $tname. " +
            s"For example, ‘$tname(col_name) as prop_name’ or ‘cast(col_name as $tname) as prop_name’.")
      .getOrElse("")

    s"$a of type ${a.getClass} is not a valid $propertyType.$sparkSqlTypeMessage"
  }
  def propertyNotNullableMessage(propertyType: PropertyType[_]): String =
    s"Property of ${propertyType.code} type is not nullable."

  def parsePropertyValue(value: Any): DataModelProperty[_] = value match {
    case x: Double => PropertyType.Float64.Property(x)
    case x: Int => PropertyType.Int32.Property(x)
    case x: Float => PropertyType.Float32.Property(x)
    case x: Long => PropertyType.Int64.Property(x)
    case x: java.math.BigDecimal => PropertyType.Float64.Property(x.doubleValue)
    case x: java.math.BigInteger => PropertyType.Int64.Property(x.longValue)
    case x: BigDecimal => PropertyType.Float64.Property(x.doubleValue)
    case x: BigInt => PropertyType.Int64.Property(x.longValue)
    case x: String => PropertyType.Text.Property(x)
    case x: Boolean => PropertyType.Boolean.Property(x)
    case x: Array[Double] => PropertyType.Array.Float64.Property(x.toIndexedSeq)
    case x: Array[Int] => PropertyType.Array.Int32.Property(x.toIndexedSeq)
    case x: Array[Float] => PropertyType.Array.Float32.Property(x.toIndexedSeq)
    case x: Array[Long] => PropertyType.Array.Int64.Property(x.toIndexedSeq)
    case x: Array[String] => PropertyType.Array.Text.Property(x.toIndexedSeq)
    case x: Array[Boolean] => PropertyType.Array.Boolean.Property(x.toIndexedSeq)
    case x: Array[java.math.BigDecimal] =>
      PropertyType.Array.Float64.Property(x.map(i => i.doubleValue).toIndexedSeq)
    case x: Array[java.math.BigInteger] =>
      PropertyType.Array.Int64.Property(x.map(i => i.longValue).toIndexedSeq)
    case x: Array[BigDecimal] =>
      PropertyType.Array.Float64.Property(x.map(i => i.doubleValue).toIndexedSeq)
    case x: Array[BigInt] => PropertyType.Array.Int64.Property(x.map(i => i.longValue).toIndexedSeq)
    case x: LocalDate => PropertyType.Date.Property(x)
    case x: java.sql.Date => PropertyType.Date.Property(x.toLocalDate)
    case x: LocalDateTime => PropertyType.Date.Property(x.toLocalDate)
    case x: Instant =>
      PropertyType.Timestamp.Property(OffsetDateTime.ofInstant(x, ZoneId.of("UTC")).toZonedDateTime)
    case x: java.sql.Timestamp =>
      PropertyType.Timestamp.Property(
        OffsetDateTime.ofInstant(x.toInstant, ZoneId.of("UTC")).toZonedDateTime)
    case x: java.time.ZonedDateTime =>
      PropertyType.Timestamp.Property(x)
    case x: Array[LocalDate] => PropertyType.Array.Date.Property(x.toIndexedSeq)
    case x: Array[java.sql.Date] => PropertyType.Array.Date.Property(x.map(_.toLocalDate).toIndexedSeq)
    case x: Array[LocalDateTime] => PropertyType.Array.Date.Property(x.map(_.toLocalDate).toIndexedSeq)
    case x: Array[Instant] =>
      PropertyType.Array.Timestamp.Property(
        x.map(OffsetDateTime.ofInstant(_, ZoneId.of("UTC")).toZonedDateTime).toIndexedSeq
      )
    case x: Array[java.sql.Timestamp] =>
      PropertyType.Array.Timestamp.Property(
        x.map(ts => OffsetDateTime.ofInstant(ts.toInstant, ZoneId.of("UTC")).toZonedDateTime)
          .toIndexedSeq
      )
    case x: Array[java.time.ZonedDateTime] =>
      PropertyType.Array.Timestamp.Property(x.toIndexedSeq)
    case x =>
      throw new CdfSparkException(s"Unsupported value ${x.toString} of type ${x.getClass.getName}")
  }

  def propertyTypeToSparkType(propertyType: PropertyType[_]): DataType =
    propertyType match {
      case PropertyType.Text =>
        DataTypes.StringType
      case PropertyType.Boolean => DataTypes.BooleanType
      case PropertyType.Numeric | PropertyType.Float64 => DataTypes.DoubleType
      case PropertyType.Float32 => DataTypes.FloatType
      case PropertyType.Int => DataTypes.IntegerType
      case PropertyType.Int32 => DataTypes.IntegerType
      case PropertyType.Int64 => DataTypes.LongType
      case PropertyType.Bigint => DataTypes.LongType
      case PropertyType.Date => DataTypes.DateType
      case PropertyType.Timestamp => DataTypes.TimestampType
      case PropertyType.DirectRelation => DataTypes.StringType
      case PropertyType.Geometry => DataTypes.StringType
      case PropertyType.Geography => DataTypes.StringType
      case PropertyType.Array.Text => DataTypes.createArrayType(DataTypes.StringType)
      case PropertyType.Array.Boolean => DataTypes.createArrayType(DataTypes.BooleanType)
      case PropertyType.Array.Numeric | PropertyType.Array.Float64 =>
        DataTypes.createArrayType(DataTypes.DoubleType)
      case PropertyType.Array.Float32 => DataTypes.createArrayType(DataTypes.FloatType)
      case PropertyType.Array.Int => DataTypes.createArrayType(DataTypes.IntegerType)
      case PropertyType.Array.Int32 => DataTypes.createArrayType(DataTypes.IntegerType)
      case PropertyType.Array.Int64 => DataTypes.createArrayType(DataTypes.LongType)
      case PropertyType.Array.Bigint => DataTypes.createArrayType(DataTypes.LongType)
      case a => throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }

  private def toDateProperty: Any => DataModelProperty[_] = {
    case x: LocalDate => PropertyType.Date.Property(x)
    case x: java.sql.Date => PropertyType.Date.Property(x.toLocalDate)
    case x: LocalDateTime => PropertyType.Date.Property(x.toLocalDate)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "date", Some("date")))
  }

  private def toTimestampProperty: Any => DataModelProperty[_] = {
    case x: Instant =>
      PropertyType.Timestamp.Property(OffsetDateTime.ofInstant(x, ZoneId.of("UTC")).toZonedDateTime)
    case x: java.sql.Timestamp =>
      // Using ZonedDateTime directly without OffSetDateTime, adds ZoneId to the end of encoded string
      // 2019-10-02T07:00+09:00[Asia/Seoul] instead of 2019-10-02T07:00+09:00, API does not like it.
      PropertyType.Timestamp.Property(
        OffsetDateTime.ofInstant(x.toInstant, ZoneId.of("UTC")).toZonedDateTime)
    case x: ZonedDateTime =>
      PropertyType.Timestamp.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Timestamp.code, Some("timestamp")))
  }

  private def toDirectRelationProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.DirectRelation.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.DirectRelation.code, Some("string")))
  }

  private def toGeographyProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.Geography.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Geography.code, Some("string")))
  }

  private def toGeometryProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.Geometry.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Geometry.code, Some("string")))
  }

  private def toFloat32Property: Any => DataModelProperty[_] = {
    case x: Float => PropertyType.Float32.Property(x)
    case x: Int => PropertyType.Float32.Property(x.toFloat)
    case x: java.math.BigDecimal => PropertyType.Float32.Property(x.floatValue())
    case x: java.math.BigInteger => PropertyType.Float32.Property(x.floatValue())
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Float32.code, Some("float")))
  }

  private def toFloat64Property(propAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Double => PropertyType.Float64.Property(x)
    case x: Float => PropertyType.Float64.Property(x.toDouble)
    case x: Int => PropertyType.Float64.Property(x.toDouble)
    case x: Long => PropertyType.Float64.Property(x.toDouble)
    case x: java.math.BigDecimal => PropertyType.Float64.Property(x.doubleValue)
    case x: java.math.BigInteger => PropertyType.Float64.Property(x.doubleValue)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propAlias.code, Some("double")))
  }

  private def toBooleanProperty: Any => DataModelProperty[_] = {
    case x: Boolean => PropertyType.Boolean.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Boolean.code, Some("boolean")))
  }

  private def toInt32Property(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Int => PropertyType.Int.Property(x)
    case x: java.math.BigInteger => PropertyType.Int.Property(x.intValue())
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code, Some("int")))
  }

  private def toInt64Property(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Int => PropertyType.Bigint.Property(x.toLong)
    case x: Long => PropertyType.Bigint.Property(x)
    case x: java.math.BigInteger => PropertyType.Bigint.Property(x.longValue)
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code, Some("bigint")))
  }

  private def toStringProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.Text.Property(x)
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, PropertyType.Text.code, Some("string")))
  }

  private def toStringArrayProperty: Any => DataModelProperty[_] = {
    case x: Iterable[_] =>
      x.headOption match {
        case None => PropertyType.Array.Boolean.Property(Vector.empty)
        case Some(_: String) =>
          PropertyType.Array.Text.Property(x.collect { case s: String => s }.toVector)
        case _ =>
          throw new CdfSparkException(notValidPropertyTypeMessage(x, PropertyType.Array.Text.code))
      }
    case x =>
      throw new CdfSparkException(notValidPropertyTypeMessage(x, PropertyType.Array.Text.code))
  }

  private def toFloat32ArrayProperty: Any => DataModelProperty[_] = {
    case x: Iterable[_] =>
      x.headOption match {
        case None => PropertyType.Array.Float32.Property(Vector.empty)
        case Some(_: Float) =>
          PropertyType.Array.Float32.Property(x.collect { case f: Float => f }.toVector)
        case Some(_: Int) =>
          PropertyType.Array.Float32.Property(x.collect { case i: Int => i.toFloat }.toVector)
        case Some(_: java.math.BigDecimal) =>
          PropertyType.Array.Float32.Property(x.collect {
            case bigDecimal: java.math.BigDecimal => bigDecimal.floatValue()
          }.toVector)
        case Some(_: java.math.BigInteger) =>
          PropertyType.Array.Float32.Property(x.collect {
            case bigInteger: java.math.BigInteger => bigInteger.floatValue()
          }.toVector)
        case x =>
          throw new CdfSparkException(notValidPropertyTypeMessage(x, PropertyType.Array.Float32.code))
      }
    case x =>
      throw new CdfSparkException(notValidPropertyTypeMessage(x, PropertyType.Array.Float32.code))
  }

  private def toFloat64ArrayProperty(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Iterable[_] =>
      x.headOption match {
        case None => PropertyType.Array.Float64.Property(Vector.empty)
        case Some(_: Double) =>
          PropertyType.Array.Float64.Property(x.collect { case d: Double => d }.toVector)
        case Some(_: Float) =>
          PropertyType.Array.Float64.Property(x.collect { case f: Float => f.toDouble }.toVector)
        case Some(_: Long) =>
          PropertyType.Array.Float64.Property(x.collect { case l: Long => l.toDouble }.toVector)
        case Some(_: Int) =>
          PropertyType.Array.Float64.Property(x.collect { case i: Int => i.toDouble }.toVector)
        case Some(_: java.math.BigDecimal) =>
          PropertyType.Array.Float64.Property(x.collect {
            case bigDecimal: java.math.BigDecimal => bigDecimal.doubleValue()
          }.toVector)
        case Some(_: java.math.BigInteger) =>
          PropertyType.Array.Float64.Property(x.collect {
            case bigInteger: java.math.BigInteger => bigInteger.doubleValue()
          }.toVector)
        case _ => throw new CdfSparkException(notValidPropertyTypeMessage(x, propertyAlias.code))
      }
    case x => throw new CdfSparkException(notValidPropertyTypeMessage(x, propertyAlias.code))
  }

  private def toBooleanArrayProperty: Any => DataModelProperty[_] = {
    case x: Iterable[_] =>
      x.headOption match {
        case None => PropertyType.Array.Boolean.Property(Vector.empty)
        case Some(_: Boolean) =>
          PropertyType.Array.Boolean.Property(x.collect { case bool: Boolean => bool }.toVector)
        case _ =>
          throw new CdfSparkException(notValidPropertyTypeMessage(x, PropertyType.Array.Boolean.code))
      }
    case x =>
      throw new CdfSparkException(notValidPropertyTypeMessage(x, PropertyType.Array.Boolean.code))
  }

  private def toInt32ArrayProperty(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Iterable[_] =>
      x.headOption match {
        case None => PropertyType.Array.Int32.Property(Vector.empty)
        case Some(_: Int) =>
          PropertyType.Array.Int32.Property(x.collect { case i: Int => i }.toVector)
        case Some(_: java.math.BigInteger) =>
          PropertyType.Array.Int32.Property(x.collect {
            case bigInteger: java.math.BigInteger => bigInteger.intValue()
          }.toVector)
        case _ => throw new CdfSparkException(notValidPropertyTypeMessage(x, propertyAlias.code))
      }
    case x => throw new CdfSparkException(notValidPropertyTypeMessage(x, propertyAlias.code))
  }

  def toInt64ArrayProperty(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Iterable[_] =>
      x.headOption match {
        case None => PropertyType.Array.Bigint.Property(Vector.empty)
        case Some(_: Int) =>
          PropertyType.Array.Bigint.Property(x.collect { case i: Int => i.toLong }.toVector)
        case Some(_: Long) =>
          PropertyType.Array.Bigint.Property(x.collect { case l: Long => l }.toVector)
        case Some(_: java.math.BigInteger) =>
          PropertyType.Array.Bigint.Property(x.collect {
            case bigInteger: java.math.BigInteger => bigInteger.longValue()
          }.toVector)
        case _ => throw new CdfSparkException(notValidPropertyTypeMessage(x, propertyAlias.code))
      }
    case x => throw new CdfSparkException(notValidPropertyTypeMessage(x, propertyAlias.code))
  }

  def toPropertyType(propertyType: PropertyType[_]): Any => DataModelProperty[_] =
    propertyType match {
      case PropertyType.Float32 => toFloat32Property
      case PropertyType.Float64 | PropertyType.Numeric => toFloat64Property(propertyType)
      case PropertyType.Boolean => toBooleanProperty
      case PropertyType.Int32 | PropertyType.Int => toInt32Property(propertyType)
      case PropertyType.Int64 | PropertyType.Bigint => toInt64Property(propertyType)
      case PropertyType.Text => toStringProperty
      case PropertyType.Date => toDateProperty
      case PropertyType.Timestamp => toTimestampProperty
      case PropertyType.DirectRelation => toDirectRelationProperty
      case PropertyType.Geometry => toGeometryProperty
      case PropertyType.Geography => toGeographyProperty
      case PropertyType.Array.Text => toStringArrayProperty
      case PropertyType.Array.Float32 => toFloat32ArrayProperty
      case PropertyType.Array.Float64 | PropertyType.Array.Numeric =>
        toFloat64ArrayProperty(propertyType)
      case PropertyType.Array.Boolean => toBooleanArrayProperty
      case PropertyType.Array.Int | PropertyType.Array.Int32 => toInt32ArrayProperty(propertyType)
      case PropertyType.Array.Int64 | PropertyType.Array.Bigint => toInt64ArrayProperty(propertyType)
      case a =>
        throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }

  def getRequiredStringPropertyIndex(
      rSchema: StructType,
      modelType: DataModelType,
      keyString: String): Int = {
    val typeString = if (modelType == NodeType) "node" else "edge"
    val index = rSchema.fieldNames.indexOf(keyString)
    if (index < 0) {
      throw new CdfSparkException(s"Can't upsert data model $typeString, `$keyString` is missing.")
    } else {
      index
    }
  }

  def getStringValueForFixedProperty(row: Row, keyString: String, index: Int): String =
    row.get(index) match {
      case x: String => x
      case _ =>
        throw SparkSchemaHelperRuntime.badRowError(row, keyString, "String", "")
    }
}
// scalastyle:on cyclomatic.complexity
