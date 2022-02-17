package cognite.spark.v1
import com.cognite.sdk.scala.v1.RawRow
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types._

import java.util
import java.util.{Base64, Locale}

object RawJsonConverter {
  // scalastyle:off cyclomatic.complexity
  /** Maps the object from Spark into circe Json object. This is the types we need to cover (according to Row.get javadoc):
    * BooleanType -> java.lang.Boolean
       ByteType -> java.lang.Byte
       ShortType -> java.lang.Short
       IntegerType -> java.lang.Integer
       LongType -> java.lang.Long
       FloatType -> java.lang.Float
       DoubleType -> java.lang.Double
       StringType -> String
       DecimalType -> java.math.BigDecimal

       DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
       DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true

       TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
       TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true

       BinaryType -> byte array
       ArrayType -> scala.collection.Seq (use getList for java.util.List)
       MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
       StructType -> org.apache.spark.sql.Row */
  def anyToRawJson(v: Any): Json =
    v match {
      case null => Json.Null // scalastyle:off null
      case v: java.lang.Boolean => Json.fromBoolean(v.booleanValue)
      case v: java.lang.Float => Json.fromFloatOrString(v.floatValue)
      case v: java.lang.Double => Json.fromDoubleOrString(v.doubleValue)
      case v: java.math.BigDecimal => Json.fromBigDecimal(v)
      case v: java.lang.Number => Json.fromLong(v.longValue())
      case v: String => Json.fromString(v)
      case v: java.sql.Date => Json.fromString(v.toString)
      case v: java.time.LocalDate => Json.fromString(v.toString)
      case v: java.sql.Timestamp => Json.fromString(v.toString)
      case v: java.time.Instant => Json.fromString(v.toString)
      case v: Array[Byte] =>
        throw new CdfSparkIllegalArgumentException(
          "BinaryType is not supported when writing raw, please convert it to base64 string or array of numbers")
      case v: Iterable[Any] => Json.arr(v.toSeq.map(anyToRawJson): _*)
      case v: Map[Any @unchecked, Any @unchecked] =>
        Json.obj(v.toSeq.map(x => x._1.toString -> anyToRawJson(x._2)): _*)
      case v: Row => rowToJson(v)
      case _ =>
        throw new CdfSparkIllegalArgumentException(
          s"Value $v of type ${v.getClass} can not be written to RAW")
    }

  def rowToJson(r: Row): Json =
    if (r.schema != null) {
      Json.obj(r.schema.fieldNames.map(f => f -> anyToRawJson(r.getAs[Any](f))): _*)
    } else {
      Json.arr(r.toSeq.map(anyToRawJson): _*)
    }

  def rowsToRawItems(
      nonKeyColumnNames: Seq[String],
      keyColumnName: String,
      rows: Seq[Row]): Seq[RawRow] =
    rows.map { row =>
      val key = row.getString(row.fieldIndex(keyColumnName))
      if (key == null) {
        throw new CdfSparkIllegalArgumentException("\"key\" can not be null.")
      }
      RawRow(
        key,
        nonKeyColumnNames.map(f => f -> anyToRawJson(row.getAs[Any](f))).toMap
      )
    }

  case class RawJsonColumn(jsonName: String, columnName: String, dataType: DataType)

  def untypedRowConverter(row: RawRow): Row =
    new GenericRowWithSchema(
      Array(
        row.key,
        row.lastUpdatedTime.map(java.sql.Timestamp.from).orNull,
        JsonObject(row.columns.toSeq: _*).asJson.noSpaces
      ),
      RawTableRelation.defaultSchema
    )

  def makeRowConverter(
      sparkSchema: StructType,
      jsonFieldNames: Array[String],
      lastUpdatedTimeColumn: String,
      keyColumn: String): RawRow => Row = {
    assert(sparkSchema.fieldNames(0) == keyColumn, "Key must be the first column")
    assert(
      sparkSchema.fieldNames(1) == lastUpdatedTimeColumn,
      "LastUpdatedTime must be the second column")

    val fieldConverters = sparkSchema.fields.drop(2).map(f => makeConverter(f.dataType))
    val nameMap = new java.util.HashMap[String, Int]()
    for ((field, i) <- jsonFieldNames.zipWithIndex) {
      nameMap.put(field, i)
    }

    row =>
      {
        val rowArray = new Array[Any](sparkSchema.fields.length)
        rowArray(0) = row.key
        rowArray(1) = row.lastUpdatedTime.map(java.sql.Timestamp.from).orNull
        for ((key, value) <- row.columns) {
          val fieldIndex = nameMap.getOrDefault(key, -1)
          if (fieldIndex >= 0) {
            try {
              rowArray(fieldIndex + 2) = fieldConverters(fieldIndex).convertNullSafe(value)
            } catch {
              case e: Exception =>
                throw new SparkRawRowMappingException(key, row, e)
            }
          }
        }
        new GenericRowWithSchema(rowArray, sparkSchema)
      }
  }

  // private lazy val timestampFormatter = TimestampFormatter(
  //   None,
  //   java.time.ZoneOffset.UTC,
  //   Locale.US,
  //   legacyFormat = FAST_DATE_FORMAT,
  //   isParsing = true)
  // private lazy val timestampNTZFormatter = TimestampFormatter(
  //   options.timestampFormatInRead,
  //   options.zoneId,
  //   legacyFormat = FAST_DATE_FORMAT,
  //   isParsing = true,
  //   forTimestampNTZ = true)
  // private lazy val dateFormatter = DateFormatter(
  //   options.dateFormatInRead,
  //   options.locale,
  //   legacyFormat = FAST_DATE_FORMAT,
  //   isParsing = true)

  trait ValueConverter {
    def convert(j: Json): AnyRef

    def convertNullSafe(j: Json): AnyRef =
      if (j.isNull) {
        null
      } else {
        convert(j)
      }
  }

  private def mappingError(t: DataType, v: Json): Nothing =
    throw new CdfSparkIllegalArgumentException(s"Expected value of type $t but got $v.")

  def makeObjectConverter(st: StructType, jsonNames: Array[String]): Iterator[(String, Json)] => Row = {
    val fieldConverters = st.fields.map(f => makeConverter(f.dataType))
    val nameMap = new java.util.HashMap[String, Int]()
    for ((field, i) <- jsonNames.zipWithIndex) {
      nameMap.put(field, i)
    }

    fields =>
      {
        val rowArray = new Array[Any](fieldConverters.length)
        for ((key, value) <- fields) {
          val fieldIndex = nameMap.getOrDefault(key, -1)
          if (fieldIndex >= 0) {
            rowArray(fieldIndex) = fieldConverters(fieldIndex).convertNullSafe(value)
          }
        }
        new GenericRowWithSchema(rowArray, st)
      }
  }

  // scalastyle:off method.length
  /** Copied and adjusted from Spark's JacksonParser
    * We want to use Circe's model, because that's what Scala SDK uses,
    * so we don't need to go through a conversion back to JSON string and then parse it again with Jackson
    *
    * Create a converter which converts the JSON documents held by the `JsonParser`
    * to a value according to a desired schema.
    */
  def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType => {
      case Json.True => java.lang.Boolean.TRUE
      case Json.False => java.lang.Boolean.FALSE
      case j: Json =>
        j.asString
          .map {
            case "" => null
            case s => java.lang.Boolean.valueOf(java.lang.Boolean.parseBoolean(s))
          }
          .getOrElse(mappingError(dataType, j))
    }

    case ByteType =>
      (j: Json) =>
        j.asNumber
          .flatMap(_.toByte)
          .map(java.lang.Byte.valueOf)
          .orElse(j.asString.map {
            case "" => null
            case s => java.lang.Byte.valueOf(java.lang.Byte.parseByte(s))
          })
          .getOrElse(mappingError(dataType, j))

    case ShortType =>
      (j: Json) =>
        j.asNumber
          .flatMap(_.toShort)
          .map(java.lang.Short.valueOf)
          .orElse(j.asString.map {
            case "" => null
            case s => java.lang.Short.valueOf(java.lang.Short.parseShort(s))
          })
          .getOrElse(mappingError(dataType, j))

    case IntegerType =>
      (j: Json) =>
        j.asNumber
          .flatMap(_.toInt)
          .map(java.lang.Integer.valueOf)
          .orElse(j.asString.map {
            case "" => null
            case s => java.lang.Integer.valueOf(java.lang.Integer.parseInt(s))
          })
          .getOrElse(mappingError(dataType, j))

    case LongType =>
      (j: Json) =>
        j.asNumber
          .flatMap(_.toLong)
          .map(java.lang.Long.valueOf)
          .orElse(j.asString.map {
            case "" => null
            case s => java.lang.Long.valueOf(java.lang.Long.parseLong(s))
          })
          .getOrElse(mappingError(dataType, j))

    case FloatType =>
      (j: Json) =>
        j.asNumber
          .map(n => java.lang.Float.valueOf(n.toFloat))
          .orElse(j.asString.map {
            case "" => null
            case s => java.lang.Float.valueOf(java.lang.Float.parseFloat(s))
          })
          .getOrElse(mappingError(dataType, j))

    case DoubleType =>
      (j: Json) =>
        j.asNumber
          .map(n => java.lang.Double.valueOf(n.toDouble))
          .orElse(j.asString.map {
            case "" => null
            case s => java.lang.Double.valueOf(java.lang.Double.parseDouble(s))
          })
          .getOrElse(mappingError(dataType, j))

    case StringType =>
      (j: Json) =>
        j.asString
        // just format as json if it's not string
          .getOrElse(j.noSpaces)

    // date and time types are probably not usable from RAW anyway, and it's not easy to implement them in the same
    // way as Spark, so I'm leaving these cases commented out for now

    // case TimestampType =>
    //   (j: Json) =>
    //     j.asString.map(java.sql.Timestamp.valueOf)
    //       .orElse(j.asNumber.map(v => new Timestamp(v.toLong)))
    //       .getOrElse(mappingError(dataType, j))
    //   (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
    //     case VALUE_STRING if parser.getTextLength >= 1 =>
    //       try {
    //         timestampFormatter.parse(parser.getText)
    //       } catch {
    //         case NonFatal(e) =>
    //           // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
    //           // compatibility.
    //           val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(parser.getText))
    //           DateTimeUtils.stringToTimestamp(str, options.zoneId).getOrElse(throw e)
    //       }

    //     case VALUE_NUMBER_INT =>
    //       parser.getLongValue * 1000000L
    //   }

    // case TimestampNTZType =>
    //   (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
    //     case VALUE_STRING if parser.getTextLength >= 1 =>
    //       timestampNTZFormatter.parseWithoutTimeZone(parser.getText)
    //   }

    // case DateType =>
    //   (parser: JsonParser) => parseJsonToken[java.lang.Integer](parser, dataType) {
    //     case VALUE_STRING if parser.getTextLength >= 1 =>
    //       try {
    //         dateFormatter.parse(parser.getText)
    //       } catch {
    //         case NonFatal(e) =>
    //           // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
    //           // compatibility.
    //           val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(parser.getText))
    //           DateTimeUtils.stringToDate(str).getOrElse {
    //             // In Spark 1.5.0, we store the data as number of days since epoch in string.
    //             // So, we just convert it to Int.
    //             try {
    //               RebaseDateTime.rebaseJulianToGregorianDays(parser.getText.toInt)
    //             } catch {
    //               case _: NumberFormatException => throw e
    //             }
    //           }.asInstanceOf[Integer]
    //       }
    //   }

    case BinaryType =>
      (j: Json) =>
        j.asString.map(Base64.getDecoder.decode).getOrElse(mappingError(dataType, j))

    case dt: DecimalType =>
      (j: Json) =>
        j.asNumber
          .flatMap(_.toBigDecimal)
          .map(_.underlying())
          .orElse(j.asString.map(str => new java.math.BigDecimal(str)))
          .getOrElse(mappingError(dataType, j))

    // case CalendarIntervalType => (parser: JsonParser) =>
    //   parseJsonToken[CalendarInterval](parser, dataType) {
    //     case VALUE_STRING =>
    //       IntervalUtils.safeStringToInterval(UTF8String.fromString(parser.getText))
    //   }

    // case ym: YearMonthIntervalType => (parser: JsonParser) =>
    //   parseJsonToken[Integer](parser, dataType) {
    //     case VALUE_STRING =>
    //       val expr = Cast(Literal(parser.getText), ym)
    //       Integer.valueOf(expr.eval(EmptyRow).asInstanceOf[Int])
    //   }

    // case dt: DayTimeIntervalType => (parser: JsonParser) =>
    //   parseJsonToken[java.lang.Long](parser, dataType) {
    //     case VALUE_STRING =>
    //       val expr = Cast(Literal(parser.getText), dt)
    //       java.lang.Long.valueOf(expr.eval(EmptyRow).asInstanceOf[Long])
    //   }

    case st: StructType =>
      val objConverter = makeObjectConverter(st, st.fieldNames)

      (j: Json) =>
        j.asObject
          .map { o =>
            objConverter(o.toIterable.iterator)
          }
          .getOrElse(mappingError(dataType, j))

    case at: ArrayType =>
      val innerConverter = makeConverter(at.elementType)
      (j: Json) =>
        j.asArray
          .map(v => v.map(innerConverter.convertNullSafe))
          .getOrElse(mappingError(dataType, j))

    case mt: MapType =>
      val valueConverter = makeConverter(mt.valueType)
      (j: Json) =>
        j.asObject
          .map { o =>
            o.toMap.mapValues(valueConverter.convertNullSafe)
          }
          .getOrElse(mappingError(dataType, j))

    case udt: UserDefinedType[_] =>
      makeConverter(udt.sqlType)

    case _: NullType =>
      (j: Json) =>
        null

    // We don't actually hit this exception though, we keep it for understandability
    case _ =>
      throw new CdfSparkIllegalArgumentException(
        s"Unsupported type for reading RAW: ${dataType.typeName}. " +
          s"If this is a problem for you (for example, if it used to work in the past), please report it as a bug.");
  }

}

class SparkRawRowMappingException(val column: String, val row: RawRow, cause: Throwable)
    extends CdfSparkException(
      s"Error while loading RAW row [key='${row.key}'] in column '$column': $cause",
      cause)
