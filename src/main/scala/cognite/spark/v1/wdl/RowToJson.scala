package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import cognite.spark.v1.wdl.JsonObjectToRow.RequiredOption
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import scala.collection.mutable

// sscalastyle:off
object RowToJson {

  /**
    * Creates a JsonObject from the values of `row` with the schema of `schema`.
    * The row must have a schema, because it matches the `row` with `schema` on the schema keys.
    *
    * @param row A GenericRowWithSchema coming from apache spark.
    * @param schema The <i>output</i> schema of the generated JsonObject.
    *
    * @return a JsonObject
    */
  def toJsonObject(row: Row, schema: StructType, fieldName: Option[String]): JsonObject =
    if (row == null) {
      JsonObject.empty
    } else if (row.schema == null) {
      throw new CdfSparkException(
        s"Schema for $row is null. The input row needs a schema, because it must be matched with the schema of the output format.")
    } else {
      val rowFields = row.schema.map(f => f.name -> row.get(row.fieldIndex(f.name))).toMap
      val jsonFields = schema.toList
        .flatMap(
          structField => {
            val childFieldName = fieldName.map(_ + ".").getOrElse("") + structField.name
            rowFields
              .get(structField.name)
              .map(rowField => {
                val converted = try {
                  convertToJson(rowField, structField.dataType, childFieldName, structField.nullable)
                } catch {
                  case e: CdfSparkException =>
                    throw new CdfSparkException(s"${e.getMessage}. Row: `$row`", e)
                }
                structField.name -> converted
              })
              .orThrow(childFieldName, structField.nullable, structField.dataType)
          }
        )
        .toMap
      JsonObject.fromMap(jsonFields)
    }

  def toJson(row: Row, schema: StructType, fieldName: Option[String] = None): Json = {
    val jsonObject = toJsonObject(row, schema, fieldName)
    if (jsonObject.isEmpty) {
      Json.Null
    } else {
      Json.fromJsonObject(jsonObject)
    }
  }

  private lazy val dateFormatter = DateFormatter()
  private lazy val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
  private lazy val timestampFormatter = TimestampFormatter(zoneId)

  // Convert an iterator of values to a json array
  private def iteratorToJsonArray(
      fieldName: String,
      iterator: Iterator[_],
      elementType: DataType): Json =
    Json.fromValues(iterator.map(value => toJsonHelper(fieldName, value, elementType)).toList)

  // Convert a value to json.
  // scalastyle:off cyclomatic.complexity
  private def toJsonHelper(fieldName: String, value: Any, dataType: DataType): Json =
    (value, dataType) match {
      case (null, _) => Json.Null // scalastyle:ignore
      case (None, _) => Json.Null
      case (b: Boolean, _) => Json.fromBoolean(b)
      case (b: Byte, _) => Json.fromInt(b.toInt)
      case (s: Short, _) => Json.fromInt(s.toInt)
      case (i: Int, _) => Json.fromInt(i)
      case (l: Long, _) => Json.fromLong(l)
      case (f: Float, _) => Json.fromFloatOrNull(f)
      case (d: Double, _) => Json.fromDoubleOrNull(d)
      case (d: BigDecimal, _) => Json.fromBigDecimal(d)
      case (d: java.math.BigDecimal, _) => Json.fromBigDecimal(d)
      case (d: Decimal, _) => Json.fromBigDecimal(d.toBigDecimal)
      case (s: String, _) => Json.fromString(s)
      case (d: LocalDate, _) => Json.fromString(dateFormatter.format(d))
      case (d: Date, _) => Json.fromString(dateFormatter.format(d))
      case (i: Instant, _) => Json.fromString(timestampFormatter.format(i))
      case (t: Timestamp, _) => Json.fromString(timestampFormatter.format(t))
      case (i: CalendarInterval, _) => Json.fromString(i.toString)
      case (a: Array[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(fieldName, a.iterator, elementType)
      case (a: mutable.ArraySeq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(fieldName, a.iterator, elementType)
      case (s: Seq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(fieldName, s.iterator, elementType)
      case (m: Map[String @unchecked, _], MapType(StringType, valueType, _)) =>
        Json.fromFields(m.toList.sortBy(_._1).map {
          case (k, v) => k -> toJsonHelper(fieldName + ".value", v, valueType)
        })
      case (m: Map[_, _], MapType(keyType, valueType, _)) =>
        Json.fromValues(m.iterator.map {
          case (k, v) =>
            val key = toJsonHelper(fieldName + ".key", k, keyType)
            val value = toJsonHelper(fieldName + ".value", v, valueType)
            Json.fromFields(
              "key" -> key ::
                "value" -> value ::
                Nil
            )
        }.toList)
      case (r: Row, s: StructType) => toJson(r, s, Some(fieldName))
      case (badValue, dataType) =>
        throw new WrongFieldTypeException(fieldName, dataType, badValue)
    }
  // scalastyle:on cyclomatic.complexity

  private def convertToJson(
      dataValue: Any,
      dataType: DataType,
      fieldName: String,
      nullable: Boolean): Json =
    if (dataValue == null) {
      if (nullable) {
        Json.Null
      } else {
        throw new RequiredFieldIsNullException(fieldName, dataType)
      }
    } else {
      val json = toJsonHelper(fieldName, dataValue, dataType)
      if (json.isNull && !nullable) {
        throw new RequiredFieldIsNullException(fieldName, dataType)
      }
      json
    }
}
