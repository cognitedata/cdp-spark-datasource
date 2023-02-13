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
  def toJsonObject(row: Row, schema: StructType): JsonObject =
    if (row == null) {
      JsonObject.empty
    } else if (row.schema == null) {
      throw new CdfSparkException(
        s"Schema for $row is null. The input row needs a schema, because it must be matched with the schema of the output format.")
    } else {
      val rowFields = row.schema.map(f => f.name -> row.get(row.fieldIndex(f.name))).toMap
      val jsonFields = schema.toList
        .flatMap(
          structField =>
            rowFields
              .get(structField.name)
              .map(rowField => {
                val converted =
                  convertToJson(rowField, structField.dataType, structField.name, structField.nullable)
                structField.name -> converted
              })
              .orThrow(structField.name, structField.nullable)
        )
        .toMap
      JsonObject.fromMap(jsonFields)
    }

  def toJson(row: Row, schema: StructType): Json = {
    val jsonObject = toJsonObject(row, schema)
    jsonObject.isEmpty match {
      case true => Json.Null
      case false => Json.fromJsonObject(jsonObject)
    }
  }

  private lazy val dateFormatter = DateFormatter()
  private lazy val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
  private lazy val timestampFormatter = TimestampFormatter(zoneId)

  // Convert an iterator of values to a json array
  private def iteratorToJsonArray(iterator: Iterator[_], elementType: DataType): Json =
    Json.fromValues(iterator.map(toJsonHelper(_, elementType)).toList)

  // Convert a value to json.
  // scalastyle:off cyclomatic.complexity
  private def toJsonHelper(value: Any, dataType: DataType): Json = (value, dataType) match {
    case (null, _) => Json.Null // scalastyle:ignore
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
      iteratorToJsonArray(a.iterator, elementType)
    case (a: mutable.ArraySeq[_], ArrayType(elementType, _)) =>
      iteratorToJsonArray(a.iterator, elementType)
    case (s: Seq[_], ArrayType(elementType, _)) =>
      iteratorToJsonArray(s.iterator, elementType)
    case (m: Map[String @unchecked, _], MapType(StringType, valueType, _)) =>
      Json.fromFields(m.toList.sortBy(_._1).map {
        case (k, v) => k -> toJsonHelper(v, valueType)
      })
    case (m: Map[_, _], MapType(keyType, valueType, _)) =>
      Json.fromValues(m.iterator.map {
        case (k, v) =>
          Json.fromFields(
            "key" -> toJsonHelper(k, keyType) :: "value" -> toJsonHelper(v, valueType) :: Nil)
      }.toList)
    case (r: Row, s: StructType) => toJson(r, s)
    case _ =>
      throw new CdfSparkException(
        s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType to JSON.")
  }
  // scalastyle:on cyclomatic.complexity

  private def convertToJson(
      dataValue: Any,
      dataType: DataType,
      structFieldName: String,
      nullable: Boolean): Json =
    if (dataValue == null) {
      if (nullable) {
        Json.Null
      } else {
        throw new CdfSparkException(s"Element ${structFieldName} should not be NULL.")
      }
    } else {
      val json = toJsonHelper(dataValue, dataType)
      if (json.isNull && !nullable) {
        throw new CdfSparkException(s"Element ${structFieldName} have incorrect type. $dataValue")
      }
      json
    }
}
