package cognite.spark.v1.wdl

import io.circe.parser.decode
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// scalastyle:off
object RowToJsonObject {

  /**
    * Creates a JsonObject from the values of `row` with the schema of `schema`.
    * The row must have a schema, because it matches the `row` with `schema` on the schema keys.
    *
    * @param row A GenericRowWithSchema coming from apache spark.
    * @param schema The <i>output</i> schema of the generated JsonObject.
    *
    * @return a JsonObject
    */
  def toJsonObject(row: Row, schema: StructType): JsonObject = {
    if (row.schema == null) {
      sys.error(
        s"Schema for $row is null. The input row needs a schema, because it must be matched with the schema of the output format.")
    }

    val fields = schema
      .map(field => {
        // i is the index in the Row for the field in the schema.
        val fieldIndex: Option[Int] = try {
          Some(row.fieldIndex(field.name))
        } catch {
          case _: IllegalArgumentException =>
            None
        }

        fieldIndex match {
          case None => null
          case Some(i) =>
            val jsonValue: Json = row.isNullAt(i) match {
              case true =>
                field.nullable match {
                  case true => Json.Null
                  case false =>
                    sys.error(s"Failed to parse non-nullable ${field} from NULL")
                }
              case false =>
                field.dataType match {
                  case _: NullType => Json.Null
                  case s: StructType => parseStructType(row.get(i), field, s)
                  case StringType => Json.fromString(row.getString(i))
                  case DoubleType | DecimalType() | FloatType => parseDouble(row.get(i), field)
                  case IntegerType => Json.fromInt(row.getInt(i))
                  case LongType => Json.fromLong(row.getLong(i))
                  case _ => sys.error(s"Unknown type: ${field.dataType} with name ${field.name}")
                }
            }
            (field.name, jsonValue)
        }
      })
      .filterNot(p => p == null) // removed _continued_ iterations.
    JsonObject.fromMap(fields.toMap)
  }

  private def parseStructType(value: Any, field: StructField, schema: StructType) =
    value match {
      case r: Row =>
        val inner = toJsonObject(r, schema)
        Json.fromJsonObject(inner)
      case s: String =>
        decode[JsonObject](s) match {
          case Left(_) => sys.error(s"Failed to convert string `$s` into a struct field")
          case Right(j) => Json.fromJsonObject(j)
        }
      case err =>
        sys.error(s"Failed to parse $field as a StructField. Value was $err of type ${err.getClass}")
    }

  private def parseDouble(value: Any, field: StructField): Json =
    value match {
      case d: Double => Json.fromDoubleOrNull(d)
      case f: Float => Json.fromDoubleOrNull(f.toDouble)
      case decimal: java.math.BigDecimal => Json.fromDoubleOrNull(decimal.doubleValue())
      case _ =>
        sys.error(s"Failed to parse ${field} as Double. Value was ${value} of type ${value.getClass}")
    }
}
