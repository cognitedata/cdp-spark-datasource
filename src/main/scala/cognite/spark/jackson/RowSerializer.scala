package cognite.spark.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.spark.sql.Row

/** Serializes a [[Row]] to a plain JSON object if it has a schema, or an array otherwise. */
private[spark] class RowSerializer extends StdSerializer[Row](classOf[Row]) {
  def serialize(row: Row, gen: JsonGenerator, provider: SerializerProvider): Unit =
    if (row.schema != null) {
      val entries = row.schema.fieldNames.map(key => key -> row(row.fieldIndex(key)))

      gen.writeStartObject()
      for ((key, value) <- entries) {
        provider.defaultSerializeField(key, value, gen)
      }
      gen.writeEndObject()
    } else {
      val values = row.toSeq

      gen.writeStartArray()
      for (value <- values) {
        provider.defaultSerializeValue(value, gen)
      }
      gen.writeEndArray()
    }
}
