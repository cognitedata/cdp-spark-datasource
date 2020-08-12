package cognite.spark.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.spark.sql.Row

/** Serializes a [[Row]] to a plain JSON object if it has a schema, or an array otherwise. */
private[spark] class RowSerializer extends StdSerializer[Row](classOf[Row]) {
  def serialize(row: Row, gen: JsonGenerator, provider: SerializerProvider): Unit =
    if (row.schema != null) {
      gen.writeStartObject()
      for (field <- row.schema.fields) {
        val value = row(row.fieldIndex(field.name))
        provider.defaultSerializeField(field.name, value, gen)
      }
      gen.writeEndObject()
    } else {
      gen.writeStartArray()
      for (value <- row.toSeq) {
        provider.defaultSerializeValue(value, gen)
      }
      gen.writeEndArray()
    }
}
