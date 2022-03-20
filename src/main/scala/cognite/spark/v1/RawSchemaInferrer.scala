package cognite.spark.v1

import io.circe.Json
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object RawSchemaInferrer {
  // reimplementation of JsonInferSchema.infer from Spark which we used to rely on in the past
  // our version works directly on circe Json and we can invoke it without starting a Spark job

  def infer(j: Json): JsonType =
    j.fold(
      JsonNull,
      _ => JsonBool,
      num =>
        if (num.toLong.isDefined) {
          JsonInteger
        } else {
          // TODO: do we need to handle big integers?
          JsonDouble
      },
      str =>
        if (str.isEmpty) {
          JsonNull
        } else if (str.matches("^[-+]?\\d{1,18}$")) {
          JsonInteger
        } else if (str
            .matches("^[-+]?\\d+(.\\d+)?(e[+-]?\\d+)?$") || str == "+Infinity" || str == "Infinity" || str == "-Infinity" || str == "NaN") {
          JsonDouble
        } else {
          JsonString
      },
      array => JsonArray(array.map(infer).foldLeft(JsonNull: JsonType)(unify)),
      obj => infer(obj.toIterable)
    )

  def infer(obj: Iterable[(String, Json)]): JsonObject =
    JsonObject(
      obj.map {
        case (key, value) => key -> infer(value)
      }.toMap
    )

  def isAssignableTo(from: JsonType, to: JsonType): Boolean =
    (from, to) match {
      case (JsonNull, _) => true
      case (JsonInteger, JsonDouble) => true
      case (_, JsonString) => true
      case (JsonObject(x), JsonObject(y)) =>
        x.keys.forall(k => y.contains(k) && isAssignableTo(x(k), y(k)))
      case (JsonArray(x), JsonArray(y)) => isAssignableTo(x, y)
      case (_, _) if from == to => true
      case _ => false
    }

  def unify(a: JsonType, b: JsonType): JsonType =
    (a, b) match {
      case (_, _) if isAssignableTo(b, a) => a
      case (_, _) if isAssignableTo(a, b) => b
      case (x: JsonObject, y: JsonObject) => unifyObjects(x, y)
      case (JsonArray(x), JsonArray(y)) => JsonArray(unify(x, y))

      // anything can be put into a string - we just put the serialized JSON into it
      case (_, _) => JsonString
    }

  def unifyObjects(a: JsonObject, b: JsonObject): JsonObject = {
    val keys = a.fields.keySet.union(b.fields.keySet)
    val fields = keys.iterator.map(k => k -> unify(a.field(k), b.field(k))).toMap
    JsonObject(fields)
  }

  def toSparkSchema(j: JsonType): DataType = {
    import org.apache.spark.sql.types._
    j match {
      case JsonNull => StringType
      case JsonInteger => LongType
      case JsonDouble => DoubleType
      case JsonString => StringType
      case JsonBool => BooleanType
      case obj: JsonObject => toSparkSchema(obj)
      case JsonArray(element) => ArrayType(toSparkSchema(element))
    }
  }

  def toSparkSchema(j: JsonObject): StructType =
    StructType(j.fields.map {
      case (name, t) => StructField(name, toSparkSchema(t), nullable = true)
    }.toArray)

  sealed trait JsonType

  /** null or empty string */
  object JsonNull extends JsonType

  /** int64 */
  object JsonInteger extends JsonType
  object JsonDouble extends JsonType
  object JsonString extends JsonType
  object JsonBool extends JsonType
  final case class JsonObject(fields: Map[String, JsonType]) extends JsonType {
    def field(name: String): JsonType = fields.getOrElse(name, JsonNull)
  }
  final case class JsonArray(element: JsonType) extends JsonType
}
