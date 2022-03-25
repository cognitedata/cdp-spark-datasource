package cognite.spark.v1

import cats.effect.kernel.Concurrent
import com.cognite.sdk.scala.v1.RawRow
import io.circe.Json
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import cats.implicits._

object RawSchemaInferrer {
  // reimplementation of JsonInferSchema.infer from Spark which we used to rely on in the past
  // our version works directly on circe Json and we can invoke it without starting a Spark job

  /** Returns the inferred schema of one Json value */
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
        } else if (isIntegerString(str)) {
          JsonInteger
        } else if (isFloatString(str)) {
          JsonDouble
        } else {
          JsonString
      },
      array => JsonArray(array.map(infer).fold(JsonNull: JsonType)(unify)),
      obj => {
        val result = scala.collection.mutable.HashMap[String, JsonType]()
        inferObject(obj.toIterable, result)
        JsonObject(result.toMap)
      }
    )

  private def inferObject(
      obj: Iterable[(String, Json)],
      result: scala.collection.mutable.HashMap[String, JsonType]): Unit =
    for ((k, v) <- obj) {
      val t = infer(v)
      result.get(k) match {
        case None =>
          result.update(k, t)
        case Some(originalType) =>
          if (!isAssignableTo(t, originalType)) {
            result.update(k, unify(t, originalType))
          }
      }
    }

  /** Returns unified inferred schema of all objects in the Stream */
  def inferRows[F[_]: Concurrent](rows: fs2.Stream[F, RawRow]): F[JsonObject] =
    rows.compile
      .fold(scala.collection.mutable.HashMap[String, JsonType]()) { (result, row) =>
        inferObject(row.columns, result)
        result
      }
      .map(r => JsonObject(r.toMap))

  // scalastyle:off cyclomatic.complexity
  /** Returns true if the `to` type is compatible with `from` */
  def isAssignableTo(from: JsonType, to: JsonType): Boolean =
    (from, to) match {
      case (JsonNull, _) => true
      case (JsonInteger, JsonDouble) => true
      case (_, JsonString) => true
      case (JsonObject(x), JsonObject(y)) =>
        x.forall {
          case (k, v) =>
            y.get(k).exists(isAssignableTo(v, _))
        }
      case (JsonArray(x), JsonArray(y)) => isAssignableTo(x, y)
      case (_, _) if from == to => true
      case _ => false
    }

  /** Returns a "lower common supertype" - a type to which both `a` and `b` are assignable.
    * Note that in this type system, everything is assignable to String */
  def unify(a: JsonType, b: JsonType): JsonType =
    (a, b) match {
      case (x: JsonObject, y: JsonObject) => unifyObjects(x, y)
      case (_, _) if isAssignableTo(b, a) => a
      case (_, _) if isAssignableTo(a, b) => b
      case (JsonArray(x), JsonArray(y)) => JsonArray(unify(x, y))

      // anything can be put into a string - we just put the serialized JSON into it
      case (_, _) => JsonString
    }

  def unifyObjects(a: JsonObject, b: JsonObject): JsonObject =
    if (isAssignableTo(b, a)) {
      a
    } else if (isAssignableTo(a, b)) {
      b
    } else {
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

  /** Skips a sequence of digits. Returns index right after the last consumed digit */
  private def skipInteger(str: String, index: Int): Int = {
    var i = index
    while (i < str.length && str(i) >= '0' && str(i) <= '9') {
      i += 1
    }
    i
  }

  /** Skips + or - sign. Returns index right after this sign */
  private def skipPlusMinus(str: String, index: Int): Int =
    if (str(index) == '-' || str(index) == '+') {
      index + 1
    } else {
      index
    }

  /** Return true if the string can be parsed a 64-bit Int.
    * Essentially implements this regex: [-+]?\d{1,18} */
  private def isIntegerString(str: String): Boolean = {
    val i = skipPlusMinus(str, 0)

    if (i + 18 < str.length) {
      // won't fit into int64
      false
    } else {
      val i2 = skipInteger(str, i)
      i2 > i && i2 == str.length
    }
  }

  /** Returns true if the string can be parsed as a Float.
    * Essentially allows NaN, + or - Infinity and anything matching [-+]?\d*(.\d*)?(e[+-]?\d+)? */
  private def isFloatString(str: String): Boolean = {
    if ("NaN".equals(str)) {
      return true
    }
    var i = skipPlusMinus(str, 0)

    if (str.length == i + "Infinity".length && str.endsWith("Infinity")) {
      return true
    }

    i = skipInteger(str, i)
    if (i < str.length && str(i) == '.') {
      // consume the decimal part `.1234`
      i = skipInteger(str, i + 1)
    }
    if (i < str.length && (str(i) == 'e' || str(i) == 'E')) {
      // consume exponential part ("scientific notation"), for example ...`e-12`
      i = skipPlusMinus(str, i + 1)
      val i2 = skipInteger(str, i)
      if (i2 == i) {
        // force that we have at least one digit after the `e`
        return false
      }
      i = i2
    }

    i == str.length
  }

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
