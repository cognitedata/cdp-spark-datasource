package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object JsonObjectToRow {
  implicit class RequiredOption[T](optionValue: Option[T]) {
    def orThrow(structFieldName: String, nullable: Boolean): Option[T] =
      optionValue match {
        case Some(Some(_)) =>
          throw new CdfSparkException(s"Option should not contain option, but it is: $optionValue")
        case None | Some(null) => // scalastyle:ignore null
          if (nullable) {
            None
          } else {
            throw new CdfSparkException(s"Element ${structFieldName} have incorrect type. $optionValue")
          }
        case Some(value) => Some(value)
      }
  }

  def toRow(jsonObject: JsonObject, dataType: StructType): Option[Row] =
    if (jsonObject == null) {
      None
    } else {
      val jsonFields = jsonObject.toMap
      val cols = dataType.toList.map(
        structField =>
          jsonFields
            .get(structField.name)
            .map(jsonField => {
              convertToValue(jsonField, structField.dataType, structField.name, structField.nullable)
            })
            .orThrow(structField.name, structField.nullable)
            .orNull
      )

      val row = Row.fromSeq(cols)
      Some(row)
    }

// scalastyle:off cyclomatic.complexity
  private def convertToValue(
      jsonValue: Json,
      dataType: DataType,
      structFieldName: String,
      nullable: Boolean): Any =
    if (jsonValue == null || jsonValue.isNull) {
      if (nullable) {
        val circumventingScalaStyleThatDoesNotLikeNulls: Option[Any] = None
        circumventingScalaStyleThatDoesNotLikeNulls.orNull
      } else {
        throw new CdfSparkException(s"Element ${structFieldName} should not be NULL.")
      }
    } else {
      val maybeResult: Option[Any] = dataType match {
        case ArrayType(elementType, containsNull) =>
          jsonValue.asArray match {
            case Some(arr) =>
              Some(arr.map(value => convertToValue(value, elementType, structFieldName, containsNull)))
            case None => None
          }
        case structType: StructType =>
          toRow(jsonValue.asObject.orThrow(structFieldName, nullable).orNull, structType)
        case StringType => jsonValue.asString
        case DoubleType => jsonValue.asNumber.map(_.toDouble)
        case IntegerType => jsonValue.asNumber.flatMap(_.toInt)
        case LongType => jsonValue.asNumber.flatMap(_.toLong)
        case BooleanType => jsonValue.asBoolean
        case _ =>
          throw new CdfSparkException(
            s"Conversion to type ${dataType.typeName} is not supported for WDL, element $structFieldName.")
      }

      maybeResult.orThrow(structFieldName, nullable).orNull
    }
// scalastyle:on cyclomatic.complexity{
}
