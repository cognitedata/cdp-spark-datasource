package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfPartition, CdfSparkException, RelationConfig}
import io.circe.{Json, JsonObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

private object implicits { // scalastyle:ignore object.name
  implicit class RequiredOption[T](optionValue: Option[T]) {
    def orThrow(structFieldName: String, nullable: Boolean): T =
      optionValue match {
        case Some(Some(_)) =>
          throw new CdfSparkException(s"Element ${structFieldName} have incorrect type. $optionValue")
        case None | Some(null) => // scalastyle:ignore null
          if (nullable) {
            null.asInstanceOf[T] // scalastyle:ignore null
          } else {
            throw new CdfSparkException(s"Element ${structFieldName} have incorrect type. $optionValue")
          }
        case Some(value) => value
      }
  }
}

class WdlRDD(
    @transient override val sparkContext: SparkContext,
    val schema: StructType,
    val model: String,
    val config: RelationConfig,
) extends RDD[Row](sparkContext, Nil) {

  import implicits._

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val client = new WdlClient(config)
    val response = client.getItems(model)

    val rows = response.items.map(jsonObject => convertToValue(jsonObject, schema).orNull)

    rows.iterator
  }

  // scalastyle:off cyclomatic.complexity
  private def convertToValue(
      jsonValue: Json,
      dataType: DataType,
      structFieldName: String,
      nullable: Boolean): Any = {
    if (jsonValue == null || jsonValue.isNull) {
      if (nullable) {
        return null // scalastyle:ignore
      }
      throw new CdfSparkException(s"Element ${structFieldName} should not be NULL.")
    }

    val result = dataType match {
      case ArrayType(elementType, containsNull) =>
        jsonValue.asArray.map(values =>
          values.map(value => convertToValue(value, elementType, structFieldName, containsNull)))
      case structType: StructType =>
        convertToValue(jsonValue.asObject.orThrow(structFieldName, nullable), structType)
      case StringType => jsonValue.asString
      case DoubleType => jsonValue.asNumber.map(value => value.toDouble)
      case IntegerType => jsonValue.asNumber.flatMap(value => value.toInt)
      case LongType => jsonValue.asNumber.flatMap(value => value.toLong)
      case _ =>
        throw new CdfSparkException(
          s"Conversion to type ${dataType.typeName} is not supported for WDL, element $structFieldName.")
    }

    result.orThrow(structFieldName, nullable)
  }
  // scalastyle:on cyclomatic.complexity

  private def convertToValue(jsonObject: JsonObject, dataType: StructType): Option[Row] = {
    if (jsonObject == null) {
      return None // scalastyle:ignore return
    }
    val jsonFields = jsonObject.toMap
    val cols = dataType.toList.map(
      structField =>
        jsonFields
          .get(structField.name)
          .map(jsonField => {
            val converted =
              convertToValue(jsonField, structField.dataType, structField.name, structField.nullable)
            converted
          })
          .orThrow(structField.name, structField.nullable))
    val row = Row.fromSeq(cols)
    Some(row)
  }

  override protected def getPartitions: Array[Partition] = Array(
    CdfPartition(0),
  )
}
