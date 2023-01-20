package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfPartition, CdfSparkException, RelationConfig}
import io.circe.{Json, JsonObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

private object Implicits {
  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  implicit class RequiredOption[T](optionValue: Option[T]) {
    def orThrow(structFieldName: String, nullable: Boolean): T =
      optionValue match {
        case None | Some(null) => // scalastyle:ignore null
          if (nullable) {
            null.asInstanceOf[T]
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
    val config: RelationConfig
) extends RDD[Row](sparkContext, Nil) {

  import Implicits.RequiredOption

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val client = WdlClient.fromConfig(config)
    val response = client.getItems(model)

    val rows = response.items.map(jsonObject =>
      convertToRow(jsonObject, schema) match {
        case Some(row) => row
        case None => throw new CdfSparkException("Got null as top level row.")
    })

    rows.iterator
  }

  // scalastyle:off cyclomatic.complexity
  private def convertToValue(
      jsonValue: Json,
      dataType: DataType,
      structFieldName: String,
      nullable: Boolean): Any =
    if (jsonValue == null || jsonValue.isNull) {
      if (nullable) {
        null
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
          convertToRow(jsonValue.asObject.orThrow(structFieldName, nullable), structType)
        case StringType => jsonValue.asString
        case DoubleType => jsonValue.asNumber.map(_.toDouble)
        case IntegerType => jsonValue.asNumber.flatMap(_.toInt)
        case LongType => jsonValue.asNumber.flatMap(_.toLong)
        case _ =>
          throw new CdfSparkException(
            s"Conversion to type ${dataType.typeName} is not supported for WDL, element $structFieldName.")
      }

      maybeResult.orThrow(structFieldName, nullable)
    }

  // scalastyle:on cyclomatic.complexity

  private def convertToRow(jsonObject: JsonObject, dataType: StructType): Option[Row] =
    if (jsonObject == null) {
      None // scalastyle:ignore return
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
      )

      val row = Row.fromSeq(cols)
      Some(row)
    }

  override protected def getPartitions: Array[Partition] = Array(
    CdfPartition(0)
  )
}
