package cognite.spark.v1.wdl

import io.circe.{Json, Printer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class RowConversionTest extends FlatSpec with Matchers with ParallelTestExecution {
  it should "convert a Row with nested StructTypes into a JsonObject" in {
    val sourceSchema = new StructType()
      .add("assetExternalId", StringType)
      .add("sourceName", StringType)

    val schema = new StructType()
      .add("name", StringType)
      .add("source", sourceSchema)

    val input: Row = new GenericRowWithSchema(
      Array(
        "MyName",
        new GenericRowWithSchema(Array("MyAssetExternalId", "MySourceName"), sourceSchema),
      ),
      schema
    )

    val json = Json.fromJsonObject(RowConversion.toJsonObject(input, schema))
    val actual = json.printWith(Printer.spaces2.withSortedKeys)
    val expected =
      """{
        |  "name" : "MyName",
        |  "source" : {
        |    "assetExternalId" : "MyAssetExternalId",
        |    "sourceName" : "MySourceName"
        |  }
        |}""".stripMargin
    assert(actual == expected)
  }

  it should "convert a Row with a struct type string value into JsonObject" in {
    val rowSchema = new StructType()
      .add("source", StringType)

    val input = new GenericRowWithSchema(
      Array("""{
        |  "assetExternalId" : "MyAssetExternalId",
        |  "sourceName": "MySourceName"
        |}""".stripMargin),
      rowSchema,
    )

    val schema = new StructType()
      .add(
        "source",
        new StructType()
          .add("assetExternalId", StringType)
          .add("sourceName", StringType))
    val json = Json.fromJsonObject(RowConversion.toJsonObject(input, schema))
    val actual = json.printWith(Printer.spaces2.withSortedKeys)
    val expected =
      """{
        |  "source" : {
        |    "assetExternalId" : "MyAssetExternalId",
        |    "sourceName" : "MySourceName"
        |  }
        |}""".stripMargin
    assert(actual == expected)
  }

  it should "convert a Row with number types into a JsonObject" in {
    val schema = new StructType()
      .add("double", DoubleType)
      .add("float", FloatType)
      .add("big decimal", DecimalType(10, 10))
      .add("int", IntegerType)
      .add("long", LongType)

    val input: Row = new GenericRowWithSchema(
      Array(
        10.0,
        11.0f,
        new java.math.BigDecimal(12.0),
        13,
        14L
      ),
      schema
    )

    val json = Json.fromJsonObject(RowConversion.toJsonObject(input, schema))
    val actual = json.printWith(Printer.spaces2.withSortedKeys)
    val expected =
      """{
        |  "big decimal" : 12.0,
        |  "double" : 10.0,
        |  "float" : 11.0,
        |  "int" : 13,
        |  "long" : 14
        |}""".stripMargin
    assert(actual == expected)
  }

}
