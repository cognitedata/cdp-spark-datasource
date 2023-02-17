package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import io.circe.Printer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class RowToJsonTest extends FlatSpec with Matchers with ParallelTestExecution {
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
        BigDecimal(12.2),
        13,
        14L
      ),
      schema
    )

    val json = RowToJson.toJson(input, schema)
    val actual = json.printWith(Printer.spaces2.withSortedKeys)
    val expected =
      """{
        |  "big decimal" : 12.2,
        |  "double" : 10.0,
        |  "float" : 11.0,
        |  "int" : 13,
        |  "long" : 14
        |}""".stripMargin
    assert(actual == expected)
  }

  it should "convert a Row with nullable types into a JsonObject" in {
    val schema = new StructType()
      .add("double", DoubleType)
      .add("float", FloatType)
      .add("big decimal", DecimalType(10, 10))
      .add("int", IntegerType)
      .add("long", LongType)
      .add("string", StringType)

    val input: Row = new GenericRowWithSchema(
      Array(
        null,
        null,
        null,
        null,
        null,
        null,
      ),
      schema
    )

    val json = RowToJson.toJson(input, schema)
    val actual = json.printWith(Printer.spaces2.withSortedKeys)
    val expected =
      """{
        |  "big decimal" : null,
        |  "double" : null,
        |  "float" : null,
        |  "int" : null,
        |  "long" : null,
        |  "string" : null
        |}""".stripMargin
    assert(actual == expected)
  }

  it should "fail to convert a Row with non-nullable types into a JsonObject" in {
    val schema = new StructType()
      .add("double", DoubleType, nullable = false)
      .add("float", FloatType, nullable = false)
      .add("big decimal", DecimalType(10, 10), nullable = false)
      .add("int", IntegerType, nullable = false)
      .add("long", LongType, nullable = false)
      .add("string", StringType, nullable = false)

    val input: Row = new GenericRowWithSchema(
      Array(
        null,
        null,
        null,
        null,
        null,
        null,
      ),
      schema
    )

    val expectedException = intercept[CdfSparkException] {
      RowToJson.toJsonObject(input, schema)
    }

    assert(expectedException.getMessage.startsWith("Element "))
    assert(expectedException.getMessage.endsWith(" should not be NULL."))
  }

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

    val json = RowToJson.toJson(input, schema)
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

  it should "convert a Row with array of StructTypes into a JsonObject" in {
    val sourceSchema = new StructType()
      .add("assetExternalId", StringType)
      .add("sourceName", StringType)

    val schema = new StructType()
      .add("name", StringType)
      .add("sources", ArrayType(sourceSchema))

    val input: Row = new GenericRowWithSchema(
      Array(
        "MyName",
        Array(new GenericRowWithSchema(Array("MyAssetExternalId", "MySourceName"), sourceSchema)),
      ),
      schema
    )

    val json = RowToJson.toJson(input, schema)
    val actual = json.printWith(Printer.spaces2.withSortedKeys)
    val expected =
      """{
        |  "name" : "MyName",
        |  "sources" : [
        |    {
        |      "assetExternalId" : "MyAssetExternalId",
        |      "sourceName" : "MySourceName"
        |    }
        |  ]
        |}""".stripMargin
    assert(actual == expected)
  }
}
