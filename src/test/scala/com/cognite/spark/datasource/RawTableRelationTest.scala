package com.cognite.spark.datasource

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class RawTableRelationTest extends FlatSpec with Matchers with SparkTest {
  import RawTableRelation._
  import spark.implicits._

  private val readApiKey = System.getenv("TEST_API_KEY_READ")
  private val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  private def collectToSet(df: DataFrame): Set[String] =
    df.collect().map(_.getString(0)).toSet

  private val dfWithoutKeySchema = StructType(
    Seq(StructField("notKey", StringType, false), StructField("value", IntegerType, false)))
  private val dfWithoutKeyData = Seq(
    ("key1", """{ "notKey": "k1", "value": 1 }"""),
    ("key2", """{ "notKey": "k2", "value": 2 }""")
  )
  private val dfWithKeySchema = StructType(
    Seq(StructField("key", StringType, false), StructField("value", IntegerType, false)))
  private val dfWithKeyData = Seq(
    ("key3", """{ "key": "k1", "value": 1 }"""),
    ("key4", """{ "key": "k2", "value": 2 }""")
  )
  private val dfWithNullKeyData = Seq(
    (null, """{ "key": "k1", "value": 1 }"""),
    ("key4", """{ "key": "k2", "value": 2 }""")
  )
  private val dfWithManyKeysSchema = StructType(
    Seq(
      StructField("key", StringType, true),
      StructField("__key", StringType, false),
      StructField("___key", StringType, false),
      StructField("value", IntegerType, false)
    ))
  private val dfWithManyKeysData = Seq(
    ("key5", """{ "___key": "___k1", "__key": "__k1", "value": 1 }"""),
    ("key6", """{ "___key": "___k2", "value": 2, "__key": "__k2", "key": "k2" }""")
  )

  "A RawTableRelation" should "allow data columns named key, _key etc. but rename them to _key, __key etc." in {
    val dfWithoutKey = dfWithoutKeyData.toDF("key", "columns")
    val processedWithoutKey =
      flattenAndRenameKeyColumns(spark.sqlContext, dfWithoutKey, dfWithoutKeySchema)
    processedWithoutKey.schema.fieldNames.toSet should equal(Set("key", "notKey", "value"))
    collectToSet(processedWithoutKey.select($"key")) should equal(Set("key1", "key2"))

    val dfWithKey = dfWithKeyData.toDF("key", "columns")
    val processedWithKey = flattenAndRenameKeyColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    processedWithKey.schema.fieldNames.toSet should equal(Set("key", "_key", "value"))
    collectToSet(processedWithKey.select($"key")) should equal(Set("key3", "key4"))
    collectToSet(processedWithKey.select($"_key")) should equal(Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "columns")
    val processedWithManyKeys =
      flattenAndRenameKeyColumns(spark.sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    processedWithManyKeys.schema.fieldNames.toSet should equal(
      Set("key", "____key", "___key", "_key", "value"))

    collectToSet(processedWithManyKeys.select($"key")) should equal(Set("key5", "key6"))
    collectToSet(processedWithManyKeys.select($"_key")) should equal(Set(null, "k2"))
    collectToSet(processedWithManyKeys.select($"___key")) should equal(Set("__k1", "__k2"))
    collectToSet(processedWithManyKeys.select($"____key")) should equal(Set("___k1", "___k2"))
  }

  it should "insert data with columns named _key, __key etc. as data columns key, _key, etc." in {
    val dfWithKey = dfWithKeyData.toDF("key", "columns")
    val processedWithKey = flattenAndRenameKeyColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames1, unRenamed1) = prepareForInsert(processedWithKey)
    columnNames1.toSet should equal(Set("key", "value"))
    collectToSet(unRenamed1.select("key")) should equal(Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "columns")
    val processedWithManyKeys =
      flattenAndRenameKeyColumns(spark.sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    val (columnNames2, unRenamed2) = prepareForInsert(processedWithManyKeys)
    columnNames2.toSet should equal(Set("key", "__key", "___key", "value"))
    collectToSet(unRenamed2.select("key")) should equal(Set(null, "k2"))
    collectToSet(unRenamed2.select("__key")) should equal(Set("__k1", "__k2"))
    collectToSet(unRenamed2.select("___key")) should equal(Set("___k1", "___k2"))
  }

  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }

  "rowsToRawItems" should "return RawItems from Rows" in {
    val dfWithKey = dfWithKeyData.toDF("key", "columns")
    val processedWithKey = flattenAndRenameKeyColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames, unRenamed) = prepareForInsert(processedWithKey)
    val rawItems = rowsToRawItems(columnNames, unRenamed.collect.toSeq, mapper)
    rawItems.map(_.key.toString).toSet should equal(Set("key3", "key4"))

    val expectedResult: Seq[JsonObject] = Seq[JsonObject](
      JsonObject(("key", Json.fromString("k1")), ("value", Json.fromInt(1))),
      JsonObject(("key", Json.fromString("k2")), ("value", Json.fromInt(2)))
    )

    rawItems.map(_.columns) should equal(expectedResult)
  }

  it should "throw an IllegalArgumentException when DataFrame has null key" in {
    val dfWithKey = dfWithNullKeyData.toDF("key", "columns")
    val processedWithKey = flattenAndRenameKeyColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames, unRenamed) = prepareForInsert(processedWithKey)
    an[IllegalArgumentException] should be thrownBy rowsToRawItems(
      columnNames,
      unRenamed.collect.toSeq,
      mapper)
  }

  "Infer Schema" should "use a different limit for infer schema" in {
    val metricsPrefix = "infer_schema_1"
    val database = "testdb"
    val table = "future-event"
    val inferSchemaLimit = 1
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", database)
      .option("table", table)
      .option("inferSchema", true)
      .option("metricsPrefix", metricsPrefix)
      .option("inferSchemaLimit", inferSchemaLimit)
      .option("collectSchemaInferenceMetrics", true)
      .option("limit", 200)
      .load()

    // Trigger schema evaluation
    val schema = df.schema

    val numRowsReadDuringSchemaInferance =
      getNumberOfRowsRead(metricsPrefix, s"raw.$database.$table.rows")
    numRowsReadDuringSchemaInferance should be(inferSchemaLimit)
  }
}
