package cognite.spark

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class RawTableRelationTest extends FlatSpec with Matchers with SparkTest {
  import RawTableRelation._
  import spark.implicits._

  private val readApiKey = System.getenv("TEST_API_KEY_READ")
  private val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  private def collectToSet[A](df: DataFrame): Set[A] =
    df.collect().map(_.getAs[A](0)).toSet

  private val dfWithoutKeySchema = StructType(
    Seq(StructField("notKey", StringType, false), StructField("value", IntegerType, false)))
  private val dfWithoutKeyData = Seq(
    ("key1", 123, """{ "notKey": "k1", "value": 1 }"""),
    ("key2", 123, """{ "notKey": "k2", "value": 2 }""")
  )
  private val dfWithKeySchema = StructType(
    Seq(StructField("key", StringType, false), StructField("value", IntegerType, false)))
  private val dfWithKeyData = Seq(
    ("key3", 123, """{ "key": "k1", "value": 1 }"""),
    ("key4", 123, """{ "key": "k2", "value": 2 }""")
  )
  private val dfWithNullKeyData = Seq(
    (null, 123, """{ "key": "k1", "value": 1 }"""),
    ("key4", 123, """{ "key": "k2", "value": 2 }""")
  )
  private val dfWithManyKeysSchema = StructType(
    Seq(
      StructField("key", StringType, true),
      StructField("__key", StringType, false),
      StructField("___key", StringType, false),
      StructField("value", IntegerType, false)
    ))
  private val dfWithManyKeysData = Seq(
    ("key5", 123, """{ "___key": "___k1", "__key": "__k1", "value": 1 }"""),
    ("key6", 123, """{ "___key": "___k2", "value": 2, "__key": "__k2", "key": "k2" }""")
  )

  private val dfWithoutLastChangedSchema = StructType(
    Seq(StructField("notLastChanged", LongType, false), StructField("value", IntegerType, false)))
  private val dfWithoutLastChangedData = Seq(
    ("key1", 121, """{ "notLastChanged": 1, "value": 1 }"""),
    ("key2", 122, """{ "notLastChanged": 2, "value": 2 }""")
  )
  private val dfWithLastChangedSchema = StructType(
    Seq(StructField("lastChanged", LongType, false), StructField("value", IntegerType, false)))
  private val dfWithLastChangedData = Seq(
    ("key3", 123, """{ "lastChanged": 1, "value": 1 }"""),
    ("key4", 124, """{ "lastChanged": 2, "value": 2 }""")
  )
  private val dfWithManyLastChangedSchema = StructType(
    Seq(
      StructField("lastChanged", LongType, true),
      StructField("__lastChanged", LongType, false),
      StructField("___lastChanged", LongType, false),
      StructField("value", IntegerType, false)
    ))
  private val dfWithManyLastChangedData = Seq(
    ("key5", 125, """{ "___lastChanged": 111, "__lastChanged": 11, "value": 1 }"""),
    (
      "key6",
      126,
      """{ "___lastChanged": 222, "value": 2, "__lastChanged": 22, "lastChanged": 2 }""")
  )

  "A RawTableRelation" should "allow data columns named key, _key etc. but rename them to _key, __key etc." in {
    val dfWithoutKey = dfWithoutKeyData.toDF("key", "lastChanged", "columns")
    val processedWithoutKey =
      flattenAndRenameColumns(spark.sqlContext, dfWithoutKey, dfWithoutKeySchema)
    processedWithoutKey.schema.fieldNames.toSet should equal(
      Set("key", "lastChanged", "notKey", "value"))
    collectToSet[String](processedWithoutKey.select($"key")) should equal(Set("key1", "key2"))

    val dfWithKey = dfWithKeyData.toDF("key", "lastChanged", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    processedWithKey.schema.fieldNames.toSet should equal(
      Set("key", "lastChanged", "_key", "value"))
    collectToSet[String](processedWithKey.select($"key")) should equal(Set("key3", "key4"))
    collectToSet[String](processedWithKey.select($"_key")) should equal(Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "lastChanged", "columns")
    val processedWithManyKeys =
      flattenAndRenameColumns(spark.sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    processedWithManyKeys.schema.fieldNames.toSet should equal(
      Set("key", "lastChanged", "____key", "___key", "_key", "value"))

    collectToSet[String](processedWithManyKeys.select($"key")) should equal(Set("key5", "key6"))
    collectToSet[String](processedWithManyKeys.select($"_key")) should equal(Set(null, "k2"))
    collectToSet[String](processedWithManyKeys.select($"___key")) should equal(Set("__k1", "__k2"))
    collectToSet[String](processedWithManyKeys.select($"____key")) should equal(
      Set("___k1", "___k2"))
  }

  it should "allow data columns named lastChanged, _lastChanged etc. but rename them to _lastChanged, __lastChanged etc." in {
    val dfWithoutLastChanged = dfWithoutLastChangedData.toDF("key", "lastChanged", "columns")
    val processedWithoutLastChanged =
      flattenAndRenameColumns(spark.sqlContext, dfWithoutLastChanged, dfWithoutLastChangedSchema)
    processedWithoutLastChanged.schema.fieldNames.toSet should equal(
      Set("key", "lastChanged", "notLastChanged", "value"))
    collectToSet[Long](processedWithoutLastChanged.select($"lastChanged")) should equal(
      Set(121, 122))

    val dfWithLastChanged = dfWithLastChangedData.toDF("key", "lastChanged", "columns")
    val processedWithLastChanged =
      flattenAndRenameColumns(spark.sqlContext, dfWithLastChanged, dfWithLastChangedSchema)
    processedWithLastChanged.schema.fieldNames.toSet should equal(
      Set("key", "lastChanged", "_lastChanged", "value"))
    collectToSet[Long](processedWithLastChanged.select($"lastChanged")) should equal(Set(123, 124))
    collectToSet[Long](processedWithLastChanged.select($"_lastChanged")) should equal(Set(1, 2))

    val dfWithManyLastChanged = dfWithManyLastChangedData.toDF("key", "lastChanged", "columns")
    val processedWithManyLastChanged =
      flattenAndRenameColumns(spark.sqlContext, dfWithManyLastChanged, dfWithManyLastChangedSchema)
    processedWithManyLastChanged.schema.fieldNames.toSet should equal(
      Set("key", "lastChanged", "____lastChanged", "___lastChanged", "_lastChanged", "value"))

    collectToSet[Long](processedWithManyLastChanged.select($"lastChanged")) should equal(
      Set(125, 126))
    collectToSet[Long](processedWithManyLastChanged.select($"_lastChanged")) should equal(
      Set(null, 2))
    collectToSet[Long](processedWithManyLastChanged.select($"___lastChanged")) should equal(
      Set(11, 22))
    collectToSet[Long](processedWithManyLastChanged.select($"____lastChanged")) should equal(
      Set(111, 222))
  }

  it should "insert data with columns named _key, __key etc. as data columns key, _key, etc." in {
    val dfWithKey = dfWithKeyData.toDF("key", "lastChanged", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames1, unRenamed1) = prepareForInsert(processedWithKey)
    columnNames1.toSet should equal(Set("key", "value"))
    collectToSet[String](unRenamed1.select("key")) should equal(Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "lastChanged", "columns")
    val processedWithManyKeys =
      flattenAndRenameColumns(spark.sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    val (columnNames2, unRenamed2) = prepareForInsert(processedWithManyKeys)
    columnNames2.toSet should equal(Set("key", "__key", "___key", "value"))
    collectToSet[String](unRenamed2.select("key")) should equal(Set(null, "k2"))
    collectToSet[String](unRenamed2.select("__key")) should equal(Set("__k1", "__k2"))
    collectToSet[String](unRenamed2.select("___key")) should equal(Set("___k1", "___k2"))
  }

  it should "insert data with columns named _lastChanged, __lastChanged etc. as data columns lastChanged, _lastChanged, etc." in {
    val dfWithLastChanged = dfWithLastChangedData.toDF("key", "lastChanged", "columns")
    val processedWithLastChanged =
      flattenAndRenameColumns(spark.sqlContext, dfWithLastChanged, dfWithLastChangedSchema)
    val (columnNames1, unRenamed1) = prepareForInsert(processedWithLastChanged)
    columnNames1.toSet should equal(Set("lastChanged", "value"))
    collectToSet[Long](unRenamed1.select("lastChanged")) should equal(Set(1, 2))

    val dfWithManyLastChanged = dfWithManyLastChangedData.toDF("key", "lastChanged", "columns")
    val processedWithManyLastChanged =
      flattenAndRenameColumns(spark.sqlContext, dfWithManyLastChanged, dfWithManyLastChangedSchema)
    val (columnNames2, unRenamed2) = prepareForInsert(processedWithManyLastChanged)
    columnNames2.toSet should equal(Set("lastChanged", "__lastChanged", "___lastChanged", "value"))
    collectToSet[Long](unRenamed2.select($"lastChanged")) should equal(Set(null, 2))
    collectToSet[Long](unRenamed2.select($"__lastChanged")) should equal(Set(11, 22))
    collectToSet[Long](unRenamed2.select($"___lastChanged")) should equal(Set(111, 222))
  }

  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }

  "rowsToRawItems" should "return RawItems from Rows" in {
    val dfWithKey = dfWithKeyData.toDF("key", "lastChanged", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
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
    val dfWithKey = dfWithNullKeyData.toDF("key", "lastChanged", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
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
      .format("cognite.spark")
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

  "LastChanged" should "insert data without error" taggedAs (WriteTest) in {
    val destinationDf = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "raw-write-test")
      .option("inferSchema", false)
      .load()
    destinationDf.createTempView("destinationTable")

    spark
      .sql(s"""
          |select "key1" as key,
          |"foo" as columns,
          |null as lastChanged
      """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationTable")
  }

  it should "test that lastChanged filters are handled correctly" taggedAs (ReadTest) in {
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchema", true)
      .load()
      .where(s"lastChanged > 1561117603000 and lastChanged < 1561117753000")
    assert(df.count() == 10)
  }
}
