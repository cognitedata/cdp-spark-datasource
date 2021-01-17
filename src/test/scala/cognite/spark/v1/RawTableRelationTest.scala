package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{RawRow, RawTable}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, LoneElement, Matchers, ParallelTestExecution}

class RawTableRelationTest
  extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with LoneElement {
  import RawTableRelation._
  import spark.implicits._

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

  private val dfWithoutlastUpdatedTimeSchema = StructType(
    Seq(StructField("notlastUpdatedTime", LongType, false), StructField("value", IntegerType, false)))
  private val dfWithoutlastUpdatedTimeData = Seq(
    ("key1", 121, """{ "notlastUpdatedTime": 1, "value": 1 }"""),
    ("key2", 122, """{ "notlastUpdatedTime": 2, "value": 2 }""")
  )
  private val dfWithlastUpdatedTimeSchema = StructType(
    Seq(StructField("lastUpdatedTime", LongType, false), StructField("value", IntegerType, false)))
  private val dfWithlastUpdatedTimeData = Seq(
    ("key3", 123, """{ "lastUpdatedTime": 1, "value": 1 }"""),
    ("key4", 124, """{ "lastUpdatedTime": 2, "value": 2 }""")
  )
  private val dfWithManylastUpdatedTimeSchema = StructType(
    Seq(
      StructField("lastUpdatedTime", LongType, true),
      StructField("__lastUpdatedTime", LongType, false),
      StructField("___lastUpdatedTime", LongType, false),
      StructField("value", IntegerType, false)
    ))
  private val dfWithManylastUpdatedTimeData = Seq(
    ("key5", 125, """{ "___lastUpdatedTime": 111, "__lastUpdatedTime": 11, "value": 1 }"""),
    ("key6", 126, """{ "___lastUpdatedTime": 222, "value": 2, "__lastUpdatedTime": 22, "lastUpdatedTime": 2 }""")
  )

  it should "smoke test raw" taggedAs WriteTest in {
    val limit = 100
    val partitions = 10
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("limitPerPartition", limit)
      .option("partitions", partitions)
      .option("database", "testdb")
      .option("table", "cryptoAssets")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()
    println("loadfirst")
    df.createTempView("raw")
    println("loaded")
    val res = spark.sqlContext
      .sql("select * from raw")
    assert(res.count == limit * partitions)
  }

  "A RawTableRelation" should "allow data columns named key, _key etc. but rename them to _key, __key etc." in {
    val dfWithoutKey = dfWithoutKeyData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithoutKey =
      flattenAndRenameColumns(spark.sqlContext, dfWithoutKey, dfWithoutKeySchema)
    processedWithoutKey.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "notKey", "value"))
    collectToSet[String](processedWithoutKey.select($"key")) should equal(Set("key1", "key2"))

    val dfWithKey = dfWithKeyData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    processedWithKey.schema.fieldNames.toSet should equal(Set("key", "lastUpdatedTime", "_key", "value"))
    collectToSet[String](processedWithKey.select($"key")) should equal(Set("key3", "key4"))
    collectToSet[String](processedWithKey.select($"_key")) should equal(Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithManyKeys =
      flattenAndRenameColumns(spark.sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    processedWithManyKeys.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "____key", "___key", "_key", "value"))

    collectToSet[String](processedWithManyKeys.select($"key")) should equal(Set("key5", "key6"))
    collectToSet[String](processedWithManyKeys.select($"_key")) should equal(Set(null, "k2"))
    collectToSet[String](processedWithManyKeys.select($"___key")) should equal(Set("__k1", "__k2"))
    collectToSet[String](processedWithManyKeys.select($"____key")) should equal(Set("___k1", "___k2"))
  }

  it should "allow data columns named lastUpdatedTime, _lastUpdatedTime etc. but rename them to _lastUpdatedTime, __lastUpdatedTime etc." in {
    val dfWithoutlastUpdatedTime = dfWithoutlastUpdatedTimeData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithoutlastUpdatedTime =
      flattenAndRenameColumns(spark.sqlContext, dfWithoutlastUpdatedTime, dfWithoutlastUpdatedTimeSchema)
    processedWithoutlastUpdatedTime.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "notlastUpdatedTime", "value"))
    collectToSet[Long](processedWithoutlastUpdatedTime.select($"lastUpdatedTime")) should equal(Set(121, 122))

    val dfWithlastUpdatedTime = dfWithlastUpdatedTimeData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithlastUpdatedTime =
      flattenAndRenameColumns(spark.sqlContext, dfWithlastUpdatedTime, dfWithlastUpdatedTimeSchema)
    processedWithlastUpdatedTime.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "_lastUpdatedTime", "value"))
    collectToSet[Long](processedWithlastUpdatedTime.select($"lastUpdatedTime")) should equal(Set(123, 124))
    collectToSet[Long](processedWithlastUpdatedTime.select($"_lastUpdatedTime")) should equal(Set(1, 2))

    val dfWithManylastUpdatedTime = dfWithManylastUpdatedTimeData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithManylastUpdatedTime =
      flattenAndRenameColumns(spark.sqlContext, dfWithManylastUpdatedTime, dfWithManylastUpdatedTimeSchema)
    processedWithManylastUpdatedTime.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "____lastUpdatedTime", "___lastUpdatedTime", "_lastUpdatedTime", "value"))

    collectToSet[Long](processedWithManylastUpdatedTime.select($"lastUpdatedTime")) should equal(Set(125, 126))
    collectToSet[Long](processedWithManylastUpdatedTime.select($"_lastUpdatedTime")) should equal(Set(null, 2))
    collectToSet[Long](processedWithManylastUpdatedTime.select($"___lastUpdatedTime")) should equal(Set(11, 22))
    collectToSet[Long](processedWithManylastUpdatedTime.select($"____lastUpdatedTime")) should equal(
      Set(111, 222))
  }

  it should "insert data with columns named _key, __key etc. as data columns key, _key, etc." in {
    val dfWithKey = dfWithKeyData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames1, unRenamed1) = prepareForInsert(processedWithKey)
    columnNames1.toSet should equal(Set("key", "value"))
    collectToSet[String](unRenamed1.select("key")) should equal(Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithManyKeys =
      flattenAndRenameColumns(spark.sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    val (columnNames2, unRenamed2) = prepareForInsert(processedWithManyKeys)
    columnNames2.toSet should equal(Set("key", "__key", "___key", "value"))
    collectToSet[String](unRenamed2.select("key")) should equal(Set(null, "k2"))
    collectToSet[String](unRenamed2.select("__key")) should equal(Set("__k1", "__k2"))
    collectToSet[String](unRenamed2.select("___key")) should equal(Set("___k1", "___k2"))
  }

  it should "insert data with columns named _lastUpdatedTime, __lastUpdatedTime etc. as data columns lastUpdatedTime, _lastUpdatedTime, etc." in {
    val dfWithlastUpdatedTime = dfWithlastUpdatedTimeData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithlastUpdatedTime =
      flattenAndRenameColumns(spark.sqlContext, dfWithlastUpdatedTime, dfWithlastUpdatedTimeSchema)
    val (columnNames1, unRenamed1) = prepareForInsert(processedWithlastUpdatedTime)
    columnNames1.toSet should equal(Set("lastUpdatedTime", "value"))
    collectToSet[Long](unRenamed1.select("lastUpdatedTime")) should equal(Set(1, 2))

    val dfWithManylastUpdatedTime = dfWithManylastUpdatedTimeData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithManylastUpdatedTime =
      flattenAndRenameColumns(spark.sqlContext, dfWithManylastUpdatedTime, dfWithManylastUpdatedTimeSchema)
    val (columnNames2, unRenamed2) = prepareForInsert(processedWithManylastUpdatedTime)
    columnNames2.toSet should equal(Set("lastUpdatedTime", "__lastUpdatedTime", "___lastUpdatedTime", "value"))
    collectToSet[Long](unRenamed2.select($"lastUpdatedTime")) should equal(Set(null, 2))
    collectToSet[Long](unRenamed2.select($"__lastUpdatedTime")) should equal(Set(11, 22))
    collectToSet[Long](unRenamed2.select($"___lastUpdatedTime")) should equal(Set(111, 222))
  }

  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.registerModule(cognite.spark.jackson.SparkModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }

  "rowsToRawItems" should "return RawRows from Rows" in {
    val dfWithKey = dfWithKeyData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames, unRenamed) = prepareForInsert(processedWithKey)
    val rawItems: Seq[RawRow] = rowsToRawItems(columnNames, unRenamed.collect.toSeq, mapper)
    rawItems.map(_.key.toString).toSet should equal(Set("key3", "key4"))

    val expectedResult: Seq[Map[String, Json]] = Seq[Map[String, Json]](
      Map(("key" -> Json.fromString("k1")), ("value" -> Json.fromInt(1))),
      Map(("key" -> Json.fromString("k2")), ("value" -> Json.fromInt(2)))
    )

    rawItems.map(_.columns) should equal(expectedResult)
  }

  it should "throw an CDFSparkIllegalArgumentException when DataFrame has null key" in {
    val dfWithKey = dfWithNullKeyData.toDF("key", "lastUpdatedTime", "columns")
    val processedWithKey = flattenAndRenameColumns(spark.sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames, unRenamed) = prepareForInsert(processedWithKey)
    an[CdfSparkIllegalArgumentException] should be thrownBy rowsToRawItems(
      columnNames,
      unRenamed.collect.toSeq,
      mapper)
  }

  "Infer Schema" should "use a different limit for infer schema" in {
    val metricsPrefix = "infer_schema_1"
    val database = "testdb"
    val table = "future-event"
    val inferSchemaLimit = 1
    val partitions = 10
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", database)
      .option("table", table)
      .option("inferSchema", true)
      .option("partitions", partitions)
      .option("metricsPrefix", metricsPrefix)
      .option("inferSchemaLimit", inferSchemaLimit)
      .option("collectSchemaInferenceMetrics", true)
      .option("limitPerPartition", 200)
      .load()

    // Trigger schema evaluation
    val schema = df.schema

    val numRowsReadDuringSchemaInferance =
      getNumberOfRowsRead(metricsPrefix, s"raw.$database.$table.rows")
    numRowsReadDuringSchemaInferance should be(inferSchemaLimit)
  }

  "lastUpdatedTime" should "insert data without error" taggedAs (WriteTest) in {
    val destinationDf = spark.read
      .format("cognite.spark.v1")
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
          |null as lastUpdatedTime
      """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationTable")
  }

  it should "test that lastUpdatedTime filters are handled correctly" taggedAs (ReadTest) in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchema", true)
      .option("partitions", "5")
      .load()
      .where(s"lastUpdatedTime >= timestamp('2019-06-21 11:48:00.000Z') and lastUpdatedTime <= timestamp('2019-06-21 11:50:00.000Z')")
    assert(df.count() == 10)
  }

  it should "check partition sizes" taggedAs (ReadTest) in {
    val metricsPrefix = "partitionSizeTest"
    val resourceType = "raw.testdb.future-event"
    val partitions = 10
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("metricsPrefix",  metricsPrefix)
      .option("partitions", partitions)
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchema", "true")
      .option("collectSchemaInferenceMetrics", false)
      .option("collectTestMetrics", true)
      .option("parallelismPerPartition", 1)
      .load()

    df.createTempView("futureEvents")
    val res = spark.sqlContext
      .sql("select * from futureEvents").count()

//    for(a <- MetricsSource.metricsMap.keys)
//      println(a)
    for (partitionIndex <- 0 until partitions){
      println( getPartitionSize( "partitionSizeTest", resourceType, partitionIndex))
    }
    // todo implement equality check between partitions, should it be exact equality?
  }

  it should "handle various numbers of partitions" taggedAs (ReadTest) in {
    for(partitions <- Seq("1", "5", "10", "20")) {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchema", true)
      .option("partitions", partitions)
      .load()
      .where(s"lastUpdatedTime >= timestamp('2019-06-21 11:48:00.000Z') and lastUpdatedTime <= timestamp('2019-06-21 11:50:00.000Z')")
    assert(df.count() == 10)
    }
  }

  it should "write nested struct values" in {
    val database = "testdb"
    val table = "struct-test"

    try {
      writeClient.rawTables(database).createOne(RawTable(table))
    } catch {
      case e: CdpApiException if e.code == 400 => // Ignore if already exists
    }

    val key = shortRandomString()
    val tempView = "struct_test_" + shortRandomString()

    val source = spark.sql(
      s"""select
         |  '$key' as key,
         |  struct(
         |    123                        as long,
         |    'foo'                      as string,
         |    struct(123 as foo)         as struct,
         |    array(struct(123 as foo))  as array_of_struct
         |  ) as value
         |""".stripMargin)
    val destination = spark.read
      .format("cognite.spark.v1")
      .schema(source.schema)
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", database)
      .option("table", table)
      .load()
    destination.createTempView(tempView)
    source
      .select(destination.columns.map(c => col(c)): _*)
      .write
      .insertInto(tempView)

    try {
      val df = spark.read
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "raw")
        .option("database", database)
        .option("table", table)
        .option("inferSchema", true)
        .load()
        .where(s"key = '$key'")

      assert(df.count() == 1)
      val row = df.first()

      val struct = row.getStruct(row.fieldIndex("value"))

      assert(struct.getAs[Long]("long") == 123L)
      assert(struct.getAs[String]("string") == "foo")

      val nestedStruct = struct.getStruct(struct.fieldIndex("struct"))
      assert(nestedStruct.schema != null)
      nestedStruct.schema.fieldNames.toSeq.loneElement shouldBe "foo"
      nestedStruct.toSeq.loneElement shouldBe 123L

      val arrayOfStruct = struct.getSeq[Row](struct.fieldIndex("array_of_struct"))
      val structInArray = arrayOfStruct.loneElement

      assert(structInArray.schema != null)
      structInArray.schema.fieldNames.toSeq.loneElement shouldBe "foo"
      structInArray.toSeq.loneElement shouldBe 123L
    } finally {
      writeClient.rawRows(database, table).deleteById(key)
    }
  }
}
