package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{RawDatabase, RawRow, RawTable}
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, LoneElement, Matchers, ParallelTestExecution}

class RawTableRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with LoneElement
    with BeforeAndAfterAll {
  import RawTableRelation._
  import spark.implicits._

  import scala.collection.JavaConverters._

  private def collectToSet[A](df: DataFrame): Set[A] =
    df.collectAsList().asScala.map(_.getAs[A](0)).toSet

  private def checkRange(leftLimit: Double, rightLimit: Double, number: Long): Boolean =
    (number >= leftLimit) && (number <= rightLimit)

  private val dfWithoutKeySchema = StructType(
    Seq(StructField("notKey", StringType, false), StructField("value", IntegerType, false)))
  private val dataWithoutKey = Seq(
    RawRow("key1", Map("notKey" -> Json.fromString("k1"), "value" -> Json.fromInt(1))),
    RawRow("key2", Map("notKey" -> Json.fromString("k2"), "value" -> Json.fromInt(2)))
  )
  private val dfWithKeySchema = StructType(
    Seq(StructField("key", StringType, false), StructField("value", IntegerType, false)))
  private val dataWithKey = Seq(
    RawRow("key3", Map("key" -> Json.fromString("k1"), "value" -> Json.fromInt(1))),
    RawRow("key4", Map("key" -> Json.fromString("k2"), "value" -> Json.fromInt(2)))
  )
  private val dfWithManyKeysSchema = StructType(
    Seq(
      StructField("key", StringType, true),
      StructField("__key", StringType, false),
      StructField("___key", StringType, false),
      StructField("value", IntegerType, false)
    ))
  private val dataWithManyKeys = Seq(
    RawRow(
      "key5",
      Map(
        "___key" -> Json.fromString("___k1"),
        "__key" -> Json.fromString("__k1"),
        "value" -> Json.fromInt(1))),
    RawRow(
      "key6",
      Map(
        "___key" -> Json.fromString("___k2"),
        "value" -> Json.fromInt(2),
        "__key" -> Json.fromString("__k2"),
        "key" -> Json.fromString("k2")))
  )

  private val dfWithoutlastUpdatedTimeSchema = StructType(
    Seq(StructField("notlastUpdatedTime", LongType, false), StructField("value", IntegerType, false)))
  private val dataWithoutlastUpdatedTime = Seq(
    RawRow("key1", Map("notlastUpdatedTime" -> 1, "value" -> 1).mapValues(Json.fromInt).toMap),
    RawRow("key2", Map("notlastUpdatedTime" -> 2, "value" -> 2).mapValues(Json.fromInt).toMap)
  )
  private val dfWithlastUpdatedTimeSchema = StructType(
    Seq(StructField("lastUpdatedTime", LongType, false), StructField("value", IntegerType, false)))
  private val dataWithlastUpdatedTime = Seq(
    RawRow("key3", Map("lastUpdatedTime" -> Json.fromInt(1), "value" -> Json.fromInt(1))),
    RawRow("key4", Map("lastUpdatedTime" -> Json.fromInt(2), "value" -> Json.fromInt(2)))
  )
  private val dfWithManylastUpdatedTimeSchema = StructType(
    Seq(
      StructField("lastUpdatedTime", LongType, true),
      StructField("__lastUpdatedTime", LongType, false),
      StructField("___lastUpdatedTime", LongType, false),
      StructField("value", IntegerType, false)
    ))
  private val dataWithManylastUpdatedTime = Seq(
    RawRow("key5", Map("___lastUpdatedTime" -> 111, "__lastUpdatedTime" -> 11, "value" -> 1).mapValues(Json.fromInt).toMap),
    RawRow("key6", Map("___lastUpdatedTime" -> 222, "value" -> 2, "__lastUpdatedTime" -> 22, "lastUpdatedTime" -> 2).mapValues(Json.fromInt).toMap)
  )

  private val dataWithSimpleNestedStruct = Seq(
    RawRow("k", Map("nested" -> Json.obj("field" -> Json.fromString("Ř"), "field2" -> Json.fromInt(1))))
  )

  private val dataWithEmptyStringInByteField = Seq(
    RawRow("k1", Map("byte" -> Json.fromString(""))),
    RawRow("k2", Map("byte" -> Json.fromInt(1.toByte)))
  )
  private val dataWithEmptyStringInShortField = Seq(
    RawRow("k1", Map("short" -> Json.fromString(""))),
    RawRow("k2", Map("short" -> Json.fromInt(12.toShort)))
  )
  private val dataWithEmptyStringInIntegerField = Seq(
    RawRow("k1", Map("integer" -> Json.fromString(""))),
    RawRow("k2", Map("integer" -> Json.fromInt(123)))
  )
  private val dataWithEmptyStringInLongField = Seq(
    RawRow("k1", Map("long" -> Json.fromString(""))),
    RawRow("k2", Map("long" -> Json.fromLong(12345L)))
  )
  private val dataWithEmptyStringInDoubleField = Seq(
    RawRow("k1", Map("num" -> Json.fromString(""))),
    RawRow("k2", Map("num" -> Json.fromDouble(12.3).get))
  )
  private val dataWithEmptyStringInBooleanField = Seq(
    RawRow("k1", Map("bool" -> Json.fromString(""))),
    RawRow("k2", Map("bool" -> Json.fromBoolean(java.lang.Boolean.parseBoolean("true")))),
    RawRow("k3", Map("bool" -> Json.fromBoolean(false)))
  )

  override def beforeAll(): Unit = {
    val db = "spark-test-database"
    val tables = Seq(
      ("without-key", dataWithoutKey),
      ("with-key", dataWithKey),
      ("with-many-keys", dataWithManyKeys),
      ("without-lastUpdatedTime", dataWithoutlastUpdatedTime),
      ("with-lastUpdatedTime", dataWithlastUpdatedTime),
      ("with-many-lastUpdatedTime", dataWithManylastUpdatedTime),
      ("with-nesting", dataWithSimpleNestedStruct),
      ("with-byte-empty-str", dataWithEmptyStringInByteField),
      ("with-short-empty-str", dataWithEmptyStringInShortField),
      ("with-integer-empty-str", dataWithEmptyStringInIntegerField),
      ("with-long-empty-str", dataWithEmptyStringInLongField),
      ("with-number-empty-str", dataWithEmptyStringInDoubleField),
      ("with-boolean-empty-str", dataWithEmptyStringInBooleanField)
    )
    if (!writeClient.rawDatabases.list().compile.toVector.exists(_.name == db)) {
      writeClient.rawDatabases.createOne(RawDatabase(db))
    }
    writeClient.rawTables(db).list().compile.toVector.map(_.name).foreach {
      writeClient.rawTables(db).deleteById(_)
    }
    writeClient.rawTables(db).create(tables.map(t => RawTable(t._1)))

    for ((n, data) <- tables) {
      writeClient.rawRows(db, n).create(data)
    }
  }

  lazy private val dfWithoutKey = rawRead("without-key")
  lazy private val dfWithKey = rawRead("with-key")
  lazy private val dfWithManyKeys = rawRead("with-many-keys")

  lazy private val dfWithoutlastUpdatedTime = rawRead("without-lastUpdatedTime")
  lazy private val dfWithlastUpdatedTime = rawRead("with-lastUpdatedTime")
  lazy private val dfWithManylastUpdatedTime = rawRead("with-many-lastUpdatedTime")

  lazy private val dfWithSimpleNestedStruct = rawRead("with-nesting")

  lazy private val dfWithEmptyStringInByteField = rawRead("with-byte-empty-str")
  lazy private val dfWithEmptyStringInShortField = rawRead("with-short-empty-str")
  lazy private val dfWithEmptyStringInIntegerField = rawRead("with-integer-empty-str")
  lazy private val dfWithEmptyStringInLongField = rawRead("with-long-empty-str")
  lazy private val dfWithEmptyStringInDoubleField = rawRead("with-number-empty-str")
  lazy private val dfWithEmptyStringInBooleanField = rawRead("with-boolean-empty-str")

  it should "smoke test raw" taggedAs WriteTest in {
    val limit = 100
    val partitions = 10
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("limitPerPartition", limit)
      .option("partitions", partitions)
      .option("database", "testdb")
      .option("table", "cryptoAssets")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()
    df.createTempView("raw")
    val res = spark.sqlContext
      .sql("select * from raw")
    assert(res.count == limit * partitions)
  }

  def rawRead(
      table: String,
      database: String = "spark-test-database",
      inferSchema: Boolean = true): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("partitions", 1)
      .option("database", database)
      .option("table", table)
      .option("inferSchema", inferSchema)
      .option("inferSchemaLimit", "100")
      .load()
  //.cache()

  "A RawTableRelation" should "allow data columns named key, _key etc. but rename them to _key, __key etc." in {
    dfWithoutKey.schema.fieldNames.toSet should equal(Set("key", "lastUpdatedTime", "notKey", "value"))
    collectToSet[String](dfWithoutKey.select($"key")) should equal(Set("key1", "key2"))

    dfWithKey.schema.fieldNames.toSet should equal(Set("key", "lastUpdatedTime", "_key", "value"))
    collectToSet[String](dfWithKey.select($"key")) should equal(Set("key3", "key4"))
    collectToSet[String](dfWithKey.select($"_key")) should equal(Set("k1", "k2"))

    dfWithManyKeys.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "____key", "___key", "_key", "value"))

    collectToSet[String](dfWithManyKeys.select($"key")) should equal(Set("key5", "key6"))
    collectToSet[String](dfWithManyKeys.select($"_key")) should equal(Set(null, "k2"))
    collectToSet[String](dfWithManyKeys.select($"___key")) should equal(Set("__k1", "__k2"))
    collectToSet[String](dfWithManyKeys.select($"____key")) should equal(Set("___k1", "___k2"))
  }

  it should "allow data columns named lastUpdatedTime, _lastUpdatedTime etc. but rename them to _lastUpdatedTime, __lastUpdatedTime etc." in {
    dfWithoutlastUpdatedTime.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "notlastUpdatedTime", "value"))
    collectToSet[java.sql.Timestamp](dfWithoutlastUpdatedTime.select($"lastUpdatedTime"))

    dfWithlastUpdatedTime.schema.fieldNames.toSet should equal(
      Set("key", "lastUpdatedTime", "_lastUpdatedTime", "value"))
    collectToSet[java.sql.Timestamp](dfWithlastUpdatedTime.select($"lastUpdatedTime"))
    collectToSet[Long](dfWithlastUpdatedTime.select($"_lastUpdatedTime")) should equal(Set(1, 2))

    dfWithManylastUpdatedTime.schema.fieldNames.toSet should equal(
      Set(
        "key",
        "lastUpdatedTime",
        "____lastUpdatedTime",
        "___lastUpdatedTime",
        "_lastUpdatedTime",
        "value"))

    collectToSet[java.sql.Timestamp](dfWithManylastUpdatedTime.select($"lastUpdatedTime"))
    collectToSet[Long](dfWithManylastUpdatedTime.select($"_lastUpdatedTime")) should equal(Set(null, 2))
    collectToSet[Long](dfWithManylastUpdatedTime.select($"___lastUpdatedTime")) should equal(Set(11, 22))
    collectToSet[Long](dfWithManylastUpdatedTime.select($"____lastUpdatedTime")) should equal(
      Set(111, 222))
  }

  it should "insert data with columns named _key, __key etc. as data columns key, _key, etc." in {
    val (columnNames1, unRenamed1) = prepareForInsert(dfWithKey)
    columnNames1.toSet should equal(Set("key", "value"))
    unRenamed1.schema.fieldNames should contain("key")
    unRenamed1.schema.fieldNames should contain(temporaryKeyName)
    collectToSet[String](unRenamed1.select(temporaryKeyName)) should equal(Set("key3", "key4"))
    collectToSet[String](unRenamed1.select("key")) should equal(Set("k1", "k2"))

    val (columnNames2, unRenamed2) = prepareForInsert(dfWithManyKeys)
    columnNames2.toSet should equal(Set("key", "__key", "___key", "value"))
    collectToSet[String](unRenamed2.select("key")) should equal(Set(null, "k2"))
    collectToSet[String](unRenamed2.select("__key")) should equal(Set("__k1", "__k2"))
    collectToSet[String](unRenamed2.select("___key")) should equal(Set("___k1", "___k2"))
  }

  it should "insert data with columns named _lastUpdatedTime, __lastUpdatedTime etc. as data columns lastUpdatedTime, _lastUpdatedTime, etc." in {
    val (columnNames1, unRenamed1) = prepareForInsert(dfWithlastUpdatedTime)
    columnNames1.toSet should equal(Set("lastUpdatedTime", "value"))
    collectToSet[Long](unRenamed1.select("lastUpdatedTime")) should equal(Set(1, 2))

    val (columnNames2, unRenamed2) = prepareForInsert(dfWithManylastUpdatedTime)
    columnNames2.toSet should equal(
      Set("lastUpdatedTime", "__lastUpdatedTime", "___lastUpdatedTime", "value"))
    collectToSet[Long](unRenamed2.select($"lastUpdatedTime")) should equal(Set(null, 2))
    collectToSet[Long](unRenamed2.select($"__lastUpdatedTime")) should equal(Set(11, 22))
    collectToSet[Long](unRenamed2.select($"___lastUpdatedTime")) should equal(Set(111, 222))
  }

  it should "read nested StructType" in {
    val schema = dfWithSimpleNestedStruct.schema
    schema.fieldNames should contain("nested")
    val nestedSchema = schema.fields(schema.fieldIndex("nested")).dataType.asInstanceOf[StructType]
    nestedSchema.fieldNames should (contain("field").and(contain("field2")))

    collectToSet[String](dfWithSimpleNestedStruct.selectExpr("nested.field")) should equal(Set("Ř"))
    collectToSet[Int](dfWithSimpleNestedStruct.selectExpr("nested.field2")) should equal(Set(1))

  }

  "rowsToRawItems" should "return RawRows from Rows" in {
    val (columnNames, unRenamed) = prepareForInsert(dfWithKey)
    val rawItems: Seq[RawRow] =
      RawJsonConverter.rowsToRawItems(columnNames, temporaryKeyName, unRenamed.collect.toSeq)
    rawItems.map(_.key.toString).toSet should equal(Set("key3", "key4"))

    val expectedResult: Seq[Map[String, Json]] = Seq[Map[String, Json]](
      Map(("key" -> Json.fromString("k1")), ("value" -> Json.fromInt(1))),
      Map(("key" -> Json.fromString("k2")), ("value" -> Json.fromInt(2)))
    )

    rawItems.map(_.columns) should equal(expectedResult)
  }

  it should "unrename _key" in {
    val data = Seq(
      ("notkey1", "key1", 1),
      ("notkey2", "key2", 2)
    )

    val dfWithKey = data.toDF("_key", "key", "value2")
    val (columnNames, unRenamed) = prepareForInsert(dfWithKey)
    val toInsert =
      RawJsonConverter.rowsToRawItems(columnNames, temporaryKeyName, unRenamed.collect.toSeq).toVector

    toInsert.map(_.key) shouldBe Vector("key1", "key2")
    toInsert.map(_.columns.get("key")) shouldBe Vector(
      Some(Json.fromString("notkey1")),
      Some(Json.fromString("notkey2")))
  }

  it should "throw an CDFSparkIllegalArgumentException when DataFrame has null key" in {
    val dataWithNullKey = Seq(
      ("k3", null, 1),
      ("k4", "key4", 2)
    )

    val dfWithKey = dataWithNullKey.toDF("_key", "key", "value2")
    val (columnNames, unRenamed) = prepareForInsert(dfWithKey)
    an[CdfSparkIllegalArgumentException] should be thrownBy RawJsonConverter
      .rowsToRawItems(columnNames, temporaryKeyName, unRenamed.collect.toSeq)
      .toArray
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

  it should "check partition sizes for partitions=10" taggedAs (ReadTest) in {
    val shortRand = shortRandomString()
    val metricsPrefix = s"partitionSizeTest$shortRand"
    val tablename = "bigTable"
    val resourceType = s"raw.testdb.$tablename"
    val partitions = 10

    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", partitions)
      .option("database", "testdb")
      .option("table", tablename)
      .option("inferSchema", "true")
      .option("collectTestMetrics", true)
      .option("parallelismPerPartition", 1)
      .load()
    df.createTempView(s"futureEvents$shortRand")
    val totalRows = spark.sqlContext
      .sql(s"select * from futureEvents$shortRand")
      .count()
    val partitionSizes = for (partitionIndex <- 0 until partitions)
      yield getPartitionSize(metricsPrefix, resourceType, partitionIndex)
    assert(partitionSizes.sum == totalRows)
    val expectedSize = totalRows / partitions
    assert(
      partitionSizes.forall(
        checkRange(expectedSize - expectedSize * 0.20, expectedSize + expectedSize * 0.20, _)))
  }

  it should "handle various numbers of partitions" taggedAs (ReadTest) in {
    for (partitions <- Seq("1", "5", "10", "20")) {
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

  it should "read individual columns successfully" taggedAs (ReadTest) in {
    val dfArray = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchema", true)
      .load()
      .limit(10)
      .select("key", "source", "subtype")
      .collect()

    assert(dfArray.map(_.getAs[String]("key")).forall(k => k.toInt > 0))
    assert(dfArray.map(_.getAs[String]("source")).forall(_ == "generator"))
    assert(dfArray.map(_.getAs[String]("subtype")).forall(_ == "past"))
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

    val source = spark.sql(s"""select
         |  '$key' as key,
         |  struct(
         |    123                        as long,
         |    'foo'                      as string,
         |    cast(null as string)       as `null`,
         |    named_struct('message', 'asd') as namedstruct,
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
      assert(struct.getAs[String]("null") == null)
      assert(struct.getAs[Row]("namedstruct").getAs[String]("message") == "asd")

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

  it should "create the table with ensureParent option" in {
    val database = "testdb"
    val table = "ensureParent-test-" + shortRandomString()

    // remove the DB to be sure
    try {
      writeClient.rawTables(database).deleteById(table)
    } catch {
      case _: CdpApiException => ()// Ignore
    }

    val key = shortRandomString()

    try {
      val source = spark.sql(
        s"""select
           |  '$key' as key,
           |  123 as something
           |""".stripMargin)
      val destination = spark.read
        .format("cognite.spark.v1")
        .schema(source.schema)
        .option("apiKey", writeApiKey)
        .option("type", "raw")
        .option("database", database)
        .option("table", table)
        .option("rawEnsureParent", "true")
        .load()
      destination.createTempView("ensureParent_test")
      source
        .select(destination.columns.map(c => col(c)): _*)
        .write
        .insertInto("ensureParent_test")

    } finally {
      try {
        writeClient.rawTables(database).deleteById(table)
      } catch {
        case _: CdpApiException => ()// Ignore
      }
    }
  }

  it should "be able to duplicate a table with a large number of columns(384)" taggedAs WriteTest in {
    val source = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "MegaColumnTable")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    val dest = spark.read
      .format("cognite.spark.v1")
      .schema(source.schema)
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "MegaColumnTableDuplicate")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()
    dest.createTempView("megaColumnTableDuplicateTempView")

    source
      .select("*")
      .write
      .insertInto("megaColumnTableDuplicateTempView")

    assert(source.count() == dest.count())
  }

  it should "be treated as a 'select *' when the column names combined, exceeds the character limit of 200" taggedAs WriteTest in {
    val source = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "MegaColumnTable")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    val dest = spark.read
      .format("cognite.spark.v1")
      .schema(source.schema)
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "testdb")
      .option("table", "MegaColumnTableDuplicate2")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()
    dest.createTempView("megaColumnTableDuplicate2TempView")

    val allColumnNamesAsString = source.schema.fields.map(_.name).mkString(",")
    assert(allColumnNamesAsString.length > 200)

    source
      .select(source.schema.fields.map(f => col(f.name)): _*)
      .write
      .insertInto("megaColumnTableDuplicate2TempView")

    assert(source.count() == dest.count())
  }

  it should "fail reasonably when table does not exist" in {
    val source = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("database", "datybasy")
      .option("table", "assets")
      .option("inferSchema", "false")
      .load()
    source.createTempView("table_which_does_not_exist")

    // This query causes Spark to crash in Jetfire, let's try how does it work in here

    val query =
      """
        WITH
          raw_table AS (
            SELECT
            from_json(columns,
            '''
            externalId string
            ''') as json
            FROM table_which_does_not_exist
          )
        select first(externalId)
        from (select json.externalId as externalId from raw_table)
        GROUP BY concat_ws(':', externalId, externalId)
        """

    val exception = sparkIntercept(spark.sql(query).collect())
    assert(exception.getMessage.contains("Following databases not found: datybasy."))

    // just test that Spark did not die in the process
    val select1result = spark.sql("select 1 as col").collect()
    assert(select1result.map(_.getInt(0)).toList == List(1))
  }

  // It's a weird use case, but some customer complained when we broke this, so let's make sure we don't do that again :)
  "RawJsonConverter" should "handle empty string as null for Byte type" in {
    val schema = dfWithEmptyStringInByteField.schema
    schema("byte").dataType shouldBe LongType
    dfWithEmptyStringInByteField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInByteField
      .collect()
      .map(_.getAs[Any]("byte"))
      .toSet shouldBe Set(null, 1.toByte) // scalastyle:off null
  }

  it should "handle empty string as null for Short type" in {
    val schema = dfWithEmptyStringInShortField.schema
    schema("short").dataType shouldBe LongType
    dfWithEmptyStringInShortField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInShortField
      .collect()
      .map(_.getAs[Any]("short"))
      .toSet shouldBe Set(null, 12.toShort) // scalastyle:off null
  }

  it should "handle empty string as null for Integer type" in {
    val schema = dfWithEmptyStringInIntegerField.schema
    schema("integer").dataType shouldBe LongType
    dfWithEmptyStringInIntegerField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInIntegerField
      .collect()
      .map(_.getAs[Any]("integer"))
      .toSet shouldBe Set(null, 123) // scalastyle:off null
  }

  it should "handle empty string as null for Long type" in {
    val schema = dfWithEmptyStringInLongField.schema
    schema("long").dataType shouldBe LongType
    dfWithEmptyStringInLongField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInLongField
      .collect()
      .map(_.getAs[Any]("long"))
      .toSet shouldBe Set(null, 12345L) // scalastyle:off null
  }

  it should "handle empty string as null for Double type" in {
    val schema = dfWithEmptyStringInDoubleField.schema
    schema("num").dataType shouldBe DoubleType
    dfWithEmptyStringInDoubleField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInDoubleField
      .collect()
      .map(_.getAs[Any]("num"))
      .toSet shouldBe Set(null, 12.3) // scalastyle:off null
  }

  it should "handle empty string as null for Boolean type" in {
    val schema = dfWithEmptyStringInBooleanField.schema
    schema("bool").dataType shouldBe BooleanType
    dfWithEmptyStringInBooleanField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set(
      "k1",
      "k2",
      "k3")
    dfWithEmptyStringInBooleanField
      .collect()
      .map(_.getAs[Any]("bool"))
      .toSet shouldBe Set(null, true, false) // scalastyle:off null
  }

  it should "fail reasonably on invalid types" in {
    val schema: StructType = StructType(
      Seq(
        StructField("key", DataTypes.StringType),
        StructField("lastUpdatedTime", DataTypes.TimestampType),
        StructField("value", DataTypes.FloatType)
      ))
    val converter =
      RawJsonConverter.makeRowConverter(schema, Array("value"), "lastUpdatedTime", "key")

    val testRow = RawRow("k", Map("value" -> Json.fromString("test")))

    val err = intercept[SparkRawRowMappingException](converter.apply(testRow))

    err.getMessage shouldBe "Error while loading RAW row [key='k'] in column 'value': java.lang.NumberFormatException: For input string: \"test\""

  }
}
