package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.CdpConnector.ioRuntime
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{RawDatabase, RawRow, RawTable}
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, LoneElement, Matchers, ParallelTestExecution}

import java.lang.{Long => JavaLong}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID
import scala.reflect.ClassTag

object RawTableRelationTest {
  // With ParallelTestExecution it's not trivial to have before/after hooks
  // do the shared setup as each thread gets own test class instance
  // Namely we should have shared random part, quick hack it to put it statically here
  val randomDbNameForTests = s"spark-test-database-${UUID.randomUUID().toString.substring(0, 8)}"
}

class RawTableRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with LoneElement
    with BeforeAndAfterAll {
  import RawTableRelation._
  import spark.implicits._

  private def collectToSet[A: ClassTag](df: DataFrame): Set[A] =
    df.collect().map(_.getAs[A](0)).toSet

  private def checkRange(leftLimit: Double, rightLimit: Double, number: Long): Boolean =
    (number >= leftLimit) && (number <= rightLimit)

  private val dataWithoutKey = Seq(
    RawRow("key1", Map("notKey" -> Json.fromString("k1"), "value" -> Json.fromInt(1))),
    RawRow("key2", Map("notKey" -> Json.fromString("k2"), "value" -> Json.fromInt(2)))
  )
  private val dataWithKey = Seq(
    RawRow("key3", Map("key" -> Json.fromString("k1"), "value" -> Json.fromInt(1))),
    RawRow("key4", Map("key" -> Json.fromString("k2"), "value" -> Json.fromInt(2)))
  )
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

  private val dataWithoutlastUpdatedTime = Seq(
    RawRow("key1", Map("notlastUpdatedTime" -> Json.fromInt(1), "value" -> Json.fromInt(1))),
    RawRow("key2", Map("notlastUpdatedTime" -> Json.fromInt(2), "value" -> Json.fromInt(2)))
  )
  private val dataWithlastUpdatedTime = Seq(
    RawRow("key3", Map("lastUpdatedTime" -> Json.fromInt(1), "value" -> Json.fromInt(1))),
    RawRow("key4", Map("lastUpdatedTime" -> Json.fromInt(2), "value" -> Json.fromInt(2)))
  )
  private val dataWithManylastUpdatedTime = Seq(
    RawRow(
      "key5",
      Map(
        "___lastUpdatedTime" -> Json.fromInt(111),
        "__lastUpdatedTime" -> Json.fromInt(11),
        "value" -> Json.fromInt(1))),
    RawRow(
      "key6",
      Map(
        "___lastUpdatedTime" -> Json.fromInt(222),
        "value" -> Json.fromInt(2),
        "__lastUpdatedTime" -> Json.fromInt(22),
        "lastUpdatedTime" -> Json.fromInt(2)))
  )

  private val dataWithSimpleNestedStruct = Seq(
    RawRow("k", Map("nested" -> Json.obj("field" -> Json.fromString("Ř"), "field2" -> Json.fromInt(1))))
  )

  private val dataWithEmptyStringInByteField = Seq(
    RawRow("k1", Map("byte" -> Json.fromString(""))),
    RawRow("k2", Map("byte" -> Json.fromInt(1.toByte.toInt)))
  )
  private val dataWithEmptyStringInShortField = Seq(
    RawRow("k1", Map("short" -> Json.fromString(""))),
    RawRow("k2", Map("short" -> Json.fromInt(12.toShort.toInt)))
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

  case class TestTable(name: String, data: Seq[RawRow])
  case class TestData(dbName: String, tables: Seq[TestTable])

  private val testData = TestData(
    dbName = RawTableRelationTest.randomDbNameForTests,
    tables = Seq(
      TestTable("without-key", dataWithoutKey),
      TestTable("with-key", dataWithKey),
      TestTable("with-many-keys", dataWithManyKeys),
      TestTable("without-lastUpdatedTime", dataWithoutlastUpdatedTime),
      TestTable("with-lastUpdatedTime", dataWithlastUpdatedTime),
      TestTable("with-many-lastUpdatedTime", dataWithManylastUpdatedTime),
      TestTable("with-nesting", dataWithSimpleNestedStruct),
      TestTable("with-byte-empty-str", dataWithEmptyStringInByteField),
      TestTable("with-short-empty-str", dataWithEmptyStringInShortField),
      TestTable("with-integer-empty-str", dataWithEmptyStringInIntegerField),
      TestTable("with-long-empty-str", dataWithEmptyStringInLongField),
      TestTable("with-number-empty-str", dataWithEmptyStringInDoubleField),
      TestTable("with-boolean-empty-str", dataWithEmptyStringInBooleanField),
      TestTable("cryptoAssets", (1 to 500).map(i =>
        RawRow(i.toString, Map("i" -> Json.fromString("exist")))
      )),
      TestTable("future-event", (1 to 100).map(i => {
        val time = if (i >= 20 && i < 30) {
            Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2019-06-21T11:48:01.000Z")).atZone(ZoneId.of("UTC"))
          } else {
            LocalDateTime.now().atZone(ZoneId.of("UTC"))
          }
        RawRow(i.toString, Map(
          "key" -> i.toString,
          "startTime" -> s"${time.plusDays(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}",
          "endTime" -> s"${time.plusDays(2).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}",
          "description" -> s"event $i",
          "source" -> "generator",
          "sourceId" -> s"test id $i",
          "subtype" -> "past",
          "type" -> "test type"
        ).map { case (k, v) => (k, Json.fromString(v)) })
      })),
      TestTable("bigTable", (1 to 1000).map(i =>
        RawRow(i.toString, Map("i" -> Json.fromString("exist")))
      )),
      TestTable("raw-write-test", Seq.empty), // used for writes
      TestTable("MegaColumnTable", Seq(
        RawRow("rowkey", (1 to 384).map(i =>
          (i.toString -> Json.fromString("value"))).toMap
        )
      )),
      TestTable("MegaColumnTableDuplicate", Seq(
        RawRow("rowkey", (1 to 384).map(i =>
          (i.toString -> Json.fromString("value"))).toMap
        )
      )),
      TestTable("MegaColumnTableDuplicate2", Seq.empty), // used for writes
      TestTable("struct-test", Seq.empty) // used for writes
    )
  )

  def createTestData: IO[Unit] = for {
    _ <- writeClient.rawDatabases.createOne(RawDatabase(testData.dbName))
    _ <- writeClient.rawTables(testData.dbName).create(testData.tables.map(_.name).map(RawTable))
    _ <- testData.tables.filterNot(_.data.isEmpty).toList.traverse(t =>
      writeClient.rawRows(testData.dbName, t.name).create(t.data))
  } yield ()

  def cleanupTestData: IO[Unit] = for {
    _ <- writeClient.rawTables(testData.dbName).deleteByIds(testData.tables.map(_.name))
    _ <- writeClient.rawDatabases.deleteById(testData.dbName)
  } yield ()

  override def beforeAll(): Unit = {
    createTestData.unsafeRunSync()
  }

  override def afterAll(): Unit = {
    cleanupTestData.unsafeRunSync()
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
    val limit = 7L
    val partitions = 1L
    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("limitPerPartition", limit)
      .option("partitions", partitions)
      .option("database", testData.dbName)
      .option("table", "cryptoAssets")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "7")
      .load()
    df.createTempView("raw")
    val res = spark.sqlContext
      .sql("select * from raw")
    assert(res.count() == limit * partitions)
  }

  def rawRead(
      table: String,
      database: String = "spark-test-database",
      inferSchema: Boolean = true,
      metricsPrefix: Option[String] = None): DataFrame = {
    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("partitions", 1)
      .option("database", database)
      .option("table", table)
      .option("inferSchema", inferSchema)
      .option("inferSchemaLimit", "100")

    metricsPrefix match {
      case Some(prefix) =>
        df.option("collectMetrics", "true")
          .option("metricsPrefix", prefix)
          .load()
      case None =>
        df.load()
    }
  }

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
    collectToSet[JavaLong](dfWithlastUpdatedTime.select($"_lastUpdatedTime")) should equal(Set(1, 2))

    dfWithManylastUpdatedTime.schema.fieldNames.toSet should equal(
      Set(
        "key",
        "lastUpdatedTime",
        "____lastUpdatedTime",
        "___lastUpdatedTime",
        "_lastUpdatedTime",
        "value"))

    collectToSet[java.sql.Timestamp](dfWithManylastUpdatedTime.select($"lastUpdatedTime"))
    collectToSet[JavaLong](dfWithManylastUpdatedTime.select($"_lastUpdatedTime")) should equal(
      Set(null, 2))
    collectToSet[JavaLong](dfWithManylastUpdatedTime.select($"___lastUpdatedTime")) should equal(
      Set(11, 22))
    collectToSet[JavaLong](dfWithManylastUpdatedTime.select($"____lastUpdatedTime")) should equal(
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
    collectToSet[JavaLong](unRenamed1.select("lastUpdatedTime")) should equal(Set(1, 2))

    val (columnNames2, unRenamed2) = prepareForInsert(dfWithManylastUpdatedTime)
    columnNames2.toSet should equal(
      Set("lastUpdatedTime", "__lastUpdatedTime", "___lastUpdatedTime", "value"))
    collectToSet[JavaLong](unRenamed2.select($"lastUpdatedTime")) should equal(Set(null, 2))
    collectToSet[JavaLong](unRenamed2.select($"__lastUpdatedTime")) should equal(Set(11, 22))
    collectToSet[JavaLong](unRenamed2.select($"___lastUpdatedTime")) should equal(Set(111, 222))
  }

  it should "read nested StructType" in {
    val schema = dfWithSimpleNestedStruct.schema
    schema.fieldNames should contain("nested")
    val nestedSchema = schema.fields(schema.fieldIndex("nested")).dataType.asInstanceOf[StructType]
    nestedSchema.fieldNames should (contain("field").and(contain("field2")))

    collectToSet[String](dfWithSimpleNestedStruct.selectExpr("nested.field")) should equal(Set("Ř"))
    collectToSet[JavaLong](dfWithSimpleNestedStruct.selectExpr("nested.field2")) should equal(Set(1))

  }

  "rowsToRawItems" should "return RawRows from Rows" in {
    val (columnNames, unRenamed) = prepareForInsert(dfWithKey)
    val rawItems: Seq[RawRow] =
      RawJsonConverter.rowsToRawItems(columnNames, temporaryKeyName, unRenamed.collect().toSeq)
    rawItems.map(_.key).toSet should equal(Set("key3", "key4"))

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
      RawJsonConverter.rowsToRawItems(columnNames, temporaryKeyName, unRenamed.collect().toSeq).toVector

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
      .rowsToRawItems(columnNames, temporaryKeyName, unRenamed.collect().toSeq)
      .toArray
  }

  "Infer Schema" should "use a different limit for infer schema" in {
    val metricsPrefix = "infer_schema_1"
    val database = testData.dbName
    val table = "future-event"
    val inferSchemaLimit = 1L
    val partitions = 10L
    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
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
    val _ = df.schema

    val numRowsReadDuringSchemaInference =
      getNumberOfRowsRead(metricsPrefix, s"raw.$database.$table.rows")
    numRowsReadDuringSchemaInference should be(inferSchemaLimit)
  }

  "lastUpdatedTime" should "insert data without error" taggedAs (WriteTest) in {
    val destinationDf = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
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
      .select(destinationDf.columns.map(col).toIndexedSeq: _*)
      .write
      .insertInto("destinationTable")
  }

  it should "test that lastUpdatedTime filters are handled correctly" taggedAs (ReadTest) in {
    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
      .option("table", "future-event")
      .option("inferSchema", true)
      .option("partitions", "5")
      .load()
      .where(s"lastUpdatedTime >= timestamp('2019-06-21 11:48:00.000Z')")
    assert(df.count() == 100)
  }

  it should "test that startTime filters are handled correctly" taggedAs (ReadTest) in {
    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
      .option("table", "future-event")
      .option("inferSchema", true)
      .option("partitions", "5")
      .load()
      .where(s"startTime >= timestamp('2019-06-22 11:48:00.000Z') and startTime <= timestamp('2019-06-22 11:50:00.000Z')")
    assert(df.count() == 10)
  }

  it should "check partition sizes for partitions=10" taggedAs (ReadTest) in {
    val shortRand = shortRandomString()
    val metricsPrefix = s"partitionSizeTest$shortRand"
    val tablename = "bigTable"
    val resourceType = s"raw.${RawTableRelationTest.randomDbNameForTests}.$tablename"
    val partitions = 10L

    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", partitions)
      .option("database", testData.dbName)
      .option("table", tablename)
      .option("inferSchema", "true")
      .option("collectTestMetrics", true)
      .option("parallelismPerPartition", 1)
      .load()
    df.createTempView(s"futureEvents$shortRand")
    val totalRows = spark.sqlContext
      .sql(s"select * from futureEvents$shortRand")
      .count()
    val partitionSizes = for (partitionIndex <- 0L until partitions)
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
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "raw")
        .option("database", testData.dbName)
        .option("table", "future-event")
        .option("inferSchema", true)
        .option("partitions", partitions)
        .load()
        .where(s"startTime >= timestamp('2019-06-21 11:48:00.000Z')")
      assert(df.count() == 100)
    }
  }

  it should "read individual columns successfully" taggedAs (ReadTest) in {
    val dfArray = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
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
    val database = testData.dbName

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
      .format(DefaultSource.sparkFormatString)
      .schema(source.schema)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", database)
      .option("table", "struct-test")
      .load()
    destination.createTempView(tempView)
    source
      .select(destination.columns.map(c => col(c)).toIndexedSeq: _*)
      .write
      .insertInto(tempView)

    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", database)
      .option("table", "struct-test")
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
  }

  it should "create the table with ensureParent option" in {
    val database = testData.dbName
    val table = "ensureParent-test"

    // remove the DB to be sure
    try {
      writeClient.rawTables(database).deleteById(table).unsafeRunSync()
    } catch {
      case _: CdpApiException => () // Ignore
    }

    val key = shortRandomString()

    try {
      val source = spark.sql(s"""select
           |  '$key' as key,
           |  123 as something
           |""".stripMargin)
      val destination = spark.read
        .format(DefaultSource.sparkFormatString)
        .schema(source.schema)
        .useOIDCWrite
        .option("type", "raw")
        .option("database", database)
        .option("table", table)
        .option("rawEnsureParent", "true")
        .load()
      destination.createTempView("ensureParent_test")
      source
        .select(destination.columns.map(c => col(c)).toIndexedSeq: _*)
        .write
        .insertInto("ensureParent_test")

    } finally {
      try {
        writeClient.rawTables(database).deleteById(table).unsafeRunSync()
      } catch {
        case _: CdpApiException => () // Ignore
      }
    }
  }

  it should "be able to duplicate a table with a large number of columns(384)" taggedAs WriteTest in {
    val source = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
      .option("table", "MegaColumnTable")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    val dest = spark.read
      .format(DefaultSource.sparkFormatString)
      .schema(source.schema)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
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
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
      .option("table", "MegaColumnTable")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    val dest = spark.read
      .format(DefaultSource.sparkFormatString)
      .schema(source.schema)
      .useOIDCWrite
      .option("type", "raw")
      .option("database", testData.dbName)
      .option("table", "MegaColumnTableDuplicate2")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()
    dest.createTempView("megaColumnTableDuplicate2TempView")

    val allColumnNamesAsString = source.schema.fields.map(_.name).mkString(",")
    assert(allColumnNamesAsString.length > 200)

    source
      .select(source.schema.fields.map(f => col(f.name)).toIndexedSeq: _*)
      .write
      .insertInto("megaColumnTableDuplicate2TempView")

    assert(source.count() == dest.count())
  }

  it should "support pushdown key filters with IN" taggedAs ReadTest in {
    val tableName = "with-boolean-empty-str"
    val metricsPrefix = s"pushdown.raw.key.${shortRandomString()}"
    val df = rawRead(tableName, metricsPrefix = Some(metricsPrefix))
      .where("key in ('some-invalid-key','k1','k3')")

    assert(df.count() == 2)

    val rowsRead = getNumberOfRowsRead(metricsPrefix, f"raw.spark-test-database.${tableName}.rows")
    assert(rowsRead == 2)
  }

  it should "support pushdown key filters with OR" taggedAs ReadTest in {
    val tableName = "with-boolean-empty-str"
    val metricsPrefix1 = s"pushdown.raw.key.${shortRandomString()}"
    val df1 = rawRead(tableName, metricsPrefix = Some(metricsPrefix1))
      .where("key = 'some-invalid-key' or key = 'k1' or key = 'k2'")

    assert(df1.count() == 2)

    val rowsRead1 = getNumberOfRowsRead(metricsPrefix1, s"raw.spark-test-database.$tableName.rows")
    assert(rowsRead1 == 2)

    // filter should not be pushed down if OR condition includes other fields
    val metricsPrefix2 = s"pushdown.raw.key.${shortRandomString()}"
    val df2 = rawRead(tableName, metricsPrefix = Some(metricsPrefix2))
      .where(
        "key = 'some-invalid-key' or key = 'k1' or key = 'k2' or key = 'k1' or lastUpdatedTime >= timestamp('2000-01-01 00:00:00.000Z')")

    assert(df2.count() == 3)

    val rowsRead2 = getNumberOfRowsRead(metricsPrefix2, s"raw.spark-test-database.$tableName.rows")
    assert(rowsRead2 == 3)
  }

  it should "support pushdown key filters with AND" taggedAs ReadTest in {
    val tableName = "with-boolean-empty-str"
    val metricsPrefix = s"pushdown.raw.key.${shortRandomString()}"
    val df = rawRead(tableName, metricsPrefix = Some(metricsPrefix))
      .where("key = 'k2' and lastUpdatedTime >= timestamp('2000-01-01 00:00:00.000Z')")

    assert(df.count() == 1)

    val rowsRead = getNumberOfRowsRead(metricsPrefix, s"raw.spark-test-database.$tableName.rows")
    assert(rowsRead == 1)
  }

  it should "support pushdown key filters with AND and IN resulting in an empty set of keys" taggedAs ReadTest in {
    val tableName = "with-boolean-empty-str"
    val metricsPrefix = s"pushdown.raw.key.${shortRandomString()}"
    val df = rawRead(tableName, metricsPrefix = Some(metricsPrefix))
      .where("key in ('k1') and key in ('k2')")

    assert(df.count() == 0)

    // No rows should have been read, so the metric should not exist.
    a[NoSuchElementException] should be thrownBy getNumberOfRowsRead(
      metricsPrefix,
      s"raw.spark-test-database.$tableName.rows")
  }

  it should "support pushdown key filters with OR and IN" taggedAs ReadTest in {
    val tableName = "with-boolean-empty-str"
    val metricsPrefix = s"pushdown.raw.key.${shortRandomString()}"
    val df = rawRead(tableName, metricsPrefix = Some(metricsPrefix))
      .where("key = 'some-invalid-key' OR key in ('k2')")

    assert(df.count() == 1)

    val rowsRead = getNumberOfRowsRead(metricsPrefix, s"raw.spark-test-database.$tableName.rows")
    assert(rowsRead == 1)
  }

  it should "fail reasonably when table does not exist" in {
    val source = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
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
      .toSet shouldBe Set(null, 1.toByte)
  }

  it should "handle empty string as null for Short type" in {
    val schema = dfWithEmptyStringInShortField.schema
    schema("short").dataType shouldBe LongType
    dfWithEmptyStringInShortField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInShortField
      .collect()
      .map(_.getAs[Any]("short"))
      .toSet shouldBe Set(null, 12.toShort)
  }

  it should "handle empty string as null for Integer type" in {
    val schema = dfWithEmptyStringInIntegerField.schema
    schema("integer").dataType shouldBe LongType
    dfWithEmptyStringInIntegerField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInIntegerField
      .collect()
      .map(_.getAs[Any]("integer"))
      .toSet shouldBe Set(null, 123)
  }

  it should "handle empty string as null for Long type" in {
    val schema = dfWithEmptyStringInLongField.schema
    schema("long").dataType shouldBe LongType
    dfWithEmptyStringInLongField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInLongField
      .collect()
      .map(_.getAs[Any]("long"))
      .toSet shouldBe Set(null, 12345L)
  }

  it should "handle empty string as null for Double type" in {
    val schema = dfWithEmptyStringInDoubleField.schema
    schema("num").dataType shouldBe DoubleType
    dfWithEmptyStringInDoubleField.collect().map(_.getAs[Any]("key")).toSet shouldBe Set("k1", "k2")
    dfWithEmptyStringInDoubleField
      .collect()
      .map(_.getAs[Any]("num"))
      .toSet shouldBe Set(null, 12.3)
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
      .toSet shouldBe Set(null, true, false)
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
