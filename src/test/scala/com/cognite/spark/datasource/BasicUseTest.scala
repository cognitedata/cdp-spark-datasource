package com.cognite.spark.datasource

import cats.effect.IO
import com.softwaremill.sttp._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.concurrent.TimeoutException

class BasicUseTest extends FunSuite with SparkTest with CdpConnector {

  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  test("smoke test time series metadata", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("limit", "100")
      .load()
    assert(df.count() == 100)
  }

  test("smoke test assets", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limit", "1000")
      .load()

    df.createTempView("assets")
    val res = spark.sql("select * from assets")
      .collect()
    assert(res.length == 1000)
  }

  test("assets with very small batchSize", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("batchSize", "1")
      .option("limit", "10")
      .load()

    assert(df.count() == 10)
  }
  //@TODO This uses jetfire2 until we have tables in publicdata
  test("smoke test raw") {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("batchSize", "100")
      .option("limit", "1000")
      .option("database", "testdb")
      .option("table", "cryptoAssets")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    df.createTempView("raw")
    val res = spark.sqlContext.sql("select * from raw")
        .collect()
    assert(res.length == 1000)
  }

  test("smoke test events", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("batchSize", "500")
      .option("limit", "1000")
      .load()

    df.createTempView("events")
    val res = spark.sqlContext.sql("select * from events")
      .collect()
    assert(res.length == 1000)
  }

  def eventDescriptions(source: String): Array[Row] = spark.sql(s"""select description, source from destinationEvent where source = "$source"""")
    .select(col("description"))
    .collect()

  test("that all fields are nullable for events", WriteTest) {
    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
    destinationDf.createOrReplaceTempView("destinationEvent")

    val source = "nulltest"
    cleanupEvents(source)
    val eventDescriptionsAfterCleanup = retryUntil[Array[Row]](eventDescriptions(source),
      rows => rows.nonEmpty)
    assert(eventDescriptionsAfterCleanup.isEmpty)

    spark.sql(s"""
              select
                 |null as id,
                 |null as startTime,
                 |null as endTime,
                 |null as description,
                 |null as type,
                 |null as subtype,
                 |map('foo', 'bar', 'nullValue', null) as metadata,
                 |null as assetIds,
                 |'nulltest' as source,
                 |null as sourceId
     """.stripMargin)
      .write
      .insertInto("destinationEvent")

    val rows = retryUntil[DataFrame](
      spark.sql(s"""select * from destinationEvent where source = "$source""""),
      rows => rows.count == 0)
    assert(rows.count() == 1)
    val storedMetadata = rows.head.getAs[Map[String, String]](6)
    assert(storedMetadata.size == 1)
    assert(storedMetadata.get("foo").contains("bar"))
  }

  test("smoke test pushing of events and upsert", WriteTest) {
    val sourceDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("limit", "1000")
      .load()

    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
    destinationDf.createOrReplaceTempView("destinationEvent")

    val source = "test"
    sourceDf.createTempView("sourceEvent")
    sourceDf.cache()

    // Cleanup events
    cleanupEvents(source)
    val eventDescriptionsReturned = retryUntil[Array[Row]](eventDescriptions(source),
      rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    // Post new events
    spark.sql(s"""
       |select "bar" as description,
       |startTime,
       |endTime,
       |type,
       |subtype,
       |null as assetIds,
       |bigint(0) as id,
       |map("foo", null, "bar", "test") as metadata,
       |"$source" as source,
       |sourceId
       |from sourceEvent
       |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if post worked
    val descriptionsAfterPost = retryUntil[Array[Row]](eventDescriptions(source),
      rows => rows.length < 100)
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "bar"))

    // Update events
    spark.sql(s"""
       |select "foo" as description,
       |startTime,
       |endTime,
       |type,
       |subtype,
       |null as assetIds,
       |bigint(0) as id,
       |metadata,
       |"$source" as source,
       |sourceId
       |from sourceEvent
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if upsert worked
    val descriptionsAfterUpdate = retryUntil[Array[Row]](eventDescriptions(source),
      rows => rows.length < 1000)
    assert(descriptionsAfterUpdate.length == 1000)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))
  }

  def retryUntil[A](action: => A, shouldRetry: A => Boolean): A =
    retryWithBackoff(
      IO {
        val actionValue = action
        if (shouldRetry(actionValue)) {
          throw new TimeoutException("Retry")
        }
        actionValue
      },
      Constants.DefaultInitialRetryDelay,
      Constants.DefaultMaxRetries
    ).unsafeRunSync()

  def cleanupEvents(source: String): Unit = {
    import io.circe.generic.auto._

    val project = getProject(writeApiKey, Constants.DefaultMaxRetries)

    val events = get[EventItem](
      writeApiKey,
      uri"https://api.cognitedata.com/api/0.6/projects/$project/events?source=$source",
      batchSize = 1000,
      limit = None,
      maxRetries = 10)

    val eventIdsChunks = events.flatMap(_.id).grouped(1000)
    for (eventIds <- eventIdsChunks) {
      post(
        writeApiKey,
        uri"https://api.cognitedata.com/api/0.6/projects/$project/events/delete",
        eventIds,
        10
      ).unsafeRunSync()
    }
  }
}
