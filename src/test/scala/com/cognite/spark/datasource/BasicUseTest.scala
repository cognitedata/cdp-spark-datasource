package com.cognite.spark.datasource

import com.softwaremill.sttp._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class BasicUseTest extends FunSuite with SparkTest with CdpConnector {
  val apiKey = System.getenv("TEST_API_KEY")

  test("smoke test time series metadata") {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .load()
    assert(df.count() == 5)
  }

  test("smoke test assets") {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", apiKey)
      .option("type", "assets")
      .option("batchSize", "1000")
      .option("limit", "1000")
      .load()

    df.createTempView("assets")
    val res = spark.sql("select * from assets")
      .collect()
    assert(res.length == 6)
  }

  test("smoke test tables") {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", apiKey)
      .option("type", "tables")
      .option("batchSize", "100")
      .option("limit", "1000")
      .option("database", "testdb")
      .option("table", "cryptoAssets")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    df.createTempView("tables")
    val res = spark.sqlContext.sql("select * from tables")
        .collect()
    assert(res.length == 1000)
  }

  test("smoke test events") {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", apiKey)
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

  test("that all fields are nullable for events") {
    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "events")
      .load()
    destinationDf.createOrReplaceTempView("destinationEvent")

    val source = "nulltest"
    cleanupEvents(source)
    assert(eventDescriptions(source).isEmpty)

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

    val rows = spark.sql(s"""select * from destinationEvent where source = "$source"""")
    assert(rows.count() == 1)
    val storedMetadata = rows.head.getAs[Map[String, String]](6)
    assert(storedMetadata.size == 1)
    assert(storedMetadata.get("foo").contains("bar"))
  }

  test("smoke test pushing of events and upsert") {
    val sourceDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", apiKey)
      .option("type", "tables")
      .option("limit", "1000")
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchemaLimit", "10")
      .option("inferSchema", "true")
      .load()

    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", apiKey)
      .option("type", "events")
      .load()
    destinationDf.createOrReplaceTempView("destinationEvent")

    val source = "test"
    sourceDf.createTempView("sourceEvent")
    sourceDf.cache()

    // Cleanup events
    cleanupEvents(source)
    assert(eventDescriptions(source).isEmpty)

    // Post new events
    spark.sql(s"""
       |select "bar" as description,
       |to_unix_timestamp(startTime, 'yyyy-MM-dd') as startTime,
       |to_unix_timestamp(endTime, 'yyyy-MM-dd') as endTime,
       |type,
       |subtype,
       |null as assetIds,
       |bigint(0) as id,
       |map() as metadata,
       |"$source" as source,
       |sourceId
       |from sourceEvent
       |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if post worked
    val descriptionsAfterPost = eventDescriptions(source)
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "bar"))

    // Update events
    spark.sql(s"""
         |select "foo" as description,
         |to_unix_timestamp(startTime, 'yyyy-MM-dd') as startTime,
         |to_unix_timestamp(endTime, 'yyyy-MM-dd') as endTime,
         |type,
         |subtype,
         |null as assetIds,
         |bigint(0) as id,
         |map() as metadata,
         |"$source" as source,
         |sourceId
         |from sourceEvent
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if upsert worked
    val descriptionsAfterUpdate = eventDescriptions(source)
    assert(descriptionsAfterUpdate.length == 1000)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))
  }

  def cleanupEvents(source: String): Unit = {
    import io.circe.generic.auto._

    val events = get[EventItem](
      apiKey,
      uri"https://api.cognitedata.com/api/0.6/projects/jetfiretest2/events?source=$source",
      batchSize = 1000,
      limit = None,
      maxRetries = 10)

    val eventIdsChunks = events.flatMap(_.id).grouped(1000)
    for (eventIds <- eventIdsChunks) {
      post(
        apiKey,
        uri"https://api.cognitedata.com/api/0.6/projects/jetfiretest2/events/delete",
        eventIds,
        10
      ).unsafeRunSync()
    }
  }
}
