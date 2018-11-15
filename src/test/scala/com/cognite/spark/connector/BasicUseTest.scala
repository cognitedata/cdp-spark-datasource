package com.cognite.spark.connector

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BasicUseTest extends FunSuite with DataFrameSuiteBase {
  val apiKey = System.getenv("TEST_API_KEY")
  test("Use our own custom format for timeseries") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("tagId", "Bitbay USD")
      .load()
    assert(df.schema.length == 3)

    assert(df.schema.fields.sameElements(Array(StructField("tagId", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("value", DoubleType, nullable = true))))
  }

  test("Iterate over period longer than limit") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "40")
      .option("limit", "100")
      .option("tagId", "Bitbay USD")
      .load()
      .where("timestamp > 0 and timestamp < 1790902000001")
    assert(df.count() == 100)
  }

  test("test that we handle initial data set below batch size.") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("tagId", "Bitbay USD")
      .load()
    assert(df.count() == 100)
  }

  test("test that we handle initial data set with the same size as the batch size.") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "100")
      .option("limit", "100")
      .option("tagId", "Bitbay USD")
      .load()
      .where("timestamp >= 0 and timestamp <= 1790902000001")
    assert(df.count() == 100)
  }
  test("test that start/stop time are handled correctly for timeseries") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "100")
      .option("limit", "100")
      .option("tagId", "stopTimeTest")
      .load()
    assert(df.count() == 2)
  }
  test("smoke test assets") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "assets")
      .option("batchSize", "1000")
      .option("limit", "1000")
      .load()

    df.createTempView("assets")
    val res = sqlContext.sql("select * from assets")
      .collect()
    assert(res.length == 6)
  }

  test("smoke test tables") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
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
    val res = sqlContext.sql("select * from tables")
        .collect()
    assert(res.length == 1000)
  }

  test("smoke test events") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "events")
      .option("batchSize", "500")
      .option("limit", "1000")
      .load()

    df.createTempView("events")
    val res = sqlContext.sql("select * from events")
      .collect()
    assert(res.length == 1000)
  }

  test("smoke test pushing of events and upsert") {
    val sourceDf = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "tables")
      .option("limit", "1000")
      .option("database", "testdb")
      .option("table", "future-event")
      .option("inferSchemaLimit", "10")
      .option("inferSchema", "true")
      .load()

    val destinationDf = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "events")
      .load()
    destinationDf.createTempView("destinationEvent")

    val source = "test"
    sourceDf.createTempView("sourceEvent")
    sourceDf.cache()

    def eventDescriptions() = sqlContext.sql(s"""select description, source from destinationEvent where source = "$source"""")
      .select(col("description"))
      .collect()

    // Cleanup events
    cleanupEvents(source)
    assert(eventDescriptions().isEmpty)

    // Post new events
    sqlContext.sql(s"""
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
    val descriptionsAfterPost = eventDescriptions()
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "bar"))

    // Update events
    sqlContext.sql(s"""
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
    val descriptionsAfterUpdate = eventDescriptions()
    assert(descriptionsAfterUpdate.length == 1000)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))
  }

  def cleanupEvents(source: String): Unit = {
    import io.circe.generic.auto._
    val events = CdpConnector.get[EventItem](
      apiKey,
      EventsRelation.baseEventsURL("jetfiretest2").addQueryParameter("source", source).build(),
      batchSize = 1000,
      limit = None)

    val eventIdsChunks = events.flatMap(_.id).grouped(1000)
    for (eventIds <- eventIdsChunks) {
      CdpConnector.post(
        apiKey,
        EventsRelation.baseEventsURL("jetfiretest2").addPathSegment("delete").build(),
        eventIds,
        wantAsync = false
      )
    }
  }
}
