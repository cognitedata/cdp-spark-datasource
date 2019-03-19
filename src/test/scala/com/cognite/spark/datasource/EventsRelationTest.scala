package com.cognite.spark.datasource
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import com.softwaremill.sttp._
import org.scalatest.{FlatSpec, Matchers}

class EventsRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  "EventsRelation" should "allow simple reads" taggedAs ReadTest in {
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

  it should "apply a single pushdown filter" taggedAs WriteTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
      .where(s"type = 'alert'")
    assert(df.count == 8)
  }

  it should "apply multiple pushdown filters" taggedAs WriteTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
      .where(s"type = 'maintenance' and subtype = 'new'")
    assert(df.count == 2)
  }

  it should "handle or conditions" taggedAs WriteTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
      .where(s"type = 'maintenance' or type = 'upgrade'")
    assert(df.count == 9)
  }

  it should "handle in() conditions" taggedAs WriteTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
      .where(s"type in('alert','replacement')")
    assert(df.count == 12)
  }

  it should "handle and, or and in() in one query" taggedAs WriteTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
      .where(s"(type = 'maintenance' or type = 'upgrade') and subtype in('manual', 'automatic')")
    assert(df.count == 4)
  }

  def eventDescriptions(source: String): Array[Row] = spark.sql(s"""select description, source from destinationEvent where source = "$source"""")
    .select(col("description"))
    .collect()

  it should "allow null values for all event fields" taggedAs WriteTest in {
    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
    destinationDf.createOrReplaceTempView("destinationEvent")

    val source = "nulltest"
    cleanupEvents(source)
    val eventDescriptionsAfterCleanup = retryWhile[Array[Row]](eventDescriptions(source),
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
                 |null as sourceId,
                 |0 as createdTime,
                 |0 as lastUpdatedTime
     """.stripMargin)
      .write
      .insertInto("destinationEvent")

    val rows = retryWhile[DataFrame](
      spark.sql(s"""select * from destinationEvent where source = "$source""""),
      rows => rows.count == 0)
    assert(rows.count() == 1)
    val storedMetadata = rows.head.getAs[Map[String, String]](6)
    assert(storedMetadata.size == 1)
    assert(storedMetadata.get("foo").contains("bar"))
  }

  it should "support upserts" taggedAs WriteTest in {
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

    val source = "spark-events-test"
    sourceDf.createTempView("sourceEvent")
    sourceDf.cache()

    // Cleanup events
    cleanupEvents(source)
    val eventDescriptionsReturned = retryWhile[Array[Row]](eventDescriptions(source),
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
                 |sourceId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
                 |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if post worked
    val descriptionsAfterPost = retryWhile[Array[Row]](eventDescriptions(source),
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
                 |array(2091657868296883) as assetIds,
                 |bigint(0) as id,
                 |metadata,
                 |"$source" as source,
                 |sourceId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if upsert worked
    val descriptionsAfterUpdate = retryWhile[Array[Row]](eventDescriptions(source),
      rows => rows.length < 1000)
    assert(descriptionsAfterUpdate.length == 1000)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))

    val dfWithCorrectAssetIds = retryWhile[DataFrame](
      spark.sql("select * from destinationEvent where assetIds = array(2091657868296883)"),
      rows => rows.count < 1000)
    assert(dfWithCorrectAssetIds.count == 1000)
  }

  def cleanupEvents(source: String): Unit = {
    import io.circe.generic.auto._

    val project = getProject(writeApiKey, Constants.DefaultMaxRetries, Constants.DefaultBaseUrl)

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
