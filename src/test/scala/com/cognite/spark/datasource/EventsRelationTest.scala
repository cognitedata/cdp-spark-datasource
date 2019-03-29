package com.cognite.spark.datasource
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import com.softwaremill.sttp._
import org.apache.spark.SparkException
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
      .option("partitions", "1")
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

  val destinationDf: DataFrame = spark.read.format("com.cognite.spark.datasource")
    .option("apiKey", writeApiKey)
    .option("type", "events")
    .load()
  destinationDf.createOrReplaceTempView("destinationEvent")

  val sourceDf: DataFrame = spark.read.format("com.cognite.spark.datasource")
    .option("apiKey", readApiKey)
    .option("type", "events")
    .option("limit", "1000")
    .option("partitions", "1")
    .load()
  sourceDf.createOrReplaceTempView("sourceEvent")
  sourceDf.cache()

  it should "allow null values for all event fields" taggedAs WriteTest in {

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

    val rows = retryWhile[Array[Row]](
      spark.sql(s"""select * from destinationEvent where source = "$source"""").collect,
      rows => rows.length < 1)
    assert(rows.length == 1)
    val storedMetadata = rows.head.getAs[Map[String, String]](6)
    assert(storedMetadata.size == 1)
    assert(storedMetadata.get("foo").contains("bar"))
  }

  it should "support upserts" taggedAs WriteTest in {
    val source = "spark-events-test"

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

    val dfWithCorrectAssetIds = retryWhile[Array[Row]](
      spark.sql("select * from destinationEvent where assetIds = array(2091657868296883)").collect,
      rows => rows.length < 1000)
    assert(dfWithCorrectAssetIds.length == 1000)
  }

  it should "allow inserts in savemode" taggedAs WriteTest in {
    val source = "spark-savemode-insert-test"

    // Cleanup events
    cleanupEvents(source)
    val eventDescriptionsReturned = retryWhile[Array[Row]](eventDescriptions(source),
      rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    // Test inserts
    spark.sql(s"""
                 |select "foo" as description,
                 |startTime,
                 |endTime,
                 |type,
                 |subtype,
                 |array(8031965690878131) as assetIds,
                 |bigint(0) as id,
                 |metadata,
                 |"$source" as source,
                 |sourceId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
                 |limit 100
     """.stripMargin)
      .write
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .save

    val dfWithSourceInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'").collect,
      df => df.length < 100
    )
    assert(dfWithSourceInsertTest.length == 100)

    // Trying to insert existing rows should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
      spark.sql(
        s"""
           |select "foo" as description,
           |startTime,
           |endTime,
           |type,
           |subtype,
           |array(8031965690878131) as assetIds,
           |bigint(0) as id,
           |metadata,
           |"$source" as source,
           |sourceId,
           |createdTime,
           |lastUpdatedTime
           |from sourceEvent
           |limit 100
     """.stripMargin)
        .write
        .format("com.cognite.spark.datasource")
        .option("apiKey", writeApiKey)
        .option("type", "events")
        .save
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 409)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "allow updates in savemode" taggedAs WriteTest in {
    val source = "spark-savemode-updates-test"

    // Cleanup old events
    cleanupEvents(source)
    val dfWithUpdatesAsSource = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'")collect,
      df => df.length > 0)
    assert(dfWithUpdatesAsSource.length == 0)

    // Insert some test data
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

    // Update the data
    spark.sql(
      s"""
         |select "bar" as description,
         |startTime,
         |endTime,
         |type,
         |subtype,
         |assetIds,
         |id,
         |metadata,
         |source,
         |sourceId,
         |createdTime,
         |lastUpdatedTime
         |from destinationEvent
         |where source = '$source'
      """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .option("onconflict", "update")
      .insertInto("destinationEvent")

    // Check if update worked
    val descriptionsAfterUpdate = retryWhile[Array[Row]](eventDescriptions(source),
      rows => rows.length < 100)
    assert(descriptionsAfterUpdate.length == 100)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

    // Trying to update non-existing ids should throw a 400 CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
    // Update the data
    spark.sql(
      s"""
         |select "bar" as description,
         |startTime,
         |endTime,
         |type,
         |subtype,
         |assetIds,
         |bigint(1) as id,
         |metadata,
         |source,
         |sourceId,
         |createdTime,
         |lastUpdatedTime
         |from destinationEvent
         |where source = '$source'
         |limit 1
        """.stripMargin)
      .write
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "update")
      .save
      }
      e.getCause shouldBe a[CdpApiException]
      val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
      assert(cdpApiException.code == 400)
      spark.sparkContext.setLogLevel("WARN")
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
