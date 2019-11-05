package cognite.spark.v1

import com.cognite.sdk.scala.common.{ApiKeyAuth, CdpApiException}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}

class EventsRelationTest extends FlatSpec with Matchers with SparkTest {
  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "events")
    .load()
  destinationDf.createOrReplaceTempView("destinationEvent")

  val sourceDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", readApiKey)
    .option("type", "events")
    .option("limitPerPartition", "1000")
    .option("partitions", "1")
    .load()
  sourceDf.createOrReplaceTempView("sourceEvent")
  sourceDf.cache()

  cleanupEvents("spark-savemode-insert-test")
  cleanupEvents("spark-events-savemode-test")
  cleanupEvents("spark-events-test")

  "EventsRelation" should "allow simple reads" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("batchSize", "500")
      .option("limitPerPartition", "100")
      .option("partitions", "10")
      .load()

    df.createTempView("events")
    val res = spark.sqlContext
      .sql("select * from events")
      .collect()
    assert(res.length == 1000)
  }

  it should "apply a single pushdown filter" taggedAs WriteTest in {
    val metricsPrefix = "single.pushdown.filter"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'alert'")
    assert(df.count == 8)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 8)
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs WriteTest in {
    val metricsPrefix = "single.pushdown.filter.duplicates"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "10")
      .load()
      .where(s"type = 'alert' or source = 'test data'")
    assert(df.count == 20)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 20)
  }

  it should "apply multiple pushdown filters" taggedAs WriteTest in {
    val metricsPrefix = "multiple.pushdown.filters"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'maintenance' and source = 'test data'")
    assert(df.count == 4)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 4)
  }

  it should "handle or conditions" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.or"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'maintenance' or type = 'upgrade'")
    assert(df.count == 9)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 9)
  }

  it should "handle in() conditions" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.in"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type in('alert','replacement')")
    assert(df.count == 12)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 12)
  }

  it should "handle and, or and in() in one query" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.and.or.in"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"(type = 'maintenance' or type = 'upgrade') and subtype in('manual', 'automatic')")
    assert(df.count == 4)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 4)
  }

  it should "handle pushdown filters on minimum startTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.minStartTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("startTime > to_timestamp(1554698747)")
    assert(df.count == 5)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 5)
  }

  it should "handle pushdown filters on maximum startTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.maxStartTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"startTime < to_timestamp(1540767) and source = 'generator'")
    assert(df.count == 5)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 5)
  }

  it should "handle pushdown filters on minimum and maximum startTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.minMaxStartTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"startTime < to_timestamp(2039468000) and startTime > to_timestamp(1539468000)")
    assert(df.count == 78)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 78)
  }

  it should "handle pushdown filters on assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"assetIds In(Array(8031965690878131), Array(2091657868296883))")
    assert(df.count == 100)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 100)
  }

  it should "handle pusdown filters on eventIds" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.id"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("id = 370545839260513")
    assert(df.count == 1)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1)
  }

  it should "handle pusdown filters on many eventIds" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.ids"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("id In(607444033860, 3965637099169, 10477877031034, 17515837146970, 19928788984614, 21850891340773)")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "handle pusdown filters on many eventIds with or" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.orids"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("""
          id = 607444033860 or id = 3965637099169 or id = 10477877031034 or
          id = 17515837146970 or id = 19928788984614 or id = 21850891340773""")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "handle pusdown filters on many eventIds with other filters" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.idsAndDescription"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("""id In(
        607444033860, 3965637099169,
        10477877031034, 17515837146970,
        19928788984614, 21850891340773) and description = "eventspushdowntest" """)
    assert(df.count == 1)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "handle a really advanced query" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.advanced"

    val df2 = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("limitPerPartition", "1000")
      .option("metricsPrefix", metricsPrefix)
      .load()

    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"((type = 'maintenance' or type = 'upgrade') " +
          s"and subtype in('manual', 'automatic')) " +
          s"or (type = 'maintenance' and subtype = 'manual') " +
          s"or (type = 'upgrade') and source = 'something'")
    assert(df.count == 4)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 4)
  }

  it should "handle pushdown on eventId or something else" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.idortype"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'maintenance' or id = 17515837146970")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "allow null values for all event fields except id" taggedAs WriteTest in {

    val source = "spark-events-test"
    cleanupEvents(source)
    val eventDescriptionsAfterCleanup =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.nonEmpty)
    assert(eventDescriptionsAfterCleanup.isEmpty)

    spark
      .sql(s"""
              select
                 |10 as id,
                 |null as startTime,
                 |null as endTime,
                 |null as description,
                 |null as type,
                 |null as subtype,
                 |map('foo', 'bar', 'nullValue', null) as metadata,
                 |null as assetIds,
                 |'$source' as source,
                 |null as externalId,
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
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    // Post new events
    spark
      .sql(s"""
                 |select "bar" as description,
                 |startTime,
                 |endTime,
                 |type,
                 |subtype,
                 |null as assetIds,
                 |bigint(0) as id,
                 |map("foo", null, "bar", "test") as metadata,
                 |"$source" as source,
                 |id as externalId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
                 |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if post worked
    val descriptionsAfterPost =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.length != 100)
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "bar"))

    // Update events
    spark
      .sql(s"""
                 |select "foo" as description,
                 |startTime,
                 |endTime,
                 |type,
                 |subtype,
                 |array(2091657868296883) as assetIds,
                 |bigint(0) as id,
                 |map("some", null, "metadata", "test") as metadata,
                 |"$source" as source,
                 |id as externalId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
                 |limit 500
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if upsert worked
    val descriptionsAfterUpdate =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.length != 500)
    assert(descriptionsAfterUpdate.length == 500)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))

    val dfWithCorrectAssetIds = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where assetIds = array(2091657868296883) and source = '$source'").collect,
      rows => rows.length != 500)
    assert(dfWithCorrectAssetIds.length == 500)
  }

  it should "allow inserts in savemode" taggedAs WriteTest in {
    val source = "spark-events-test"

    // Cleanup events
    cleanupEvents(source)
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    // Test inserts
    spark
      .sql(s"""
                 |select "foo" as description,
                 |least(startTime, endTime) as startTime,
                 |greatest(startTime, endTime) as endTime,
                 |type,
                 |subtype,
                 |array(8031965690878131) as assetIds,
                 |bigint(0) as id,
                 |map("foo", null, "bar", "test") as metadata,
                 |"$source" as source,
                 |string(id) as externalId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
                 |limit 100
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .save()

    val dfWithSourceInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'").collect,
      df => df.length < 100
    )
    assert(dfWithSourceInsertTest.length == 100)

    // Trying to insert existing rows should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
      spark
        .sql(s"""
           |select "foo" as description,
           |least(startTime, endTime) as startTime,
           |greatest(startTime, endTime) as endTime,
           |type,
           |subtype,
           |array(8031965690878131) as assetIds,
           |bigint(0) as id,
           |map("foo", null, "bar", "test") as metadata,
           |"$source" as source,
           |string(id) as externalId,
           |createdTime,
           |lastUpdatedTime
           |from sourceEvent
           |limit 100
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "events")
        .save()
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 409)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "allow partial updates in savemode" taggedAs WriteTest in {
    val source = "spark-events-test"

    // Cleanup old events
    cleanupEvents(source)
    val dfWithUpdatesAsSource = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'").collect,
      df => df.length > 0)
    assert(dfWithUpdatesAsSource.length == 0)

    // Insert some test data
    spark
      .sql(s"""
             |select "foo" as description,
             |least(startTime, endTime) as startTime,
             |greatest(startTime, endTime) as endTime,
             |type,
             |subtype,
             |null as assetIds,
             |bigint(0) as id,
             |map("foo", null, "bar", "test") as metadata,
             |"$source" as source,
             |string(id) as externalId,
             |createdTime,
             |lastUpdatedTime
             |from sourceEvent
             |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if insert worked
    val descriptionsAfterInsert =
      retryWhile[Array[Row]](
        spark
          .sql(
            s"select description from destinationEvent " +
              s"where source = '$source' and description = 'foo'")
          .collect,
        df => df.length < 100)
    assert(descriptionsAfterInsert.length == 100)
    assert(descriptionsAfterInsert.map(_.getString(0)).forall(_ == "foo"))

    // Update the data
    spark
      .sql(s"""
         |select "bar" as description,
         |id
         |from destinationEvent
         |where source = '$source'
      """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "update")
      .save()

    // Check if update worked
    val descriptionsAfterUpdate =
      retryWhile[Array[Row]](
        spark
          .sql(
            s"select description from destinationEvent " +
              s"where source = '$source' and description = 'bar'")
          .collect,
        df => df.length < 100)
    assert(descriptionsAfterUpdate.length == 100)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

    // Trying to update non-existing ids should throw a 400 CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
      // Update the data
      spark
        .sql(s"""
         |select "bar" as description,
         |startTime,
         |endTime,
         |type,
         |subtype,
         |assetIds,
         |bigint(1) as id,
         |metadata,
         |source,
         |externalId,
         |null as createdTime,
         |lastUpdatedTime
         |from destinationEvent
         |where source = '$source'
         |limit 1
        """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "events")
        .option("onconflict", "update")
        .save()
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "check for null ids on event update" taggedAs WriteTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("limitPerPartition", "10")
      .load()

    df.createTempView("nullevents")
    val wdf = spark
      .sql(s"""
      |select "bar" as description,
      |startTime,
      |endTime,
      |type,
      |subtype,
      |assetIds,
      |null as id,
      |metadata,
      |source,
      |externalId,
      |null as createdTime,
      |lastUpdatedTime
      |from events
      |limit 1
    """.stripMargin)
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console

    val e = intercept[SparkException] {
      wdf.write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "events")
        .option("onconflict", "update")
        .save()
    }
    e.getCause shouldBe a[IllegalArgumentException]
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "allow deletes in savemode" taggedAs WriteTest in {
    val source = "spark-events-test"

    // Cleanup old events
    cleanupEvents(source)
    val dfWithDeletesAsSource = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'").collect,
      df => df.length > 0)
    assert(dfWithDeletesAsSource.length == 0)

    // Insert some test data
    spark
      .sql(s"""
              |select "foo" as description,
              |least(startTime, endTime) as startTime,
              |greatest(startTime, endTime) as endTime,
              |type,
              |subtype,
              |null as assetIds,
              |bigint(0) as id,
              |map("foo", null, "bar", "test") as metadata,
              |"$source" as source,
              |externalId,
              |0 as createdTime,
              |lastUpdatedTime
              |from sourceEvent
              |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if insert worked
    val idsAfterInsert =
      retryWhile[Array[Row]](
        spark
          .sql(s"select id from destinationEvent where source = '$source'")
          .collect,
        df => df.length < 100)
    assert(idsAfterInsert.length == 100)

    // Delete the data
    spark
      .sql(s"""
              |select id
              |from destinationEvent
              |where source = '$source'
      """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "delete")
      .save()

    // Check if delete worked
    val idsAfterDelete =
      retryWhile[Array[Row]](
        spark
          .sql(s"select id from destinationEvent where source = '$source'")
          .collect,
        df => df.length > 0)
    assert(idsAfterDelete.length == 0)
  }

  def eventDescriptions(source: String): Array[Row] =
    spark
      .sql(s"""select description from destinationEvent where source = "$source"""")
      .collect()

  def cleanupEvents(source: String): Unit = {
    spark.sql(s"""select * from destinationEvent where source = '$source'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "delete")
      .save()
  }
}
