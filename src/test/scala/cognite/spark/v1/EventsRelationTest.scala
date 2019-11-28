package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
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

  spark.sql(s"""select * from destinationEvent where source like 'spark-events-%'""")
    .write
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "events")
    .option("onconflict", "delete")
    .save()

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

  it should "apply a single pushdown filter" taggedAs ReadTest in {
    val metricsPrefix = "single.pushdown.filter"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'Workpackage'")
    assert(df.count == 20)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 20)
  }

  it should "apply pushdown filters when non pushdown columns are ANDed" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.and.non.pushdown"
    // The contents of the parenthesis would need all content, but the left side should cancel that out
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"(type = 'RULE_BROKEN' or description = 'Rule test rule broken.') and type = 'RULE_BROKEN'")
      .where("createdTime < to_timestamp(1574165300)")
    assert(df.count == 269)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 269)
  }

  it should "read all data when necessary" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.or.non.pushdown"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "500")
      .load()
      .where(s"type = 'RULE_BROKEN' or description = 'Rule test rule broken.'")
      .where("createdTime < to_timestamp(1574165300)")
    assert(df.count == 269)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead > 3000)
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs ReadTest in {
    val metricsPrefix = "single.pushdown.filter.duplicates"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "200")
      .load()
      .where(s"type = 'RULE_BROKEN' or subtype = '-LLibBzAJWfs1aBXHgg3'")
      .where("createdTime < to_timestamp(1574165300)")
    assert(df.count == 269)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 269)
  }

  it should "apply multiple pushdown filters" taggedAs ReadTest in {
    val metricsPrefix = "multiple.pushdown.filters"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'Workpackage' and source = 'akerbp-cdp'")
    assert(df.count == 20)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 20)
  }

  it should "handle or conditions" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.or"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type = 'Workpackage' or type = 'RULE_BROKEN'")
      .where("createdTime < to_timestamp(1574165300)")
    assert(df.count == 289)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 289)
  }

  it should "handle in() conditions" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.in"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"type in('Workpackage','Worktask')")
    assert(df.count == 1147)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1147)
  }

  it should "handle and, or and in() in one query" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.and.or.in"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"(type = 'RULE_BROKEN' or type = '***.***') and subtype in('-LLibBzAJWfs1aBXHgg3', '*** *** by *** *** ***', '*** *** by ***-ON Application')")
      .where("createdTime < to_timestamp(1574165300)")
    assert(df.count == 22)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 22)
  }

  it should "handle pushdown filters on minimum startTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.minStartTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("startTime > to_timestamp(1568105460)")
    assert(df.count >= 1346)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead >= 1346)
  }

  it should "handle pushdown filters on maximum createdTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.maxCreatedTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where("createdTime <= to_timestamp(1535448052)")
    assert(df.count == 6907)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6907)
  }

  it should "handle pushdown filters on maximum startTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.maxStartTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"startTime < to_timestamp(1533132293) and source = 'akerbp-cdp'")
    assert(df.count == 40004)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 40004)
  }

  it should "handle pushdown filters on minimum and maximum startTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.minMaxStartTime"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"startTime < to_timestamp(1533132293) and startTime > to_timestamp(1533000293)")
    assert(df.count == 396)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 396)
  }

  it should "handle pushdown filters on assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"assetIds In(Array(3047932288982463)) and startTime <= to_timestamp(1330239600)")
    assert(df.count == 12)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 12)
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
    val source = "spark-events-test-null"
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
    val source = "spark-events-test-upsert"

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
    val source = "spark-events-test-insert-savemode"

    // Cleanup events
    cleanupEvents(source)
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    // Test inserts
    val df = spark
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
                 |string(id+1) as externalId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceEvent
                 |limit 100
     """.stripMargin)

      df.write
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
        df.write
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

  it should "allow upsert in savemode" taggedAs WriteTest in {
    val source = "spark-events-test"

    // Cleanup events
    cleanupEvents(source)
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    // Post new events
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
              |null as createdTime,
              |lastUpdatedTime
              |from sourceEvent
              |limit 100
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "upsert")
      .save()

    // Check if post worked
    val descriptionsAfterPost =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.length != 100)
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "foo"))

    // Update events
    spark
      .sql(s"""
              |select "bar" as description,
              |startTime,
              |endTime,
              |type,
              |subtype,
              |array(2091657868296883) as assetIds,
              |bigint(0) as id,
              |map("some", null, "metadata", "test") as metadata,
              |"$source" as source,
              |string(id) as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceEvent
              |limit 500
        """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("onconflict", "upsert")
      .option("type", "events")
      .save()

    // Check if upsert worked
    val descriptionsAfterUpdate =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.length != 500)
    assert(descriptionsAfterUpdate.length == 500)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

    val dfWithCorrectAssetIds = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where assetIds = array(2091657868296883) and source = '$source'").collect,
      rows => rows.length != 500)
    assert(dfWithCorrectAssetIds.length == 500)

  }

  it should "allow partial updates in savemode" taggedAs WriteTest in {
    val source = "spark-events-test-upsert-savemode"

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

  it should "allow null ids on event update" taggedAs WriteTest in {
    val source = "null-id-events"
    // Cleanup old events
    cleanupEvents(source)
    retryWhile[Array[Row]](
      spark.sql(s"select * from sourceEvent where source = '$source'").collect,
      rows => rows.length > 0
    )

    // Post new events
    spark
      .sql(
        s"""
           |select string(id) as externalId,
           |null as id,
           |'$source' as source,
           |startTime,
           |endTime,
           |type,
           |subtype,
           |'foo' as description,
           |map() as metadata,
           |null as assetIds,
           |null as lastUpdatedTime,
           |null as createdTime
           |from sourceEvent
           |limit 5
       """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if post worked
    val eventsFromTestDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source' and description = 'foo'").collect,
      df => df.length < 5)
    assert(eventsFromTestDf.length == 5)

    // Upsert events
    spark
      .sql(
        s"""
           |select externalId,
           |'bar' as description
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
    val descriptionsAfterUpdate = retryWhile[Array[Row]](
      spark
        .sql(s"select description from destinationEvent where source = '$source' and description = 'bar'").collect,
      df => df.length < 5
    )
    assert(descriptionsAfterUpdate.length == 5)
  }

  it should "allow deletes in savemode" taggedAs WriteTest in {
    val source = "spark-events-test-delete-savemode"

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

  it should "support ignoring unknown ids in deletes" in {
    val source = "ignore-unknown-id-test"
    cleanupEvents(source)
    val dfWithDeletesAsSource = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'").collect,
      df => df.length > 0)
    assert(dfWithDeletesAsSource.length == 0)

    // Insert some test data
    spark
      .sql(s"""
              select "foo" as description,
              least(startTime, endTime) as startTime,
              greatest(startTime, endTime) as endTime,
              type,
              subtype,
              null as assetIds,
              bigint(0) as id,
              map("foo", null, "bar", "test") as metadata,
              "$source" as source,
              externalId,
              0 as createdTime,
              lastUpdatedTime
              from sourceEvent
              limit 1
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
        df => df.length < 1)
    assert(idsAfterInsert.length == 1)

    spark
      .sql(
        s"""
           |select 1574865177148 as id
           |from destinationEvent
           |where source = '$source'
        """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "delete")
      .option("ignoreUnknownIds", "true")
      .save()

    // Should throw error if ignoreUnknownIds is false
    val e = intercept[SparkException] {
      spark
        .sql(
          s"""
             |select 1574865177148 as id
             |from destinationEvent
             |where source = '$source'
        """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "events")
        .option("onconflict", "delete")
        .option("ignoreUnknownIds", "false")
        .save()
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val eventInsert = EventsInsertSchema()
    eventInsert.transformInto[EventsReadSchema]

    val eventUpsert = EventsUpsertSchema()
    eventUpsert.into[EventsReadSchema].withFieldComputed(_.id, eu => eu.id.getOrElse(0L))
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
