package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.EventCreate
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

  /*
  Open Industrial Data will, every now and again, insert some more Events. To stop this from breaking our
  tests we apply an upper bound filter on createdTime for all Events
   */
  private def getBaseReader(
    collectMetrics: Boolean = false,
    metricsPrefix: String = "",
    upperTimeBound: String = "1580000000"): DataFrame = {

    spark.read
      .format("cognite.spark.v1")
      .option("type", "events")
      .option("apiKey", readApiKey)
      .option("collectMetrics", collectMetrics)
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"createdTime < to_timestamp($upperTimeBound)")
  }

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
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type = 'Workpackage'")

    assert(df.count == 20)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 20)
  }

  it should "get exception on invalid query" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filter.dataSetId"
    val df = getBaseReader(true, metricsPrefix)
      .where("dataSetId = 0")

    val thrown = the[SparkException] thrownBy df.count()
    thrown.getMessage should include ("id must be greater than or equal to 1")
  }

  it should "apply a dataSetId pushdown filter" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filter.dataSetId"
    val df = getBaseReader(true, metricsPrefix)
        .where("type = 'Workpackage' or dataSetId = 1")

    assert(df.count == 20)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 20)
  }

  it should "apply pushdown filters when non pushdown columns are ANDed" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.and.non.pushdown"
    // The contents of the parenthesis would need all content, but the left side should cancel that out
    val df = getBaseReader(true, metricsPrefix, "1574165300")
      .where(s"(type = 'RULE_BROKEN' or description = 'Rule test rule broken.') and type = 'RULE_BROKEN'")

    assert(df.count == 269)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 269)
  }

  it should "read all data when necessary" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.or.non.pushdown"
    val df = getBaseReader(true, metricsPrefix, "1574165300")
      .where(s"type = 'RULE_BROKEN' or description = 'Rule test rule broken.'")

    assert(df.count == 269)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead > 3000)
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs ReadTest in {
    val metricsPrefix = "single.pushdown.filter.duplicates"
    val df = getBaseReader(true, metricsPrefix, "1574165300")
      .where(s"type = 'RULE_BROKEN' or subtype = '-LLibBzAJWfs1aBXHgg3'")

    assert(df.count == 269)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 269)
  }

  it should "apply multiple pushdown filters" taggedAs ReadTest in {
    val metricsPrefix = "multiple.pushdown.filters"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type = 'Workpackage' and source = 'akerbp-cdp'")

    assert(df.count == 20)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 20)
  }

  it should "handle or conditions" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.or"
    val df = getBaseReader(true, metricsPrefix, "1574165300")
      .where(s"type = 'Workpackage' or type = 'RULE_BROKEN'")

    assert(df.count == 289)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 289)
  }

  it should "handle in() conditions" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.in"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type in('Workpackage','Worktask')")
    assert(df.count == 1147)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1147)
  }

  it should "handle and, or and in() in one query" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.and.or.in"
    val df = getBaseReader(true, metricsPrefix, "1574165300")
      .where(s"(type = 'RULE_BROKEN' or type = '***.***') and subtype in('-LLibBzAJWfs1aBXHgg3', '*** *** by *** *** ***', '*** *** by ***-ON Application')")

    assert(df.count == 22)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 22)
  }

  it should "handle pushdown filters on minimum startTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.minStartTime"
    val df = getBaseReader(true, metricsPrefix)
      .where("startTime > to_timestamp(1568105460)")
    assert(df.count >= 1346)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead >= 1346)
  }

  it should "handle pushdown filters on maximum createdTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.maxCreatedTime"
    val df = getBaseReader(true, metricsPrefix)
      .where("createdTime <= to_timestamp(1535448052)")
    assert(df.count == 6907)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6907)
  }

  it should "handle pushdown filters on maximum startTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.maxStartTime"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"startTime < to_timestamp(1533132293) and source = 'akerbp-cdp'")
    assert(df.count == 40004)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 40004)
  }

  it should "handle pushdown filters on minimum and maximum startTime" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.minMaxStartTime"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"startTime < to_timestamp(1533132293) and startTime > to_timestamp(1533000293)")
    assert(df.count == 396)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 396)
  }

  it should "handle pushdown filters on assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"assetIds In(Array(3047932288982463)) and startTime <= to_timestamp(1330239600)")
    assert(df.count == 12)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 12)
  }

  it should "handle pusdown filters on eventIds" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.id"
    val df = getBaseReader(true, metricsPrefix)
      .where("id = 370545839260513")
    assert(df.count == 1)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1)
  }

  it should "handle pusdown filters on many eventIds" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.ids"
    val df = getBaseReader(true, metricsPrefix)
      .where("id In(607444033860, 3965637099169, 10477877031034, 17515837146970, 19928788984614, 21850891340773)")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "handle pusdown filters on many eventIds with or" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.orids"
    val df = getBaseReader(true, metricsPrefix)
      .where("""
          id = 607444033860 or id = 3965637099169 or id = 10477877031034 or
          id = 17515837146970 or id = 19928788984614 or id = 21850891340773""")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "handle pusdown filters on many eventIds with other filters" taggedAs WriteTest ignore {
    val metricsPrefix = "pushdown.filters.idsAndDescription"
    val df = getBaseReader(true, metricsPrefix)
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
    val df = getBaseReader(true, metricsPrefix)
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
                 |0 as lastUpdatedTime,
                 |null as dataSetId
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
    val metricsPrefix = "upsert.event.metrics.insertInto"
    val source = "spark-events-test-upsert"

    // Cleanup events
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](cleanupEvents(source), rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    val destinationDf: DataFrame = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    destinationDf.createOrReplaceTempView("destinationEventUpsert")

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
                 |concat("$source", cast(id as string)) as externalId,
                 |createdTime,
                 |lastUpdatedTime,
                 |dataSetId
                 |from sourceEvent
                 |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEventUpsert")

    // Check if post worked
    val descriptionsAfterPost =
      retryWhile[Array[Row]](eventDescriptions(source), rows => rows.length < 100)
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "bar"))

    val eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreated == 100)

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
                 |concat("$source", cast(id as string)) as externalId,
                 |createdTime,
                 |lastUpdatedTime,
                 |dataSetId
                 |from sourceEvent
                 |limit 500
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEventUpsert")

    // Check if upsert worked
    val descriptionsAfterUpdate = retryWhile[Array[Row]](
      spark
        .sql(s"select description from destinationEventUpsert where source = '$source' and description = 'foo'").collect,
      df => df.length < 500
    )
    assert(descriptionsAfterUpdate.length == 500)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))

    val eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterUpsert == 500)

    val dfWithCorrectAssetIds = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where assetIds = array(2091657868296883) and source = '$source'").collect,
      rows => rows.length < 500)
    assert(dfWithCorrectAssetIds.length == 500)
  }

  it should "allow inserts in savemode" taggedAs WriteTest in {
    val source = "spark-events-test-insert-savemode"
    val metricsPrefix = "insert.event.metrics.savemode"

    // Cleanup events
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](cleanupEvents(source), rows => rows.nonEmpty)
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
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    val eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreated == 100)

    val dfWithSourceInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source'").collect,
      df => df.length < 100
    )
    assert(dfWithSourceInsertTest.length == 100)

    // Trying to insert existing rows should throw a CdpApiException
    disableSparkLogging() // Removing expected Spark executor Errors from the console
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
    val eventsCreatedAfterFailure = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterFailure == 100)
    enableSparkLogging()
  }

  it should "allow duplicated ids and external ids when using upsert in savemode" in {
    val source = s"spark-events-test-${shortRandomString()}"
    val metricsPrefix = "upsert.duplicate.event.metrics.save"

    // Cleanup events
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](cleanupEvents(source), rows => rows.nonEmpty)
    assert(eventDescriptionsReturned.isEmpty)

    val externalId = shortRandomString()

    // Post new events
    val eventsToCreate = spark
      .sql(s"""
              |select "$source" as source,
              |cast(from_unixtime(0) as timestamp) as startTime,
              |"$externalId" as externalId
              |union
              |select "$source" as source,
              |cast(from_unixtime(1) as timestamp) as startTime,
              |"$externalId" as externalId
     """.stripMargin)

    eventsToCreate
      .repartition(1)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "upsert")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    val eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreated == 1)

    a [NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "events")

    // We need to add endTime as well, otherwise Spark is clever enough to remove duplicates
    // on its own, it seems.
    val eventsToUpdate = spark
      .sql(s"""
              |select "$source" as source,
              |"bar" as description,
              |cast(from_unixtime(0) as timestamp) as endTime,
              |"$externalId" as externalId
              |union
              |select "$source" as source,
              |"bar" as description,
              |cast(from_unixtime(1) as timestamp) as endTime,
              |"$externalId" as externalId
     """.stripMargin)

    eventsToUpdate
      .repartition(1)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "upsert")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    val eventsCreatedAfterByExternalId = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterByExternalId == 1)

    val eventsUpdatedAfterByExternalId = getNumberOfRowsUpdated(metricsPrefix, "events")
    assert(eventsUpdatedAfterByExternalId == 1)

    val descriptionsAfterUpdate =
      retryWhile[Array[Row]](
        spark
          .sql(s"select description from destinationEvent where source = '$source' and description = 'bar'")
          .collect(),
        rows => rows.length < 1)
    assert(descriptionsAfterUpdate.length == 1)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

    val eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterUpsert == 1)

    val id = writeClient.events.retrieveByExternalId(externalId).id
    val eventsToUpdateById = spark
      .sql(s"""
              |select "$source" as source,
              |"bar2" as description,
              |cast(from_unixtime(0) as timestamp) as endTime,
              |${id.toString} as id
              |union
              |select "$source" as source,
              |"bar2" as description,
              |cast(from_unixtime(1) as timestamp) as endTime,
              |${id.toString} as id
     """.stripMargin)

    eventsToUpdateById
      .repartition(1)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "upsert")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    val eventsCreatedAfterById = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterById == 1)

    val eventsUpdatedAfterById = getNumberOfRowsUpdated(metricsPrefix, "events")
    assert(eventsUpdatedAfterById == 2)

    val descriptionsAfterUpdateById =
      retryWhile[Array[Row]](
        spark
          .sql(s"select description from destinationEvent where source = '$source' and description = 'bar2'")
          .collect(),
        rows => rows.length < 1)
    assert(descriptionsAfterUpdateById.length == 1)
  }

  it should "allow upsert in savemode" taggedAs WriteTest in {
    val source = s"spark-events-test-${shortRandomString()}"
    val metricsPrefix = "upsert.event.metrics.save"

    // Cleanup events
    val eventDescriptionsReturned =
      retryWhile[Array[Row]](cleanupEvents(source), rows => rows.nonEmpty)
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
              |concat("$source", string(id)) as externalId,
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
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    val eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreated == 100)

    // Check if post worked
    val descriptionsAfterPost = retryWhile[Array[Row]](
      (for (_ <- 1 to 5)
        yield eventDescriptions(source)).minBy(_.length),
      rows => rows.length < 100)
    assert(descriptionsAfterPost.length == 100)
    assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "foo"))

    // Update events
    val updateEventsByIdDf = spark
      .sql(s"""
              |select "bar" as description,
              |startTime,
              |endTime,
              |type,
              |subtype,
              |array(2091657868296883) as assetIds,
              |id,
              |metadata,
              |"$source" as source,
              |null as externalId,
              |createdTime,
              |lastUpdatedTime
              |from destinationEvent
              |where source = '$source'
              |order by id
              |limit 50
        """.stripMargin)
    updateEventsByIdDf
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("onconflict", "upsert")
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    val eventsUpdated = getNumberOfRowsUpdated(metricsPrefix, "events")
    assert(eventsUpdated == 50)

    val maxByIdUpdateId = updateEventsByIdDf.agg("id" -> "max").collect().head.getLong(0)

    val updateEventsByExternalIdDf = spark
      .sql(s"""
              |select "bar" as description,
              |startTime,
              |endTime,
              |type,
              |subtype,
              |array(2091657868296883) as assetIds,
              |null as id,
              |map("some", null, "metadata", "test") as metadata,
              |"$source" as source,
              |externalId,
              |createdTime,
              |lastUpdatedTime
              |from destinationEvent
              |where id > $maxByIdUpdateId and source = '$source'
              |order by id
              |limit 50
        """.stripMargin)
    updateEventsByExternalIdDf
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("onconflict", "upsert")
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()
    val eventsUpdatedAfterByExternalId = getNumberOfRowsUpdated(metricsPrefix, "events")
    assert(eventsUpdatedAfterByExternalId == 100)

    // Check if upsert worked
    val descriptionsAfterUpdate =
      retryWhile[Array[Row]](
        spark
          .sql(s"select description from destinationEvent where source = '$source' and description = 'bar'")
          .collect()
        , rows => rows.length < 100)
    assert(descriptionsAfterUpdate.length == 100)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

    val eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterUpsert == 100)

    val dfWithCorrectAssetIds = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where assetIds = array(2091657868296883) and source = '$source'").collect,
      rows => rows.length != 100)
    assert(dfWithCorrectAssetIds.length == 100)

  }

  it should "allow partial updates in savemode" taggedAs WriteTest in {
    val source = "spark-events-test-upsert-savemode"
    val metricsPrefix = "events.test.upsert.partial"

    // Cleanup old events
    val dfWithUpdatesAsSource = retryWhile[Array[Row]](
      cleanupEvents(source), df => df.length > 0)
    assert(dfWithUpdatesAsSource.length == 0)

    val destinationDf: DataFrame = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    destinationDf.createOrReplaceTempView("destinationEventsUpsertPartial")

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
             |concat("$source", string(id)) as externalId,
             |createdTime,
             |lastUpdatedTime,
             |dataSetId
             |from sourceEvent
             |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEventsUpsertPartial")

    // Check if insert worked
    val descriptionsAfterInsert =
      retryWhile[Array[Row]](
        spark
          .sql(
            s"select description from destinationEventsUpsertPartial " +
              s"where source = '$source' and description = 'foo'")
          .collect,
        df => df.length < 100)
    assert(descriptionsAfterInsert.length == 100)
    assert(descriptionsAfterInsert.map(_.getString(0)).forall(_ == "foo"))

    val eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterUpsert == 100)

    // Update the data
    spark
      .sql(s"""
         |select "bar" as description,
         |id
         |from destinationEventsUpsertPartial
         |where source = '$source'
      """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "update")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    // Check if update worked
    val descriptionsAfterUpdate =
      retryWhile[Array[Row]](
        spark
          .sql(
            s"select description from destinationEventsUpsertPartial " +
              s"where source = '$source' and description = 'bar'")
          .collect,
        df => df.length < 100)
    assert(descriptionsAfterUpdate.length == 100)
    assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

    val eventsUpdatedAfterUpsert = getNumberOfRowsUpdated(metricsPrefix, "events")
    assert(eventsUpdatedAfterUpsert == 100)

    // Trying to update non-existing ids should throw a 400 CdpApiException
    disableSparkLogging() // Removing expected Spark executor Errors from the console
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
         |lastUpdatedTime,
         |dataSetId
         |from destinationEventsUpsertPartial
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
    val eventsCreatedAfterFail = getNumberOfRowsCreated(metricsPrefix, "events")
    assert(eventsCreatedAfterFail == 100)
    val eventsUpdatedAfterFail = getNumberOfRowsUpdated(metricsPrefix, "events")
    assert(eventsUpdatedAfterFail == 100)
    enableSparkLogging()
  }

  it should "allow null ids on event update" taggedAs WriteTest in {
    val source = "null-id-events"
    // Cleanup old events
    retryWhile[Array[Row]](cleanupEvents(source), rows => rows.length > 0)

    // Post new events
    spark
      .sql(
        s"""
           |select concat("$source", string(id)) as externalId,
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
           |null as createdTime,
           |$testDataSetId as dataSetId
           |from sourceEvent
           |limit 5
       """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationEvent")

    // Check if post worked
    val eventsFromTestDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationEvent where source = '$source' and description = 'foo' and dataSetId = $testDataSetId").collect,
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
    val dfWithDeletesAsSource = retryWhile[Array[Row]](cleanupEvents(source), df => df.length > 0)
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
              |concat("$source", externalId) as externalId,
              |0 as createdTime,
              |lastUpdatedTime,
              |dataSetId
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

    disableSparkLogging() // Removing expected Spark executor Errors from the console
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
    enableSparkLogging()

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
    val dfWithDeletesAsSource = retryWhile[Array[Row]](cleanupEvents(source), df => df.length > 0)
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
              |concat("$source", string(id)) as externalId,
              |0 as createdTime,
              |lastUpdatedTime,
              |dataSetId
              |from sourceEvent
              |limit 1
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
    disableSparkLogging() // Removing expected Spark executor Errors from the console
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
    enableSparkLogging()
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

  def cleanupEvents(source: String): Array[Row] = {
    spark.sql(s"""select * from destinationEvent where source = '$source'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .option("onconflict", "delete")
      .save()
    spark
      .sql(s"""select description from destinationEvent where source = "$source"""")
      .collect()
  }

}
