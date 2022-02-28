package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}
import org.scalatest.prop.TableDrivenPropertyChecks.forAll

import scala.util.control.NonFatal

class EventsRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "events")
    .load()
  destinationDf.createOrReplaceTempView("destinationEvent")

  val sourceDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "events")
    .option("limitPerPartition", "1000")
    .option("partitions", "1")
    .load()
  sourceDf.createOrReplaceTempView("sourceEvent")
  sourceDf.cache()

  private def getBaseReader(
      collectMetrics: Boolean = false,
      metricsPrefix: String = "",
      upperTimeBound: String = "1580000000"): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", "events")
      .option("apiKey", writeApiKey)
      .option("collectMetrics", collectMetrics)
      .option("metricsPrefix", metricsPrefix)
      .load()

  "EventsRelation" should "allow simple reads" taggedAs WriteTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "events")
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
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type = 'Worktask'")

    assert(df.count == 227)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 227)
  }

  it should "get exception on invalid query" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filter.dataSetId"
    val df = getBaseReader(true, metricsPrefix)
      .where("dataSetId = 0")

    val thrown = the[SparkException] thrownBy df.count()
    thrown.getMessage should include("id must be greater than or equal to 1")
  }

  it should "apply a dataSetId pushdown filter" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filter.dataSetId"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        "type = 'Worktask' or dataSetId = 86163806167772 and createdTime < timestamp('2020-03-31 00:00:00.000Z')")

    assert(df.count == 232)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 232)
  }

  it should "not fetch all items if filter on id" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filter.id"
    val df = getBaseReader(true, metricsPrefix)
      .where("id = 1394439528453086")

    assert(df.count == 1)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1)

  }

  it should "not fetch all items if filter on externalId" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filter.externalId"
    val df = getBaseReader(true, metricsPrefix)
      .where("dataSetId = 86163806167772 or externalId = 'null-id-events65847147385304'")

    assert(df.count == 18)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 18)
  }

  it should "apply pushdown filters when non pushdown columns are ANDed" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.and.non.pushdown"
    // The contents of the parenthesis would need all content, but the left side should cancel that out
    val df = getBaseReader(true, metricsPrefix)
      .where(s"(type = 'Worktask' or description = 'Rule test rule broken.') and type = 'Worktask'")

    assert(df.count == 227)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 227)
  }

  it should "read all data when necessary" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.or.non.pushdown"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type = 'Worktask' or description = 'Rule test rule broken.'")

    assert(df.count == 237)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead > 25000)
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs WriteTest in {
    val metricsPrefix = "single.pushdown.filter.duplicates"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type = 'NEWS' or subtype = 'HACK'")

    val fetchedItems = df.collect()
    // check that there are actually some items that satisfy both filters
    assert(fetchedItems.exists(r =>
      r.getAs[String]("type") == "NEWS" && r.getAs[String]("subtype") == "HACK"))
    assert(fetchedItems.map(_.getAs[Long]("id")).distinct.length == fetchedItems.length)

    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == fetchedItems.length)
  }

  it should "apply multiple pushdown filters" taggedAs WriteTest in {
    val metricsPrefix = "multiple.pushdown.filters"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        s"type = '***' and assetIds = Array(2091657868296883) and createdTime < timestamp('2020-01-01 00:00:00.000Z')")

    assert(df.count == 83)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 83)
  }

  it should "handle or conditions" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.or"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        s"(type = 'Workitem' or type = 'Worktask') and createdTime < timestamp('2020-05-10 00:00:00.000Z')")

    assert(df.count == 1483)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1483)
  }

  it should "handle in() conditions" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.in"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type in('Workitem','Worktask') and createdTime < timestamp('2020-05-10 00:00:00.000Z')")
    assert(df.count == 1483)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1483)
  }

  it should "handle and, or and in() in one query" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.and.or.in"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        s"(type = 'Workitem' or type = '***') and assetIds in(array(2091657868296883), array(8031965690878131)) and createdTime < timestamp('2020-01-01 00:00:00.000Z')")

    assert(df.count == 172)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 172)
  }

  it should "handle pushdown filters on minimum startTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.minStartTime"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        "startTime > to_timestamp(1568105460) and createdTime < timestamp('2020-05-10 00:00:00.000Z')")
    assert(df.count >= 179)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead >= 179)
  }

  it should "handle pushdown filters on maximum createdTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.maxCreatedTime"
    val df = getBaseReader(true, metricsPrefix)
      .where("createdTime <= timestamp('2018-10-26 00:00:00.000Z')")
    assert(df.count == 1029)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1029)
  }

  it should "handle pushdown filters on maximum startTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.maxStartTime"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        s"startTime < timestamp('1970-01-19 00:00:00.000Z') and createdTime < timestamp('2020-01-01 00:00:00.000Z')")
    assert(df.count > 10)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == df.count)
  }

  it should "handle pushdown filters on minimum and maximum startTime" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.minMaxStartTime"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        s"startTime < timestamp('1970-01-19 00:00:00.000Z') and startTime > timestamp('1970-01-18 23:00:00.000Z') and createdTime < timestamp('2020-01-01 00:00:00.000Z')")
    assert(df.count == 9)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 9)
  }

  it should "handle pushdown filters on assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        s"assetIds In(Array(2091657868296883)) and createdTime < timestamp('2020-01-01 00:00:00.000Z')")
    assert(df.count == 89)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 89)
  }

  (it should "handle pusdown filters on eventIds" taggedAs WriteTest).ignore {
    val metricsPrefix = "pushdown.filters.id"
    val df = getBaseReader(true, metricsPrefix)
      .where("id = 370545839260513")
    assert(df.count == 1)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 1)
  }

  (it should "handle pusdown filters on many eventIds" taggedAs WriteTest).ignore {
    val metricsPrefix = "pushdown.filters.ids"
    val df = getBaseReader(true, metricsPrefix)
      .where(
        "id In(607444033860, 3965637099169, 10477877031034, 17515837146970, 19928788984614, 21850891340773)")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  (it should "handle pusdown filters on many eventIds with or" taggedAs WriteTest).ignore {
    val metricsPrefix = "pushdown.filters.orids"
    val df = getBaseReader(true, metricsPrefix)
      .where("""
          id = 607444033860 or id = 3965637099169 or id = 10477877031034 or
          id = 17515837146970 or id = 19928788984614 or id = 21850891340773""")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  (it should "handle pusdown filters on many eventIds with other filters" taggedAs WriteTest).ignore {
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

  (it should "handle a really advanced query" taggedAs WriteTest).ignore {
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

  (it should "handle pushdown on eventId or something else" taggedAs WriteTest).ignore {
    val metricsPrefix = "pushdown.filters.idortype"
    val df = getBaseReader(true, metricsPrefix)
      .where(s"type = 'maintenance' or id = 17515837146970")
    assert(df.count == 6)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
    assert(eventsRead == 6)
  }

  it should "allow null values for all event fields except id" taggedAs WriteTest in {
    val source = s"spark-events-test-null-${shortRandomString()}"
    try {
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
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support upserts" taggedAs WriteTest in {
    val metricsPrefix = s"upsert.event.insertInto.${shortRandomString()}"
    val source = "spark-events-test-upsert" + shortRandomString()

    try {
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
                |null as type,
                |null as subtype,
                |null as assetIds,
                |bigint(0) as id,
                |map("foo", null, "bar", "test") as metadata,
                |"$source" as source,
                |concat("$source", cast(id as string)) as externalId,
                |createdTime,
                |lastUpdatedTime,
                |null as dataSetId
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
                |null as id,
                |map("some", null, "metadata", "test") as metadata,
                |"$source" as source,
                |concat("$source", cast(id as string)) as externalId,
                |createdTime,
                |lastUpdatedTime,
                |null as dataSetId
                |from sourceEvent
                |limit 500
     """.stripMargin)
        .select(destinationDf.columns.map(col): _*)
        .write
        .insertInto("destinationEventUpsert")

      val eventsCreated2 = getNumberOfRowsCreated(metricsPrefix, "events")
      assert(eventsCreated2 == 500)
      val eventsUpdated = getNumberOfRowsUpdated(metricsPrefix, "events")
      assert(eventsUpdated == 100)

      // Check if upsert worked
      val descriptionsAfterUpdate = retryWhile[Array[Row]](
        spark
          .sql(
            s"select description from destinationEventUpsert where source = '$source' and description = 'foo'")
          .collect,
        df => df.length < 500
      )
      assert(descriptionsAfterUpdate.length == 500)
      assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))

      val eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
      assert(eventsCreatedAfterUpsert == 500)

      val dfWithCorrectAssetIds = retryWhile[Array[Row]](
        spark
          .sql(
            s"select * from destinationEvent where assetIds = array(2091657868296883) and source = '$source'")
          .collect,
        rows => rows.length < 500)
      assert(dfWithCorrectAssetIds.length == 500)
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "allow inserts in savemode" taggedAs WriteTest in {
    val source = s"spark-events-insert-savemode-${shortRandomString()}"
    val metricsPrefix = s"insert.event.savemode.${shortRandomString()}"

    try {
      // Test inserts
      val df = spark
        .sql(s"""
                |select "foo" as description,
                |least(startTime, endTime) as startTime,
                |greatest(startTime, endTime) as endTime,
                |null as type,
                |null as subtype,
                |array(8031965690878131) as assetIds,
                |null as id,
                |map("foo", null, "bar", "test") as metadata,
                |"$source" as source,
                |concat("$source", string(id)) as externalId,
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
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "allow NULL updates in savemode" taggedAs WriteTest in forAll(updateAndUpsert) {
    updateMode =>
      val source = s"spark-events-update-null-${shortRandomString()}"
      val metricsPrefix = s"updatenull.event.${shortRandomString()}"

      try {
        // Insert test event
        val df = spark
          .sql(s"""
                |select "foo" as description,
                |cast(from_unixtime(10) as timestamp) as startTime,
                |cast(from_unixtime(20) as timestamp) as endTime,
                |"test-type" as type,
                |"test-type" as subtype,
                |array(8031965690878131) as assetIds,
                |"$source" as source,
                |"$source-id" as externalId
     """.stripMargin)

        df.write
          .format("cognite.spark.v1")
          .option("apiKey", writeApiKey)
          .option("type", "events")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .option("onconflict", "abort")
          .save()

        val insertTest = retryWhile[Array[Row]](
          spark.sql(s"select * from destinationEvent where source = '$source'").collect,
          df => df.length == 0
        )

        spark
          .sql(s"""
                |select "foo-$source" as description,
                |NULL as endTime,
                |NULL as subtype,
                |"$source" as source,
                |${insertTest.head.getAs[Long]("id")} as id,
                |NULL as externalId
     """.stripMargin)
          .write
          .format("cognite.spark.v1")
          .option("apiKey", writeApiKey)
          .option("type", "events")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .option("ignoreNullFields", "false")
          .option("onconflict", updateMode)
          .save()

        val updateTest = retryWhile[Array[Row]](
          spark
            .sql(
              s"select * from destinationEvent where source = '$source' and description = 'foo-$source'")
            .collect,
          df => df.length == 0
        )
        updateTest.length shouldBe 1
        val Array(updatedRow) = updateTest
        val updatedEvent = SparkSchemaHelper.fromRow[EventsReadSchema](updatedRow)
        updatedEvent.endTime shouldBe None
        updatedEvent.startTime shouldBe defined
        updatedEvent.source shouldBe Some(source)
        updatedEvent.externalId shouldBe None
        updatedEvent.assetIds shouldBe Some(Seq(8031965690878131L))
        updatedEvent.`type` shouldBe Some("test-type")
        updatedEvent.subtype shouldBe None

      } finally {
        try {
          cleanupEvents(source)
        } catch {
          case NonFatal(_) => // ignore
        }
      }
  }

  it should "allow duplicated ids and external ids when using upsert in savemode" in {
    val source = s"spark-events-test-${shortRandomString()}"
    val metricsPrefix = s"upsert.duplicate.event.save.${shortRandomString()}"

    try {
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

      a[NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "events")

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
            .sql(
              s"select description from destinationEvent where source = '$source' and description = 'bar'")
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
            .sql(
              s"select description from destinationEvent where source = '$source' and description = 'bar2'")
            .collect(),
          rows => rows.length < 1)
      assert(descriptionsAfterUpdateById.length == 1)
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "allow upsert in savemode" taggedAs WriteTest in {
    val source = s"spark-events-test-${shortRandomString()}"
    val metricsPrefix = s"upsert.event.save.${shortRandomString()}"

    try {
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
      updateEventsByIdDf.write
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
      updateEventsByExternalIdDf.write
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
            .sql(
              s"select description from destinationEvent where source = '$source' and description = 'bar'")
            .collect(),
          rows => rows.length < 100)
      assert(descriptionsAfterUpdate.length == 100)
      assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

      val eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
      assert(eventsCreatedAfterUpsert == 100)

      val dfWithCorrectAssetIds = retryWhile[Array[Row]](
        spark
          .sql(
            s"select * from destinationEvent where assetIds = array(2091657868296883) and source = '$source'")
          .collect,
        rows => rows.length != 100)
      assert(dfWithCorrectAssetIds.length == 100)
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "allow partial updates in savemode" taggedAs WriteTest in {
    val source = s"spark-events-upsert-save-${shortRandomString()}"
    val metricsPrefix = s"events.upsert.partial.${shortRandomString()}"

    try {
      // Cleanup old events
      val dfWithUpdatesAsSource = retryWhile[Array[Row]](cleanupEvents(source), df => df.length > 0)
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
                |null as dataSetId
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
      val descriptionsAfterUpdate =
        retryWhile[Array[Row]](
          {
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
            spark
              .sql(
                s"select description from destinationEventsUpsertPartial " +
                  s"where source = '$source' and description = 'bar'")
              .collect
          },
          df => df.length < 100
        )
      assert(descriptionsAfterUpdate.length == 100)
      assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

      val eventsUpdatedAfterUpsert = getNumberOfRowsUpdated(metricsPrefix, "events")
      // Due to retries, this may exceed 100
      assert(eventsUpdatedAfterUpsert >= 100)

      // Trying to update non-existing ids should throw a 400 CdpApiException
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
                  |"not-existing-id" as externalId,
                  |null as createdTime,
                  |lastUpdatedTime,
                  |null as dataSetId
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
      assert(eventsCreatedAfterFail == eventsCreatedAfterUpsert)
      val eventsUpdatedAfterFail = getNumberOfRowsUpdated(metricsPrefix, "events")
      assert(eventsUpdatedAfterFail == eventsUpdatedAfterUpsert)
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "allow null ids on event update" taggedAs WriteTest in {
    val source = s"null-id-events-${shortRandomString()}"

    try {
      // Post new events
      spark
        .sql(s"""
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
        spark
          .sql(
            s"select * from destinationEvent where source = '$source' and description = 'foo' and dataSetId = $testDataSetId")
          .collect,
        df => df.length < 5)
      assert(eventsFromTestDf.length == 5)

      // Upsert events
      val descriptionsAfterUpdate = retryWhile[Array[Row]](
        {
          spark
            .sql(s"""
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
          spark
            .sql(
              s"select description from destinationEvent where source = '$source' and description = 'bar'")
            .collect
        },
        df => df.length < 5
      )
      assert(descriptionsAfterUpdate.length == 5)
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }
  it should "fail reasonably when assetIds is a string" taggedAs WriteTest in {
    val error = sparkIntercept {
      // Post new events
      spark
        .sql(s"""
                |select "foo" as description,
                |least(startTime, endTime) as startTime,
                |greatest(startTime, endTime) as endTime,
                |array('test-what-happens-with-a-string') as assetIds,
                |map("foo", null, "bar", "test") as metadata,
                |$testDataSetId as datasetId
                |from sourceEvent
                |limit 1
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "events")
        .save()

    }
    assert(error.getMessage.contains("Column 'assetIds' was expected to have type Seq[Long], but"))
  }


  it should "allow deletes in savemode" taggedAs WriteTest in {
    val source = s"spark-events-delete-save-${shortRandomString()}"

    try {
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
                |null as dataSetId
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
      val idsAfterDelete =
        retryWhile[Array[Row]](
          {
            spark
              .sql(s"select id from destinationEvent where source = '$source'")
              .write
              .format("cognite.spark.v1")
              .option("apiKey", writeApiKey)
              .option("type", "events")
              .option("onconflict", "delete")
              .save()
            spark
              .sql(s"select id from destinationEvent where source = '$source'")
              .collect
          },
          df => df.length > 0
        )
      assert(idsAfterDelete.length == 0)
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support ignoring unknown ids in deletes" in {
    val source = s"spark-ignore-unknown-id-${shortRandomString()}"

    try {
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
                |null as dataSetId
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
        .sql("select 123 as id".stripMargin)
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
          .sql("select 123 as id")
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
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support deletes by externalIds" in {
    val source = s"spark-externalIds-delete-save-${shortRandomString()}"
    val randomSuffix = shortRandomString()

    try {
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
                |concat(string(id), '${randomSuffix}') as externalId,
                |0 as createdTime,
                |lastUpdatedTime,
                |null as dataSetId
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
            .sql(s"select externalId from destinationEvent where source = '$source'")
            .collect,
          df => df.length < 100)
      assert(idsAfterInsert.length == 100)

      val metricsPrefix = s"events.delete.${shortRandomString()}"
      // Delete the data
      val idsAfterDelete =
        retryWhile[Array[Row]](
          {
            spark
              .sql(s"select externalId from destinationEvent where source = '$source'")
              .write
              .format("cognite.spark.v1")
              .option("apiKey", writeApiKey)
              .option("type", "events")
              .option("onconflict", "delete")
              .option("collectMetrics", "true")
              .option("metricsPrefix", metricsPrefix)
              .save()
            spark
              .sql(s"select externalId from destinationEvent where source = '$source'")
              .collect
          },
          df => df.length > 0
        )
      assert(idsAfterDelete.isEmpty)
      getNumberOfRowsDeleted(metricsPrefix, "events") should be >= 100L
    } finally {
      try {
        cleanupEvents(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
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
    spark
      .sql(s"""select * from destinationEvent where source = '$source'""")
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
