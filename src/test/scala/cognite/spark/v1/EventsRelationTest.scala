package cognite.spark.v1

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cognite.spark.compiletime.macros.SparkSchemaHelper
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, DataSet, DataSetCreate, Event, EventCreate}
import io.scalaland.chimney.dsl._
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.{Assertion, FlatSpec, Matchers, ParallelTestExecution}

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID

class EventsRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  val sourceView = s"sourceEvent_${shortRandomString()}"
  val testSource = s"EventsRelationTest-${shortRandomString()}"

  val sourceDf: DataFrame = spark.read
    .format(DefaultSource.sparkFormatString)
    .useOIDCWrite
    .option("type", "events")
    .option("limitPerPartition", "1000")
    .option("fetchSize", "100")
    .option("partitions", "1")
    .load()
  sourceDf.createOrReplaceTempView(sourceView)
  sourceDf.cache()

  private def getBaseReader(metricsPrefix: String): DataFrame =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", "events")
      .useOIDCWrite
      .option("collectMetrics", true)
      .option("metricsPrefix", metricsPrefix)
      .option("fetchSize", "100")
      .load()

  private def makeEvents(toCreate: Seq[EventCreate]): Resource[IO, Seq[Event]] = {
    assert(toCreate.forall(_.source.isEmpty), "event.source is reserved for test setup")
    Resource
      .make {
        writeClient.events.create(toCreate.map(e => e.copy(source = Some(testSource))))
      } { createdEvents =>
        writeClient.events.deleteByIds(createdEvents.map(_.id))
      }
      .evalTap(createdEvents =>
        IO.delay({
          assert(createdEvents.length == toCreate.length, "all events should be created")
          assert(createdEvents.forall(_.source.contains(testSource)), "all created events have source")
        }))
  }

  private def makeDataset(): Resource[IO, DataSet] =
    Resource
      .make {
        writeClient.dataSets.create(
          Seq(
            DataSetCreate(
              description = Some("cdp-spark-connector EventsRelationTest"),
              writeProtected = false,
              name = Some("for " +
                "tests"))))
      } { _ =>
        IO.unit
      }
      .map(_.head)

  private def makeDatasetId(): Resource[IO, Long] =
    makeDataset().map(_.id)

  private def makeAssets(toCreate: Seq[AssetCreate]): Resource[IO, Seq[Asset]] = {
    assert(toCreate.forall(_.source.isEmpty), "asset.source is reserved for test setup")
    Resource
      .make {
        writeClient.assets.create(toCreate.map(e => e.copy(source = Some(testSource))))
      } { createdEvents =>
        writeClient.assets.deleteByIds(createdEvents.map(_.id))
      }
      .evalTap(createdAssets =>
        IO.delay({
          assert(createdAssets.length == toCreate.length, "all assets should be created")
          assert(createdAssets.forall(_.source.contains(testSource)), "all created assets have source")
        }))
  }

  private def makeAssetIds(toCreate: Seq[AssetCreate]): Resource[IO, Seq[Long]] =
    makeAssets(toCreate).map(_.map(_.id))

  private def readEvents(selector: String => Dataset[Row]): IO[(String, Array[Row])] =
    for {
      metricsPrefix <- IO.delay { s"test.${shortRandomString()}" }
      df <- IO.delay { selector(metricsPrefix) }
      df_rows <- IO.blocking(df.collect())
      // for simplicity let's assume "select" in tests always includes "id" and do extra checks
      _ = assert(
        (df_rows.map(_.getAs[Long]("id"))).distinct.length == df_rows.length,
        "fetched items must be distinct")
    } yield (metricsPrefix, df_rows)

  private def readEventsWhere(sql: String): IO[(String, Array[Row])] =
    for {
      (metricsPrefix, df_rows) <- readEvents(
        getBaseReader(_)
          .where(s"source = '${testSource}' and (${sql})"))
      _ = assert(
        Array().sameElements(df_rows.filter(_.getAs[String]("source") != testSource)),
        "testSource filter must be respected")
    } yield (metricsPrefix, df_rows)

  private def testReadQuery(
      sqlWhereCondition: String,
      resultsPredicate: Event => Boolean,
      fetchPredicate: Event => Boolean)(createdEvents: Seq[Event]): Resource[IO, Assertion] = {
    val expectedResults = createdEvents.filter(resultsPredicate)
    assert(expectedResults.nonEmpty, "predicate should cover some events") // test sanity check

    val expectedInspectedResults = createdEvents.filter(fetchPredicate)
    assert(
      expectedResults.forall(expectedInspectedResults.contains(_)),
      "inspect predicate " +
        "should be broader or same as result predicate") // test sanity check

    for {
      (metricsPrefix, df_rows) <- Resource.eval(
        retryWhileIO[(String, Array[Row])](
          {
            readEventsWhere(sqlWhereCondition)
          },
          mr => {
            if (mr._2.length < expectedResults.size || !getNumberOfRowsReadSafe(mr._1, "events")
                .exists(_ >= expectedInspectedResults.size)) {
              println(
                s"need to retry ${mr._2.length} < ${expectedResults.size} || " +
                  s"${getNumberOfRowsReadSafe(mr._1, "events")} < ${expectedInspectedResults.size}")
              true
            } else false
          }
        ))
    } yield {
      val mismatching_events = df_rows
        .map(SparkSchemaHelper.fromRow[Event](_))
        .filterNot(resultsPredicate)
        .toList
      assert(List.empty == mismatching_events, "all results should match the filter")

      assert(df_rows.length == expectedResults.length, "spark read should read all expected events")

      val eventsRead = getNumberOfRowsRead(metricsPrefix, "events")
      assert(
        eventsRead == expectedInspectedResults.length,
        "spark should pushdown and " +
          "inspect " +
          "specific events")

    }
  }

  private def testReadSql(sql: String, retryUntilSize: Int): Resource[IO, Array[Row]] = {
    assert(sql.contains(testSource), "sql must be using testSource")
    for {
      (_, df_rows) <- Resource.eval(retryWhileIO[(String, Array[Row])]({
        readEvents(_ => spark.sql(sql))
      }, e => {
        if (e._2.length < retryUntilSize) {
          println(s"retry ${e._2.length} < ${retryUntilSize}")
          true
        } else false
      }))
    } yield df_rows
  }

  private def runIOTest[A](test: Resource[IO, A]): Unit =
    test.use(_ => IO.unit).unsafeRunSync()(cats.effect.unsafe.implicits.global)

  "EventsRelation" should "allow simple reads" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(Seq.fill(30)(EventCreate()))
      _ = assert(events.length > 20)
      res <- Resource.eval(
        retryWhileIO[Array[Row]](
          IO.blocking {
            val df = spark.read
              .format(DefaultSource.sparkFormatString)
              .useOIDCWrite
              .option("type", "events")
              .option("limitPerPartition", "10")
              .option("partitions", "5")
              .option("fetchSize", "100")
              .load()
            val view = s"events_${shortRandomString()}"
            df.createTempView(view)
            spark.sqlContext
              .sql(s"select * from ${view} where source = '$testSource'")
              .collect()
          },
          _.length < 20
        ))
      _ = assert(res.length >= 20)
    } yield ())
  }

  it should "apply a single pushdown filter" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq.fill(4)(EventCreate())
          ++ Seq.fill(5)(EventCreate(`type` = Some("Worktask")))
      )
      _ <- testReadQuery(
        s"type = 'Worktask'",
        _.`type`.contains("Worktask"),
        _.`type`.contains("Worktask")
      )(events)
    } yield ())
  }

  it should "get exception on invalid query" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filter.dataSetId"
    val df = getBaseReader(metricsPrefix)
      .where(s"dataSetId = 0 and source = '$testSource'")

    val thrown = the[SparkException] thrownBy df.count()
    thrown.getMessage should include("id must be greater than or equal to 1")
  }

  it should "apply a dataSetId pushdown filter" taggedAs WriteTest in {
    runIOTest(for {
      dataset <- makeDatasetId()
      events <- makeEvents(
        for {
          ty <- Seq("Worktask", "NotWorktask")
          dataset <- Seq(Some(dataset), None)
        } yield EventCreate(`type` = Some(ty), dataSetId = dataset)
      )
      _ <- testReadQuery(
        s"type = 'Worktask' and dataSetId = ${dataset}",
        e => e.`type`.contains("Worktask") && e.dataSetId.contains(dataset),
        e => e.`type`.contains("Worktask") && e.dataSetId.contains(dataset)
      )(events)
    } yield ())
  }

  it should "not fetch all items if filter on id" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(Seq.fill(2)(EventCreate()))
      _ <- testReadQuery(s"id = ${events.head.id}", _.id == events.head.id, _.id == events.head.id)(
        events)
    } yield ())
  }

  it should "not fetch all items if filter on externalId" taggedAs WriteTest in {
    val eventType = s"type-${shortRandomString()}"
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(`type` = Some(eventType)),
          EventCreate(externalId = Some(s"test-${shortRandomString()}"), `type` = Some(eventType)),
        ))
      e2 = events.tail.head
      _ <- testReadQuery(
        s"type = '${eventType}' and externalId = '${e2.externalId.get}'",
        e => e.`type`.contains(eventType) && e.externalId == e2.externalId,
        _.externalId == e2.externalId
      )(events)
    } yield ())
  }

  it should "apply pushdown filters when non pushdown columns are ANDed" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("NotWorktask")),
          EventCreate(`type` = Some("Worktask"), description = Some("Rule test rule broken.")),
          EventCreate(`type` = Some("Worktask"), description = Some("Rule test rule broken.")),
        ))
      _ <- testReadQuery(
        s"(type = 'Worktask' or description = 'Rule test rule broken.') and type = 'Worktask'",
        e => e.`type`.contains("Worktask"),
        e => e.`type`.contains("Worktask")
      )(events)
    } yield ())
  }

  it should "read all data when necessary" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("NotWorktask")),
          EventCreate(),
          EventCreate(`type` = Some("Worktask"), description = Some("Rule test rule broken.")),
          EventCreate(`type` = Some("Worktask"), description = Some("Rule test rule broken.")),
          EventCreate(`type` = Some("NotWorktask"), description = Some("Rule test rule broken.")),
        ))
      _ <- testReadQuery(
        s"type = 'Worktask' or description = 'Rule test rule broken.'",
        e => e.`type`.contains("Worktask") || e.description.contains("Rule test rule broken."),
        _ => true
      )(events)
    } yield ())
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(`type` = Some("NEWS"), subtype = Some("HACK")),
          EventCreate(`type` = Some("NEWS"), subtype = Some("ACK")),
          EventCreate(`type` = Some("FAKE_NEWS"), subtype = Some("HACK")),
          EventCreate(subtype = Some("HACK")),
          EventCreate(`type` = Some("NEWS")),
          EventCreate(),
        ))
      _ <- testReadQuery(
        s"type = 'NEWS' or subtype = 'HACK'",
        e => e.`type`.contains("NEWS") || e.subtype.contains("HACK"),
        e => e.`type`.contains("NEWS") || e.subtype.contains("HACK")
      )(events)
    } yield ())
  }

  it should "apply multiple pushdown filters" taggedAs WriteTest in {
    runIOTest(for {
      assets <- makeAssetIds(
        Seq(
          AssetCreate(name = "asset")
        ))
      events <- makeEvents(
        Seq(
          EventCreate(`type` = Some("***"), assetIds = Some(assets)),
          EventCreate(assetIds = Some(assets)),
          EventCreate(`type` = Some("***")),
          EventCreate(`type` = Some("***"), assetIds = Some(assets)),
          EventCreate(assetIds = Some(assets)),
          EventCreate(`type` = Some("***")),
        ))
      _ <- testReadQuery(
        s"type = '***' and assetIds = Array(${assets.mkString(", ")})",
        e => e.`type`.contains("***") && e.assetIds.contains(assets),
        e => e.`type`.contains("***") && e.assetIds.contains(assets)
      )(events)
    } yield ())
  }

  it should "handle or conditions" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(),
          EventCreate(`type` = Some("Workitem")),
          EventCreate(`type` = Some("Workitem")),
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("Work")),
        ))
      _ <- testReadQuery(
        s"type = 'Workitem' or type = 'Worktask'",
        e => e.`type`.contains("Workitem") || e.`type`.contains("Worktask"),
        e => e.`type`.contains("Workitem") || e.`type`.contains("Worktask")
      )(events)
    } yield ())
  }

  it should "handle in() conditions" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(),
          EventCreate(`type` = Some("Workitem")),
          EventCreate(`type` = Some("Workitem")),
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("Worktask")),
          EventCreate(`type` = Some("Work")),
        ))
      _ <- testReadQuery(
        s"type in ('Workitem', 'Worktask')",
        e => e.`type`.contains("Workitem") || e.`type`.contains("Worktask"),
        e => e.`type`.contains("Workitem") || e.`type`.contains("Worktask")
      )(events)
    } yield ())
  }

  it should "handle and, or and in() in one query" taggedAs WriteTest in {
    runIOTest(for {
      assets1 <- makeAssetIds(Seq(AssetCreate(name = "asset")))
      assets2 <- makeAssetIds(Seq(AssetCreate(name = "asset")))
      assets3 <- makeAssetIds(Seq(AssetCreate(name = "asset")))
      events <- makeEvents(
        Seq(
          EventCreate(`type` = Some("Workitem"), assetIds = Some(assets1)),
          EventCreate(`type` = Some("***"), assetIds = Some(assets1)),
          EventCreate(assetIds = Some(assets1)),
          EventCreate(assetIds = Some(assets1)),
          EventCreate(`type` = Some("Workitem"), assetIds = Some(assets2)),
          EventCreate(`type` = Some("***"), assetIds = Some(assets2)),
          EventCreate(assetIds = Some(assets2)),
          EventCreate(assetIds = Some(assets2)),
          EventCreate(`type` = Some("Workitem"), assetIds = Some(assets3)),
          EventCreate(`type` = Some("***"), assetIds = Some(assets3)),
          EventCreate(assetIds = Some(assets3)),
          EventCreate(assetIds = Some(assets3)),
          EventCreate(`type` = Some("Workitem")),
          EventCreate(`type` = Some("***")),
          EventCreate(),
        ))
      _ <- testReadQuery(
        s"(type = 'Workitem' or type = '***') and assetIds in(array(${assets1
          .mkString(", ")}), array(${assets2.mkString(", ")}))",
        e =>
          (e.`type`.contains("Workitem") || e.`type`.contains("***"))
            && (e.assetIds.contains(assets1) || e.assetIds.contains(assets2)),
        e =>
          (e.`type`.contains("Workitem") || e.`type`.contains("***"))
            && (e.assetIds.contains(assets1) || e.assetIds.contains(assets2))
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on minimum startTime" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(startTime = Some(Instant.ofEpochSecond(100L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(1568105460L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(1568105461L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(2568105460L))),
        ))
      _ <- testReadQuery(
        s"startTime > to_timestamp(1568105460)",
        e => e.startTime.exists(t => t.isAfter(Instant.ofEpochSecond(1568105460L))),
        // TOOD: inexact pushdown here
        e => e.startTime.exists(t => t.isAfter(Instant.ofEpochSecond(1568105459L)))
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on maximum createdTime" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(),
          EventCreate(),
        ))
      _ = testReadQuery(
        s"createdTime <= timestamp('2222-10-26 00:00:00.000Z')",
        _ => true,
        _ => true
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on maximum startTime" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(startTime = Some(Instant.ofEpochSecond(100L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(1568105460L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(1568105461L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(2568105460L))),
        ))
      _ = testReadQuery(
        s"startTime < timestamp('${DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(1568105461L))}')",
        e => e.startTime.exists(t => t.isBefore(Instant.ofEpochSecond(1568105461L))),
        // TOOD: inexact pushdown here
        e => e.startTime.exists(t => t.isBefore(Instant.ofEpochSecond(1568105462L)))
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on minimum and maximum startTime" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(startTime = Some(Instant.ofEpochSecond(100L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(1568105460L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(1568105461L))),
          EventCreate(startTime = Some(Instant.ofEpochSecond(2568105460L))),
        ))
      _ = testReadQuery(
        s"startTime >= timestamp('${DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(100L))}') and startTime <= timestamp('${DateTimeFormatter.ISO_INSTANT.format(Instant
          .ofEpochSecond(1968105460L))}')",
        e =>
          e.startTime
            .filter(_.isAfter(Instant.ofEpochSecond(99L)))
            .exists(_.isBefore(Instant.ofEpochSecond(1968105461L))),
        e =>
          e.startTime
            .filter(_.isAfter(Instant.ofEpochSecond(99L)))
            .exists(_.isBefore(Instant.ofEpochSecond(1968105461L)))
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on assetIds" taggedAs WriteTest in {
    runIOTest(for {
      assets <- makeAssetIds(
        Seq.fill(2)(
          AssetCreate(name = "asset")
        ))
      events <- makeEvents(
        Seq(
          EventCreate(),
          EventCreate(assetIds = Some(assets)),
          EventCreate(assetIds = Some(assets.drop(1))),
          EventCreate(assetIds = Some(assets.take(1))),
        ))
      _ = testReadQuery(
        s"assetIds In(Array(${assets.head}))",
        e => e.assetIds.contains(Seq(assets.head)),
        e => e.assetIds.exists(_.contains(assets.head))
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on eventIds" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(Seq.fill(3)(EventCreate()))
      _ = testReadQuery(
        s"id = ${events.head.id}",
        _.id == events.head.id,
        _.id == events.head.id
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on many eventIds" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(Seq.fill(6)(EventCreate()))
      focusIds = events.slice(1, 4).map(_.id)
      _ = testReadQuery(
        s"id In(${focusIds.mkString(", ")})",
        e => focusIds.contains(e.id),
        e => focusIds.contains(e.id)
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on many eventIds with or" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(Seq.fill(6)(EventCreate()))
      focusIds = events.slice(1, 4).map(_.id)
      _ <- testReadQuery(
        focusIds.map(i => s"id = ${i}").mkString(" or "),
        e => focusIds.contains(e.id),
        e => focusIds.contains(e.id)
      )(events)
    } yield ())
  }

  it should "handle pushdown filters on many eventIds with other filters" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq.fill(2)(EventCreate())
          ++ Seq.fill(2)(EventCreate(description = Some("eventspushdowntest")))
          ++ Seq.fill(6)(EventCreate(description = Some("eventspushdowntest")))
          ++ Seq.fill(2)(EventCreate())
      )
      _ <- testReadQuery(
        s"id In(${events.drop(4).map(_.id).mkString(", ")}) and description = 'eventspushdowntest'",
        e => events.drop(4).exists(_.id == e.id) && e.description.contains("eventspushdowntest"),
        e => events.drop(4).exists(_.id == e.id),
      )(events)
    } yield ())
  }

  it should "handle a really advanced query" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(for {
        ty <- Seq(Some("maintenance"), Some("upgrade"), Some("a"), None)
        sub <- Seq(Some("manual"), Some("automatic"), Some("b"), None)
      } yield EventCreate(`type` = ty, subtype = sub))
      _ = testReadQuery(
        s"((type = 'maintenance' or type = 'upgrade') " +
          s"and subtype in('manual', 'automatic')) " +
          s"or (type = 'maintenance' and subtype = 'manual') " +
          s"or (type = 'upgrade') and source = 'something'",
        e =>
          (e.`type`.contains("maintenance") || e.`type`.contains("upgrade"))
            && e.subtype.exists(Seq("manual", "automatic").contains(_))
            || (e.`type`.contains("maintenance") && e.subtype.contains("manual"))
            || (e.`type`.contains("upgrade") && e.subtype.contains("something")),
        e =>
          (e.`type`.contains("maintenance") || e.`type`.contains("upgrade"))
            && e.subtype.exists(Seq("manual", "automatic").contains(_))
            || (e.`type`.contains("maintenance") && e.subtype.contains("manual"))
            || (e.`type`.contains("upgrade") && e.subtype.contains("something"))
      )(events)
    } yield ())
  }

  it should "handle pushdown on eventId or something else" taggedAs WriteTest in {
    runIOTest(for {
      events <- makeEvents(
        Seq(
          EventCreate(),
          EventCreate(`type` = Some("maintenance")),
          EventCreate(),
        ))
      _ = testReadQuery(
        s"type = 'maintenance' or id = ${events.head.id}",
        e => e.`type`.contains("maintenance") || e.id == events.head.id,
        e => e.`type`.contains("maintenance") || e.id == events.head.id
      )(events)
    } yield ())
  }

  private def makeDestinationDf(): Resource[IO, (DataFrame, String, String)] = {
    val targetView = s"destinationEvent_${shortRandomString()}"
    val metricsPrefix = s"test.${shortRandomString()}"
    Resource.make {
      IO.blocking {
        val destinationDf: DataFrame = spark.read
          .format(DefaultSource.sparkFormatString)
          .useOIDCWrite
          .option("type", "events")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .load()
        destinationDf.createOrReplaceTempView(targetView)
        (destinationDf, targetView, metricsPrefix)
      }
    } {
      case (_, targetView, _) =>
        IO.blocking {
          spark
            .sql(s"""select * from ${targetView} where source = '${testSource}'""")
            .write
            .format(DefaultSource.sparkFormatString)
            .useOIDCWrite
            .option("type", "events")
            .option("onconflict", "delete")
            .save()
        }
    }
  }

  it should "allow null values for all event fields except id" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, _) <- makeDestinationDf()
      _ = spark
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
             |'$testSource' as source,
             |null as externalId,
             |0 as createdTime,
             |0 as lastUpdatedTime,
             |null as dataSetId
     """.stripMargin)
        .write
        .insertInto(targetView)
      rows <- testReadSql(
        s"select * from ${targetView} where source = '${testSource}'",
        1
      )
    } yield {
      assert(rows.length == 1)
      val storedMetadata = rows.head.getAs[Map[String, String]](6)
      assert(storedMetadata.size == 1)
      assert(storedMetadata.get("foo").contains("bar"))
    })
  }

  it should "support upserts" taggedAs WriteTest in {
    runIOTest(for {
      (destinationDf, targetView, metricsPrefix) <- makeDestinationDf()
      _ <- makeEvents(Seq.fill(10)(EventCreate()))
      // Post new events
      _ = spark
        .sql(s"""
            |select "bar" as description,
            |startTime,
            |endTime,
            |null as subtype,
            |null as assetIds,
            |id,
            |"$metricsPrefix" as type,
            |map("foo", null, "bar", "test") as metadata,
            |"$testSource" as source,
            |concat("$testSource", cast(id as string)) as externalId,
            |createdTime,
            |lastUpdatedTime,
            |null as dataSetId
            |from ${sourceView}
            |limit 10
""".stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      _ = assert(getNumberOfRowsCreated(metricsPrefix, "events") == 10)
      descriptionsAfterPost <- testReadSql(
        s"select description, id from ${targetView} where " +
          s"source =" +
          s" '${testSource}' and type='${metricsPrefix}'",
        10)
      _ = assert(descriptionsAfterPost.length == 10)
      _ = assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "bar"))
      assets <- makeAssetIds(Seq(AssetCreate(name = "asset")))
      // Update events
      _ = spark
        .sql(s"""
            |select "foo" as description,
            |startTime,
            |endTime,
            |type,
            |subtype,
            |array(${assets.mkString(", ")}) as assetIds,
            |null as id,
            |map("some", null, "metadata", "test") as metadata,
            |"$testSource" as source,
            |concat("$testSource", cast(id as string)) as externalId,
            |createdTime,
            |lastUpdatedTime,
            |null as dataSetId
            |from ${sourceView}
            |limit 5
""".stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      _ = assert(getNumberOfRowsCreated(metricsPrefix, "events") == 10)
      _ = assert(getNumberOfRowsUpdated(metricsPrefix, "events") == 5)
      descriptionsAfterUpdate <- testReadSql(
        s"select description, id from ${targetView} where " +
          s"source" +
          s" = '$testSource'" +
          s" and " +
          s"description = 'foo' and source = '${testSource}'",
        5)
      _ = assert(descriptionsAfterUpdate.length == 5)
      _ = assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "foo"))
      _ = assert(getNumberOfRowsCreated(metricsPrefix, "events") == 10)
      dfWithCorrectAssetIds <- testReadSql(
        s"select * from ${targetView} where assetIds = array(${assets.mkString(", ")}) and source = " +
          s"'${testSource}'",
        5
      )
      _ = assert(dfWithCorrectAssetIds.length == 5)
    } yield ())
  }

  it should "allow empty metadata updates" taggedAs WriteTest in {
    val externalId1 = UUID.randomUUID.toString
    runIOTest(for {
      _ <- makeEvents(
        Seq(
          EventCreate(
            externalId = Some(externalId1),
            metadata = Some(Map("test1"
              -> "test1")))))
      wdf = spark.sql(
        s"select '$externalId1' as externalId, map() as metadata, '${testSource}' " +
          s"as source")

      _ = wdf.write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("onconflict", "upsert")
        .save()
      updated = writeClient.events.retrieveByExternalId(externalId1).unsafeRunSync()(IORuntime.global)

      _ = updated.metadata shouldBe None
    } yield ())
  }

  it should "allow inserts in savemode" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, _) <- makeDestinationDf()
      assets <- makeAssetIds(Seq(AssetCreate(name = "asset")))
      _ <- makeEvents(Seq.fill(10)(EventCreate()))
      // Test inserts
      df <- Resource.eval(
        retryWhileIO[DataFrame](
          IO.blocking { spark.sql(s"""
           |select "foo" as description,
           |monotonically_increasing_id() as id,
           |least(startTime, endTime) as startTime,
           |greatest(startTime, endTime) as endTime,
           |array(${assets.mkString(", ")}) as assetIds,
           |map("foo", null, "bar", "test") as metadata,
           |"$testSource" as source,
           |concat("$testSource", string(id)) as externalId
           |from ${targetView} where source = "$testSource"
           |limit 10
           """.stripMargin) },
          df => df.collect().length < 10
        ))

      metricsPrefix = s"metrics-${shortRandomString()}"
      _ = df.write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreated == 10)
      targetView2 = s"view_${shortRandomString()}"
      _ = df.createOrReplaceTempView(targetView2)
      dfWithSourceInsertTest <- testReadSql(
        s"select * from ${targetView2} where source = '$testSource'",
        10)
      _ = assert(dfWithSourceInsertTest.length == 10)
      // Trying to insert existing rows should throw a CdpApiException
      e = intercept[SparkException] {
        df.write
          .format(DefaultSource.sparkFormatString)
          .useOIDCWrite
          .option("type", "events")
          .save()
      }
      _ = e.getCause shouldBe a[CdpApiException]
      cdpApiException = e.getCause.asInstanceOf[CdpApiException]
      _ = assert(cdpApiException.code == 409)
      eventsCreatedAfterFailure = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterFailure == 10)
    } yield ())
  }

  it should "allow NULL updates in savemode" taggedAs WriteTest in forAll(updateAndUpsert) {
    updateMode =>
      runIOTest(for {
        (_, targetView, metricsPrefix) <- makeDestinationDf()
        assets <- makeAssetIds(Seq(AssetCreate(name = "asset")))
        // Insert test event
        df = spark
          .sql(s"""
             |select "foo" as description,
             |cast(from_unixtime(10) as timestamp) as startTime,
             |cast(from_unixtime(20) as timestamp) as endTime,
             |"test-type" as type,
             |"test-type" as subtype,
             |array(${assets.mkString(", ")}) as assetIds,
             |"$testSource-$updateMode" as source,
             |"$testSource-id" as externalId
""".stripMargin)

        _ = df.write
          .format(DefaultSource.sparkFormatString)
          .useOIDCWrite
          .option("type", "events")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .option("onconflict", "abort")
          .save()

        insertTest <- Resource.eval(
          retryWhileIO[Array[Row]](
            IO.blocking {
              spark
                .sql(s"select * from ${targetView} where source = " +
                  s"'$testSource-$updateMode'")
                .collect()
            },
            df => df.length < 1
          ))

        _ = spark
          .sql(s"""
            |select "foo-$testSource" as description,
            |NULL as endTime,
            |NULL as subtype,
            |"$testSource-$updateMode" as source,
            |${insertTest.head.getAs[Long]("id")} as id,
            |NULL as externalId
""".stripMargin)
          .write
          .format(DefaultSource.sparkFormatString)
          .useOIDCWrite
          .option("type", "events")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .option("ignoreNullFields", "false")
          .option("onconflict", updateMode)
          .save()

        updateTest <- testReadSql(
          s"select * from ${targetView} where source = '$testSource-$updateMode' and description = " +
            s"'foo-$testSource'",
          1
        )
        _ = updateTest.length shouldBe 1
        Array(updatedRow) = updateTest
        updatedEvent = SparkSchemaHelper.fromRow[EventsReadSchema](updatedRow)
        _ = updatedEvent.endTime shouldBe None
        _ = updatedEvent.startTime shouldBe defined
        _ = updatedEvent.source shouldBe Some(s"$testSource-$updateMode")
        _ = updatedEvent.externalId shouldBe None
        _ = updatedEvent.assetIds shouldBe Some(assets)
        _ = updatedEvent.`type` shouldBe Some("test-type")
        _ = updatedEvent.subtype shouldBe None
      } yield ())
  }

  it should "allow duplicated ids and external ids when using upsert in savemode" in {
    runIOTest(for {
      (_, targetView, metricsPrefix) <- makeDestinationDf()
      externalId = shortRandomString()
      // Post new events
      eventsToCreate = spark
        .sql(s"""
             |select "$testSource" as source,
             |cast(from_unixtime(0) as timestamp) as startTime,
             |"$externalId" as externalId
             |union
             |select "$testSource" as source,
             |cast(from_unixtime(1) as timestamp) as startTime,
             |"$externalId" as externalId
""".stripMargin)

      _ = eventsToCreate
        .repartition(1)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("onconflict", "upsert")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreated == 1)

      _ = a[NullPointerException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "events")

      // We need to add endTime as well, otherwise Spark is clever enough to remove duplicates
      // on its own, it seems.
      eventsToUpdate = spark
        .sql(s"""
             |select "$testSource" as source,
             |"bar" as description,
             |cast(from_unixtime(0) as timestamp) as endTime,
             |"$externalId" as externalId
             |union
             |select "$testSource" as source,
             |"bar" as description,
             |cast(from_unixtime(1) as timestamp) as endTime,
             |"$externalId" as externalId
""".stripMargin)

      _ = eventsToUpdate
        .repartition(1)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("onconflict", "upsert")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      eventsCreatedAfterByExternalId = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterByExternalId == 1)

      eventsUpdatedAfterByExternalId = getNumberOfRowsUpdated(metricsPrefix, "events")
      _ = assert(eventsUpdatedAfterByExternalId == 1)

      descriptionsAfterUpdate <- testReadSql(
        s"select description, id from ${targetView} where " +
          s"source " +
          s"= '$testSource' and description = " +
          s"'bar'",
        1)
      _ = assert(descriptionsAfterUpdate.length == 1)
      _ = assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

      eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterUpsert == 1)

      id <- Resource.eval(writeClient.events.retrieveByExternalId(externalId).map(_.id))

      eventsToUpdateById = spark
        .sql(s"""
             |select "$testSource" as source,
             |"bar2" as description,
             |cast(from_unixtime(0) as timestamp) as endTime,
             |${id.toString} as id
             |union
             |select "$testSource" as source,
             |"bar2" as description,
             |cast(from_unixtime(1) as timestamp) as endTime,
             |${id.toString} as id
""".stripMargin)
      _ = eventsToUpdateById
        .repartition(1)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("onconflict", "upsert")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      eventsCreatedAfterById = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterById == 1)

      eventsUpdatedAfterById = getNumberOfRowsUpdated(metricsPrefix, "events")
      _ = assert(eventsUpdatedAfterById == 2)

      descriptionsAfterUpdateById <- testReadSql(
        s"select description, id from ${targetView} " +
          s"where" +
          s" " +
          s"source = '$testSource' and description = " +
          s"'bar2'",
        1)
      _ = assert(descriptionsAfterUpdateById.length == 1)
    } yield ())
  }

  it should "allow upsert in savemode" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, _) <- makeDestinationDf()
      metricsPrefix = s"metrics-${shortRandomString()}"
      // Post new events
      _ <- makeEvents(Seq.fill(11)(EventCreate()))
      _ = spark
        .sql(s"""
              |select "foo" as description,
              |least(startTime, endTime) as startTime,
              |greatest(startTime, endTime) as endTime,
              |"$metricsPrefix" as type,
              |subtype,
              |null as assetIds,
              |bigint(0) as id,
              |map("foo", null, "bar", "test") as metadata,
              |"$testSource" as source,
              |concat("$testSource", string(id)) as externalId,
              |null as createdTime,
              |lastUpdatedTime
              |from ${sourceView}
              |limit 10
     """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("onconflict", "upsert")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreated == 10)
      // Check if post worked
      descriptionsAfterPost <- testReadSql(
        s"select description, id from ${targetView} where " +
          s"source = " +
          s"'${testSource}' and type = '${metricsPrefix}'",
        10)
      _ = assert(descriptionsAfterPost.length == 10)
      _ = assert(descriptionsAfterPost.map(_.getString(0)).forall(_ == "foo"))
      assets <- makeAssetIds(Seq(AssetCreate(name = "asset")))
      // Update events
      updateEventsByIdDf = spark
        .sql(s"""
             |select "bar" as description,
             |startTime,
             |endTime,
             |type,
             |subtype,
             |array(${assets.mkString(", ")}) as assetIds,
             |id,
             |metadata,
             |"$testSource" as source,
             |null as externalId,
             |createdTime,
             |lastUpdatedTime
             |from ${targetView}
             |where source = '$testSource' and type='$metricsPrefix'
             |order by id
             |limit 5
  """.stripMargin)
      _ = updateEventsByIdDf.write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("onconflict", "upsert")
        .option("type", "events")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      eventsUpdated = getNumberOfRowsUpdated(metricsPrefix, "events")
      _ = assert(eventsUpdated == 5)

      maxByIdUpdateId = updateEventsByIdDf.agg("id" -> "max").collect().head.getLong(0)
      updateEventsByExternalIdDf = spark
        .sql(s"""
             |select "bar" as description,
             |startTime,
             |endTime,
             |type,
             |subtype,
             |array(${assets.mkString(", ")}) as assetIds,
             |id,
             |map("some", null, "metadata", "test") as metadata,
             |"$testSource" as source,
             |externalId,
             |createdTime,
             |lastUpdatedTime
             |from ${targetView}
             |where id > $maxByIdUpdateId and source = '$testSource' and type = '$metricsPrefix'
             |order by id
             |limit 5
  """.stripMargin)
      _ = updateEventsByExternalIdDf.write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("onconflict", "upsert")
        .option("type", "events")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()
      eventsUpdatedAfterByExternalId = getNumberOfRowsUpdated(metricsPrefix, "events")
      _ = assert(eventsUpdatedAfterByExternalId == 10)
      // Check if upsert worked
      descriptionsAfterUpdate <- testReadSql(
        s"select description, id from ${targetView} where source = '$testSource' and " +
          s"description = " +
          s"'bar'",
        10)
      _ = assert(descriptionsAfterUpdate.length == 10)
      _ = assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))

      eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterUpsert == 10)
      dfWithCorrectAssetIds <- testReadSql(
        s"select * from ${targetView} where assetIds = array" +
          s"(${assets.mkString(", ")}) and source = " +
          s"'$testSource'",
        10)
      _ = assert(dfWithCorrectAssetIds.length == 10)
    } yield ())
  }

  it should "allow partial updates in savemode" taggedAs WriteTest in {
    runIOTest(for {
      (destinationDf, targetView, metricsPrefix) <- makeDestinationDf()
      // Insert some test data
      _ <- Resource.eval(IO.blocking {
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
            |"$testSource" as source,
            |concat("$testSource", string(id)) as externalId,
            |createdTime,
            |lastUpdatedTime,
            |null as dataSetId
            |from ${sourceView}
            |limit 10
""".stripMargin)
          .select(destinationDf.columns.map(col).toIndexedSeq: _*)
          .write
          .insertInto(targetView)
      })
      // Check if insert worked
      descriptionsAfterInsert <- testReadSql(
        s"select description, id from ${targetView} " +
          s"where source = '$testSource' and description = 'foo'",
        10
      )
      _ = assert(descriptionsAfterInsert.length == 10)
      _ = assert(descriptionsAfterInsert.map(_.getString(0)).forall(_ == "foo"))
      eventsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterUpsert == 10)
      // Update the data
      descriptionsAfterUpdate <- Resource.eval(
        retryWhileIO[Array[Row]](
          IO.blocking {
            spark
              .sql(s"""
                   |select "bar" as description,
                   |id
                   |from ${targetView}
                   |where source = '$testSource'
""".stripMargin)
              .write
              .format(DefaultSource.sparkFormatString)
              .useOIDCWrite
              .option("type", "events")
              .option("onconflict", "update")
              .option("collectMetrics", "true")
              .option("metricsPrefix", metricsPrefix)
              .save()
            spark
              .sql(s"select description from ${targetView} " +
                s"where source = '$testSource' and description = 'bar'")
              .collect()
          },
          df => df.length < 10
        ))
      _ = assert(descriptionsAfterUpdate.length == 10)
      _ = assert(descriptionsAfterUpdate.map(_.getString(0)).forall(_ == "bar"))
      eventsUpdatedAfterUpsert = getNumberOfRowsUpdated(metricsPrefix, "events")
      // Due to retries, this may exceed 10
      _ = assert(eventsUpdatedAfterUpsert >= 10)
      // Trying to update non-existing ids should throw a 400 CdpApiException
      e <- Resource.eval(IO.blocking {
        intercept[SparkException] {
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
               |from ${targetView}
               |where source = '$testSource'
               |limit 1
  """.stripMargin)
            .write
            .format(DefaultSource.sparkFormatString)
            .useOIDCWrite
            .option("type", "events")
            .option("onconflict", "update")
            .save()
        }
      })
      _ = e.getCause shouldBe a[CdpApiException]
      cdpApiException = e.getCause.asInstanceOf[CdpApiException]
      _ = assert(cdpApiException.code == 400)
      eventsCreatedAfterFail = getNumberOfRowsCreated(metricsPrefix, "events")
      _ = assert(eventsCreatedAfterFail == eventsCreatedAfterUpsert)
      eventsUpdatedAfterFail = getNumberOfRowsUpdated(metricsPrefix, "events")
      _ = assert(eventsUpdatedAfterFail == eventsUpdatedAfterUpsert)
    } yield ())
  }

  it should "allow null ids on event update" taggedAs WriteTest in {
    runIOTest(for {
      (destinationDf, targetView, _) <- makeDestinationDf()
      dataset <- makeDatasetId()
      // Post new events
      _ = spark
        .sql(s"""
            |select concat("$testSource", string(id)) as externalId,
            |null as id,
            |'$testSource' as source,
            |startTime,
            |endTime,
            |type,
            |subtype,
            |'foo' as description,
            |map() as metadata,
            |null as assetIds,
            |null as lastUpdatedTime,
            |null as createdTime,
            |${dataset} as dataSetId
            |from ${sourceView}
            |limit 5
 """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      // Check if post worked
      eventsFromTestDf <- testReadSql(
        s"select * from ${targetView} where source = '$testSource' " +
          s"and" +
          s" description = 'foo' and " +
          s"dataSetId = " +
          s"${dataset}",
        5)
      _ = assert(eventsFromTestDf.length == 5)
      // Upsert events
      descriptionsAfterUpdate <- Resource.eval(
        retryWhileIO[Array[Row]](
          IO.blocking {
            spark
              .sql(s"""
                 |select externalId,
                 |'bar' as description
                 |from ${targetView}
                 |where source = '$testSource'
 """.stripMargin)
              .write
              .format(DefaultSource.sparkFormatString)
              .useOIDCWrite
              .option("type", "events")
              .option("onconflict", "update")
              .save()
            spark
              .sql(
                s"select description from ${targetView} where source = '$testSource' and " +
                  s"description = " +
                  s"'bar'")
              .collect()
          },
          df => df.length < 5
        ))
      _ = assert(descriptionsAfterUpdate.length == 5)
    } yield ())
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
                |123 as datasetId
                |from ${sourceView}
                |limit 1
     """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .save()

    }
    assert(error.getMessage.contains("Column 'assetIds' was expected to have type Seq[Long], but"))
  }

  it should "allow deletes in savemode" taggedAs WriteTest in {

    runIOTest(for {
      (destinationDf, targetView, _) <- makeDestinationDf()
      // Insert some test data
      _ = spark
        .sql(s"""
            |select "foo" as description,
            |least(startTime, endTime) as startTime,
            |greatest(startTime, endTime) as endTime,
            |type,
            |subtype,
            |null as assetIds,
            |bigint(0) as id,
            |map("foo", null, "bar", "test") as metadata,
            |"$testSource" as source,
            |concat("$testSource", externalId) as externalId,
            |0 as createdTime,
            |lastUpdatedTime,
            |null as dataSetId
            |from ${sourceView}
            |limit 10
""".stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      // Check if insert worked
      idsAfterInsert <- testReadSql(s"select id from ${targetView} where source = '$testSource'", 10)
      _ = assert(idsAfterInsert.length == 10)
    } yield ())
  }

  it should "support ignoring unknown ids in deletes" in {
    runIOTest(for {
      (destinationDf, targetView, _) <- makeDestinationDf()
      // Insert some test data
      _ = spark
        .sql(s"""
             |select "foo" as description,
             |least(startTime, endTime) as startTime,
             |greatest(startTime, endTime) as endTime,
             |type,
             |subtype,
             |null as assetIds,
             |bigint(0) as id,
             |map("foo", null, "bar", "test") as metadata,
             |"$testSource" as source,
             |concat("$testSource", string(id)) as externalId,
             |0 as createdTime,
             |lastUpdatedTime,
             |null as dataSetId
             |from ${sourceView}
             |limit 1
      """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      // Check if insert worked
      idsAfterInsert <- testReadSql(
        s"select id from ${targetView} where source = " +
          s"'$testSource'",
        1)
      _ = assert(idsAfterInsert.length == 1)
      _ = spark
        .sql("select 123 as id".stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "events")
        .option("onconflict", "delete")
        .option("ignoreUnknownIds", "true")
        .save()
      // Should throw error if ignoreUnknownIds is false
      e = intercept[SparkException] {
        spark
          .sql("select 123 as id")
          .write
          .format(DefaultSource.sparkFormatString)
          .useOIDCWrite
          .option("type", "events")
          .option("onconflict", "delete")
          .option("ignoreUnknownIds", "false")
          .save()
      }
      _ = e.getCause shouldBe a[CdpApiException]
      cdpApiException = e.getCause.asInstanceOf[CdpApiException]
      _ = assert(cdpApiException.code == 400)
    } yield ())
  }

  it should "support deletes by externalIds" in {
    val randomSuffix = shortRandomString()
    import scala.language.reflectiveCalls
    val mutableClosure = new { var deleteCounter = 0L }
    runIOTest(for {
      (destinationDf, targetView, metricsPrefix) <- makeDestinationDf()
      // Insert some test data
      _ = spark
        .sql(s"""
             |select "foo" as description,
             |least(startTime, endTime) as startTime,
             |greatest(startTime, endTime) as endTime,
             |type,
             |subtype,
             |null as assetIds,
             |bigint(0) as id,
             |map("foo", null, "bar", "test") as metadata,
             |"$testSource" as source,
             |concat(string(id), '${randomSuffix}') as externalId,
             |0 as createdTime,
             |lastUpdatedTime,
             |null as dataSetId
             |from ${sourceView}
             |limit 10
     """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      // Check if insert worked
      idsAfterInsert <- testReadSql(
        s"select id, externalId from ${targetView} where source = '$testSource'",
        10
      )
      _ = assert(idsAfterInsert.length == 10)
      // Delete the data
      idsAfterDelete <- Resource.eval(
        retryWhileIO[Array[Row]](
          IO.blocking {
            spark
              .sql(s"select externalId from ${targetView} where source = '$testSource'")
              .write
              .format(DefaultSource.sparkFormatString)
              .useOIDCWrite
              .option("type", "events")
              .option("onconflict", "delete")
              .option("collectMetrics", "true")
              .option("metricsPrefix", metricsPrefix)
              .save()
            mutableClosure.deleteCounter += getNumberOfRowsDeleted(metricsPrefix, "events")
            spark
              .sql(s"select externalId from ${targetView} where source = '$testSource'")
              .collect()
          },
          df => df.length > 0
        ))
      _ = assert(idsAfterDelete.isEmpty)
      _ = mutableClosure.deleteCounter should be >= 10L
    } yield ())
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val eventInsert = EventsInsertSchema()
    eventInsert.transformInto[EventsReadSchema]

    val eventUpsert = EventsUpsertSchema()
    eventUpsert.into[EventsReadSchema].withFieldComputed(_.id, eu => eu.id.getOrElse(0L))
  }
}
