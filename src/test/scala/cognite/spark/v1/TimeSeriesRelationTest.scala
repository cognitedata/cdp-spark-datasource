package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers, OptionValues, ParallelTestExecution}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import io.scalaland.chimney.dsl._
import org.scalatest.prop.TableDrivenPropertyChecks.forAll

import scala.util.control.NonFatal

class TimeSeriesRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with OptionValues {
  import spark.implicits._

  val sourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", readApiKey)
    .option("type", "timeseries")
    .load()
  sourceDf.createOrReplaceTempView("sourceTimeSeries")

  val destinationDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "timeseries")
    .load()
  destinationDf.createOrReplaceTempView("destinationTimeSeries")

  "TimeSeriesRelation" should "read all data regardless of the number of partitions" taggedAs ReadTest in {
    val dfreader = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
    val singlePartitionCount = dfreader.option("partitions", 1).load().count()
    assert(singlePartitionCount > 0)
    for (np <- Seq(4, 8, 12)) {
      val df = dfreader.option("partitions", np.toString).load()
      assert(df.count == singlePartitionCount)
    }
  }

  it should "handle pushdown filters on assetId with multiple assetIds" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "1")
      .load()
      .where("createdTime < to_timestamp(1593698800)")
      .where(s"assetId In(6191827428964450, 1081261865374641, 3047932288982463)")
    assert(df.count == 89)
    val timeSeriesRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(timeSeriesRead == 89)
  }

  it should "handle pushdown filters on assetId, dataSetId" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.dataSetId"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "1")
      .load()
      .where("createdTime < to_timestamp(1595400000)")
      .where(
        s"assetId In(6191827428964450, 1081261865374641, 3047932288982463) or dataSetId in (1, 2, 3)")
    assert(df.count == 89)
    val timeSeriesRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(timeSeriesRead == 89)
  }

  it should "handle pushdown filters on assetId on nonexisting assetId" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assetIds.nonexisting"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"assetId = 99")
    assert(df.count == 0)

    // Metrics counter does not create a Map key until reading the first row
    assertThrows[NoSuchElementException](getNumberOfRowsRead(metricsPrefix, "timeseries"))
  }

  it should "not fetch all items if filter on id" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.timeSeries.id"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "1")
      .load()
      .where("createdTime < to_timestamp(1595400000)")
      .where("id in (384300500341710, 881888156367988, 606890273743471)")

    assert(df.count == 3)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(eventsRead == 3)
  }

  it should "not fetch all items if filter on externalId" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.timeSeries.externalId"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "1")
      .load()
      .where("createdTime < to_timestamp(1595400000)")
      .where("name = 'VAL_23-KA-9101_PHD:VALUE' or externalId = 'pi:160224'")

    assert(df.count == 2)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(eventsRead == 2)
  }

  it should "insert a time series with no name" taggedAs WriteTest in {
    val insertNoNameUnit = s"insert-no-name-${shortRandomString()}"
    val description = s"no name ${shortRandomString()}"

    try {
      // Insert new time series test data
      spark
        .sql(s"""
                |select '$description' as description,
                |null as name,
                |isString,
                |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
                |'$insertNoNameUnit' as unit,
                |null as assetId,
                |isStep,
                |cast(array() as array<long>) as securityCategories,
                |id,
                |'no-name-time-series' as externalId,
                |createdTime,
                |lastUpdatedTime,
                |$testDataSetId as dataSetId
                |from sourceTimeSeries
                |limit 1
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationTimeSeries")

      // Check if post worked
      val dfAfterPost = retryWhile[Array[Row]](
        spark
          .sql(
            s"""select * from destinationTimeSeries where unit = '$insertNoNameUnit' and description = '$description'""")
          .collect,
        df => df.length != 1)
      assert(dfAfterPost.length == 1)
      assert(dfAfterPost.head.get(0) == null)
      dfAfterPost.head.getAs[Long]("dataSetId") shouldBe testDataSetId
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(insertNoNameUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "successfully both update and insert time series" taggedAs WriteTest in {
    val metricsPrefix = s"update-and-insert-${shortRandomString()}"
    val initialDescription = s"post-testing-${shortRandomString()}"
    val updatedDescription = s"upsert-testing-${shortRandomString()}"
    val insertTestUnit = s"insert-test${shortRandomString()}"

    try {
      val df = spark.read
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .load()
      df.createOrReplaceTempView("destinationTimeSeriesInsertAndUpdate")

      a[NoSuchElementException] should be thrownBy getNumberOfRowsCreated(metricsPrefix, "timeseries")
      a[NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "timeseries")

      val randomSuffix = shortRandomString()
      // Insert new time series test data
      spark
        .sql(s"""
                |select '$initialDescription' as description,
                |concat('TEST', name) as name,
                |isString,
                |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
                |'$insertTestUnit' as unit,
                |null as assetId,
                |isStep,
                |cast(array() as array<long>) as securityCategories,
                |id,
                |concat(string(id), "_upsert_${randomSuffix}") as externalId,
                |createdTime,
                |lastUpdatedTime,
                |dataSetId
                |from sourceTimeSeries
                |limit 5
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationTimeSeriesInsertAndUpdate")

      a[NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "timeseries")
      val rowsCreated = getNumberOfRowsCreated(metricsPrefix, "timeseries")
      assert(rowsCreated == 5)

      // Check if post worked
      val initialDescriptionsAfterPost = retryWhile[Int](
        (for (_ <- 1 to 5)
          yield
            spark
              .sql(
                s"""select * from destinationTimeSeriesInsertAndUpdate where unit = '$insertTestUnit' and description = '$initialDescription'""")
              .collect
              .length).min,
        length => length < 5
      )
      assert(initialDescriptionsAfterPost == 5)

      // Update time series data
      spark
        .sql(s"""
                |select '$updatedDescription' as description,
                |concat('TEST', name) as name,
                |isString,
                |map("foo", null, "bar", "test") as metadata,
                |unit,
                |null as assetId,
                |isStep,
                |securityCategories,
                |id,
                |externalId,
                |createdTime,
                |lastUpdatedTime,
                |dataSetId
                |from destinationTimeSeries
                |where description = '$initialDescription'
          """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationTimeSeriesInsertAndUpdate")

      val rowsCreatedAfterUpdate = getNumberOfRowsCreated(metricsPrefix, "timeseries")
      assert(rowsCreatedAfterUpdate == 5)
      val rowsUpdatedAfterUpdate = getNumberOfRowsUpdated(metricsPrefix, "timeseries")
      assert(rowsUpdatedAfterUpdate == 5)

      // Check if update worked
      val updatedDescriptionsAfterUpdate = retryWhile[Array[Row]](
        spark
          .sql(
            s"""select * from destinationTimeSeriesInsertAndUpdate where unit = '$insertTestUnit' and description = '$updatedDescription'""")
          .collect,
        df => df.length < 5
      )
      assert(updatedDescriptionsAfterUpdate.length == 5)

      val initialDescriptionsAfterUpdate = retryWhile[Array[Row]](
        spark
          .sql(
            s"""select * from destinationTimeSeriesInsertAndUpdate where unit = '$insertTestUnit' and description = '$initialDescription'""")
          .collect,
        df => df.length > 0
      )
      assert(initialDescriptionsAfterUpdate.length == 0)
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(insertTestUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support abort in savemode" taggedAs WriteTest in {
    val metricsPrefix = s"abort-savemode-${shortRandomString()}"
    val abortDescription = s"spark-test-abort-${shortRandomString()}"
    val abortUnit = s"insert-abort-${shortRandomString()}"

    try {
      val df = spark.read
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .load()
      df.createOrReplaceTempView("destinationTimeSeriesAbort")

      // Insert new time series test data
      spark
        .sql(s"""
                |select '$abortDescription' as description,
                |isString,
                |concat('TEST_', name) as name,
                |map("foo", null, "bar", "test") as metadata,
                |'$abortUnit' as unit,
                |NULL as assetId,
                |isStep,
                |cast(array() as array<long>) as securityCategories,
                |id,
                |concat(string(id), "_abort_${shortRandomString()}") as externalId,
                |createdTime,
                |lastUpdatedTime
                |from sourceTimeSeries
                |limit 7
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      a[NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "timeseries")
      val rowsCreated = getNumberOfRowsCreated(metricsPrefix, "timeseries")
      assert(rowsCreated == 7)

      val dfWithDescriptionInsertTest = retryWhile[DataFrame](
        spark
          .sql(
            s"select * from destinationTimeSeries where unit = '$abortUnit' and description = '$abortDescription'"),
        df => df.count() < 7
      )
      assert(dfWithDescriptionInsertTest.count() == 7)

      // Trying to insert existing rows should throw a CdpApiException
      val insertError = intercept[SparkException] {
        dfWithDescriptionInsertTest.write
          .format("cognite.spark.v1")
          .option("apiKey", writeApiKey)
          .option("type", "timeseries")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .save()
      }
      insertError.getCause shouldBe a[CdpApiException]
      val insertCdpApiException = insertError.getCause.asInstanceOf[CdpApiException]
      assert(insertCdpApiException.code == 409)
      val rowsCreatedAfterAbort = getNumberOfRowsCreated(metricsPrefix, "timeseries")
      assert(rowsCreatedAfterAbort == 7)
      assertThrows[NoSuchElementException](getNumberOfRowsUpdated(metricsPrefix, "timeseries"))
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(abortUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support partial update in savemode" taggedAs WriteTest in {
    val metricsPrefix = s"partial-update${shortRandomString()}"
    val insertDescription = s"spark-insert-test-${shortRandomString()}"
    val updateDescription = s"spark-update-test-${shortRandomString()}"
    val partialUpdateUnit = s"partial-update-${shortRandomString()}"
    val randomSuffix = shortRandomString()

    try {
      // Insert new time series test data
      spark
        .sql(s"""
                |select '$insertDescription' as description,
                |isString,
                |concat('TEST_', name) as name,
                |map("foo", null, "bar", "test") as metadata,
                |'$partialUpdateUnit' as unit,
                |NULL as assetId,
                |isStep,
                |cast(array() as array<long>) as securityCategories,
                |id,
                |concat(string(id), "_partial_$randomSuffix") as externalId,
                |createdTime,
                |lastUpdatedTime
                |from sourceTimeSeries
                |limit 5
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      val withDescriptionInsertTest = retryWhile[Int](
        (for (_ <- 1 to 5)
          yield
            spark
              .sql(
                s"""select * from destinationTimeSeries where description = '$insertDescription' and unit = '$partialUpdateUnit'""".stripMargin)
              .collect
              .length).min,
        length => length < 5
      )
      assert(withDescriptionInsertTest == 5)

      val rowsCreated = getNumberOfRowsCreated(metricsPrefix, "timeseries")
      assert(rowsCreated == 5)

      // Update data with a new description
      spark
        .sql(s"""
                |select '$updateDescription' as description,
                |id,
                |externalId,
                |map("bar", "test") as metadata,
                |name
                |from destinationTimeSeries
                |where unit = '$partialUpdateUnit'
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "update")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      val rowsUpdatedAfterUpdate = getNumberOfRowsUpdated(metricsPrefix, "timeseries")
      assert(rowsUpdatedAfterUpdate == 5)

      // Empty update. Does nothing, but should not fail
      // NULL means "not updated", we check later that description is actually $updateDescription, not None
      spark
        .sql(s"""
                |select
                |null as description,
                |id,
                |null as assetId
                |from destinationTimeSeries
                |where unit = '$partialUpdateUnit'
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "update")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      // We should not send anything when the update is empty
      val rowsUpdatedAfterEmptyUpdates = getNumberOfRowsUpdated(metricsPrefix, "timeseries")
      assert(rowsUpdatedAfterEmptyUpdates == 5)

      val dfWithDescriptionUpdateTest = retryWhile[Array[Row]](
        spark
          .sql(
            s"select * from destinationTimeSeries where unit = '$partialUpdateUnit' and description = '$updateDescription'")
          .collect,
        df => df.length < 5
      )
      assert(dfWithDescriptionUpdateTest.length == 5)

      // Trying to update non-existing Time Series should throw a CdpApiException
      val updateError = intercept[SparkException] {
        spark
          .sql(s"""
                  |select '$updateDescription' as description,
                  |isString,
                  |name,
                  |metadata,
                  |unit,
                  |assetId,
                  |isStep,
                  |securityCategories,
                  |bigint(1) as id,
                  |createdTime,
                  |lastUpdatedTime
                  |from destinationTimeSeries
                  |where unit = '$partialUpdateUnit'
     """.stripMargin)
          .write
          .format("cognite.spark.v1")
          .option("apiKey", writeApiKey)
          .option("type", "timeseries")
          .option("onconflict", "update")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .save()
      }
      updateError.getCause shouldBe a[CdpApiException]
      val updateCdpApiException = updateError.getCause.asInstanceOf[CdpApiException]
      assert(updateCdpApiException.code == 400)
      val rowsUpdatedAfterFailedUpdate = getNumberOfRowsUpdated(metricsPrefix, "timeseries")
      assert(rowsUpdatedAfterFailedUpdate == 5)
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(partialUpdateUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support upsert in savemode" taggedAs WriteTest in {
    val insertDescription = s"spark-insert-test-savemode-${shortRandomString()}"
    val upsertDescription = s"spark-upsert-test-savemode-${shortRandomString()}"
    val upsertUnit = s"upsert-save-${shortRandomString()}"
    val metricsPrefix = "timeserie.upsert.save-${shortRandomString()}"

    try {
      // Insert new time series test data
      val randomSuffix = shortRandomString()
      val defaultInsertTs = TimeSeriesInsertSchema(
        None,
        None,
        isString = false,
        Some(Map("foo" -> null, "bar" -> "test")), // scalastyle:off null
        Some(upsertUnit),
        None,
        isStep = false,
        Some(insertDescription),
        Some(Seq())
      )
      val insertData = Seq(
        defaultInsertTs.copy(name = Some("TEST_A")),
        defaultInsertTs.copy(name = Some("TEST_B"), isStep = true),
        defaultInsertTs.copy(name = Some("TEST_C"), isString = true),
        defaultInsertTs.copy(name = Some("TEST_D"), metadata = None),
        defaultInsertTs.copy(name = Some("TEST_E"))
      ).map(x => x.copy(externalId = Some(x.name.get + "_upsert_savemode_" + randomSuffix)))
      spark.sparkContext
        .parallelize(insertData)
        .toDF()
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      assert(getNumberOfRowsCreated(metricsPrefix, "timeseries") == 5)

      val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
        spark
          .sql(
            s"select * from destinationTimeSeries where unit = '$upsertUnit' and description = '$insertDescription'")
          .collect,
        df => df.length < 5
      )
      assert(dfWithDescriptionInsertTest.length == 5)

      // Test upserts
      val existingTimeSeriesDf =
        spark.sparkContext
          .parallelize(
            insertData.map(
              ts =>
                ts.copy(
                  description = Some(upsertDescription),
                  unit = Some(upsertUnit)
              ))
          )
          .toDF()

      val nonExistingTimeSeriesDf =
        spark.sparkContext
          .parallelize(
            insertData.map(
              ts =>
                ts.copy(
                  description = Some(upsertDescription),
                  name = Some(s"test-upserts-${randomSuffix}"),
                  unit = Some(upsertUnit),
                  externalId = None
              ))
          )
          .toDF()

      existingTimeSeriesDf
        .union(nonExistingTimeSeriesDf)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "upsert")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      assert(getNumberOfRowsCreated(metricsPrefix, "timeseries") == 10)
      assert(getNumberOfRowsUpdated(metricsPrefix, "timeseries") == 5)

      val dfWithDescriptionUpsertTest = retryWhile[Array[Row]](
        spark
          .sql(
            s"select * from destinationTimeSeries where unit = '$upsertUnit' and description = '$upsertDescription'")
          .collect,
        df => {
          df.length < 10
        }
      )
      assert(dfWithDescriptionUpsertTest.length == 10)
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(upsertUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val timeSeriesInsert = TimeSeriesInsertSchema()
    timeSeriesInsert.transformInto[TimeSeriesReadSchema]

    val timeSeriesUpsert = TimeSeriesUpsertSchema()
    timeSeriesUpsert.into[TimeSeriesReadSchema].withFieldComputed(_.id, tsu => tsu.id.getOrElse(0L))
  }

  it should "allow null ids on time series update" taggedAs WriteTest in {
    val updateTestUnit = s"test-null-id-${shortRandomString()}"
    val randomSuffix = shortRandomString()

    try {
      spark
        .sql(s"""
                   |select 'foo' as description,
                   |isString,
                   |name,
                   |map() as metadata,
                   |'$updateTestUnit' as unit,
                   |assetId,
                   |isStep,
                   |securityCategories,
                   |null as id,
                   |concat(string(id), "_null_id_${randomSuffix}") as externalId,
                   |createdTime,
                   |lastUpdatedTime,
                   |dataSetId
                   |from destinationTimeSeries
                   |limit 5
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationTimeSeries")

      // Check if post worked
      val timeSeriesFromTestdf = retryWhile[Array[Row]](
        spark.sql(s"select * from destinationTimeSeries where unit = '$updateTestUnit'").collect,
        df => df.length < 5
      )
      assert(timeSeriesFromTestdf.length == 5)

      // Upsert time series
      val descriptionsAfterUpdate = retryWhile[Array[Row]](
        {
          spark
            .sql(s"""
                  |select externalId,
                  |'bar' as description
                  |from destinationTimeSeries
                  |where unit = '$updateTestUnit'
     """.stripMargin)
            .write
            .format("cognite.spark.v1")
            .option("apiKey", writeApiKey)
            .option("type", "timeseries")
            .option("onconflict", "update")
            .save()
          spark
            .sql(
              s"select description from destinationTimeSeries where unit = '$updateTestUnit' and description = 'bar'")
            .collect
        },
        df => df.length < 5
      )
      assert(descriptionsAfterUpdate.length == 5)
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(updateTestUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }

  it should "allow NULL updates in savemode" taggedAs WriteTest in forAll(updateAndUpsert) {
    updateMode =>
      val testUnit = s"spark-setnull-${shortRandomString()}"
      val metricsPrefix = s"updatenull.timeseries.${shortRandomString()}"

      try {
        // Insert test event
        val df = spark
          .sql(s"""
                |select 'foo' as description,
                |false as isString,
                |'name-$testUnit' name,
                |'$testUnit' as unit,
                |false as isStep,
                |"id_$testUnit" as externalId
                """.stripMargin)
        df.write
          .format("cognite.spark.v1")
          .option("apiKey", writeApiKey)
          .option("type", "timeseries")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .option("onconflict", "abort")
          .save()

        val insertTest = retryWhile[Array[Row]](
          spark.sql(s"select * from destinationTimeSeries where unit = '$testUnit'").collect,
          df => df.length != 1
        )
        insertTest(0).getAs[Long]("id")

        spark
          .sql(s"""
                |select NULL as description,
                |"id_$testUnit" as externalId,
                |"$testUnit" as unit
     """.stripMargin)
          .write
          .format("cognite.spark.v1")
          .option("apiKey", writeApiKey)
          .option("type", "timeseries")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .option("ignoreNullFields", "false")
          .option("onconflict", updateMode)
          .save()

        val updateTest = retryWhile[Array[Row]](
          spark
            .sql(s"select * from destinationTimeSeries where unit = '$testUnit' and description is null")
            .collect,
          df => df.length != 1
        )
        updateTest.length shouldBe 1
        val Array(updatedRow) = updateTest
        val updatedTs = SparkSchemaHelper.fromRow[TimeSeriesReadSchema](updatedRow)
        updatedTs.assetId shouldBe None
        updatedTs.unit shouldBe Some(testUnit)
        updatedTs.isStep shouldBe false
        updatedTs.externalId shouldBe Some("id_" + testUnit)
        updatedTs.description shouldBe None
        updatedTs.name shouldBe Some("name-" + testUnit)
      } finally {
        try {
          cleanUpTimeSeriesTestDataByUnit(testUnit)
        } catch {
          case NonFatal(_) => // ignore
        }
      }
  }

  it should "support ignoring unknown ids in deletes" in {
    spark
      .sql(s"""
              |select 1234567890123 as id
        """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "delete")
      .option("ignoreUnknownIds", "true")
      .save()

    // Should throw error if ignoreUnknownIds is false
    val e = intercept[SparkException] {
      spark
        .sql(s"""
                |select 1234567890123 as id
        """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "delete")
        .option("ignoreUnknownIds", "false")
        .save()
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  it should "support deletes by externalIds" in {
    val deleteUnit = s"unit-delete-externalIds-${shortRandomString()}"
    val description = s"no name ${shortRandomString()}"

    val randomSuffix = shortRandomString()
    try {
      // Insert some test data
      spark
        .sql(s"""
                |select '$description' as description,
                |null as name,
                |isString,
                |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
                |'$deleteUnit' as unit,
                |null as assetId,
                |isStep,
                |cast(array() as array<long>) as securityCategories,
                |id,
                |concat(string(id), '${randomSuffix}') as externalId,
                |createdTime,
                |lastUpdatedTime,
                |$testDataSetId as dataSetId
                |from sourceTimeSeries
                |limit 10
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationTimeSeries")

      // Check if insert worked
      val idsAfterInsert =
        retryWhile[Array[Row]](
          spark
            .sql(s"select externalId from destinationTimeSeries where unit = '$deleteUnit'")
            .collect,
          df => df.length < 10)
      assert(idsAfterInsert.length == 10)

      val metricsPrefix = s"timeseries.delete.${shortRandomString()}"
      // Delete the data
      val idsAfterDelete =
        retryWhile[Array[Row]](
          {
            spark
              .sql(s"select externalId from destinationTimeSeries where unit = '$deleteUnit'")
              .write
              .format("cognite.spark.v1")
              .option("apiKey", writeApiKey)
              .option("type", "timeseries")
              .option("onconflict", "delete")
              .option("collectMetrics", "true")
              .option("metricsPrefix", metricsPrefix)
              .save()
            spark
              .sql(s"select externalId from destinationTimeSeries where unit = '$deleteUnit'")
              .collect
          },
          df => df.length > 0
        )
      assert(idsAfterDelete.isEmpty)
      // Due to retries, rows deleted may exceed 100
      val numberDeleteds = getNumberOfRowsDeleted(metricsPrefix, "timeseries")
      numberDeleteds should be >= 10L
    } finally {
      try {
        cleanUpTimeSeriesTestDataByUnit(deleteUnit)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  def cleanUpTimeSeriesTestDataByUnit(unit: String): Unit =
    spark
      .sql(s"""select * from destinationTimeSeries where unit = '$unit'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "delete")
      .save()
}
