package cognite.spark.v1

import java.util.UUID

import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.TimeSeries
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers, OptionValues}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import com.softwaremill.sttp._
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._

import scala.util.Try

class TimeSeriesRelationTest extends FlatSpec with Matchers with SparkTest with OptionValues {
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

  val destinationDfWithLegacyName = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "timeseries")
    .option("useLegacyName", true)
    .load()
  destinationDfWithLegacyName.createOrReplaceTempView("destinationTimeSeriesWithLegacyName")

  val destinationDfWithLegacyNameFromExternalId = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "timeseries")
    .option("useLegacyName", "externalId")
    .load()
  destinationDfWithLegacyNameFromExternalId.createOrReplaceTempView(
    "destinationTimeSeriesWithLegacyNameFromExternalId")

  val destinationDfWithLegacyNameFromExternalIdOnConflictUpsert = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "timeseries")
    .option("onconflict", "upsert")
    .option("useLegacyName", "externalId")
    .load()
  destinationDfWithLegacyNameFromExternalIdOnConflictUpsert.createOrReplaceTempView(
    "destinationTimeSeriesWithLegacyNameFromExternalIdOnConflictUpsert")

  it should "read all data regardless of the number of partitions" taggedAs ReadTest in {
    val dfreader = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
    for (np <- Seq(1, 4, 8, 12)){
      val df = dfreader.option("partitions", np.toString).load()
      assert(df.count == 363)
    }
  }

  it should "handle pushdown filters on assetId with multiple assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "1")
      .load()
      .where(s"assetId In(6191827428964450, 3424990723231138, 3047932288982463)")
    assert(df.count == 87)
    val timeSeriesRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(timeSeriesRead == 87)
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
      .where("createdTime < to_timestamp(1580000000)")
      .where(s"assetId In(6191827428964450, 3424990723231138, 3047932288982463) or dataSetId in (1, 2, 3)")
    assert(df.count == 87)
    val timeSeriesRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(timeSeriesRead == 87)
  }


  it should "handle pushdown filters on assetId on nonexisting assetId" taggedAs WriteTest in {
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

  it should "insert a time series with no name" taggedAs WriteTest in {
    val insertNoNameUnit = s"insert-no-name-${shortRandomString()}"
    cleanUpTimeSeriesTestDataByUnit(insertNoNameUnit)
    val description = s"no name ${shortRandomString()}"
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
        .sql(s"""select * from destinationTimeSeries where description = '$description'""")
        .collect,
      df => df.length != 1)
    assert(dfAfterPost.length == 1)
    assert(dfAfterPost.head.get(0) == null)
    dfAfterPost.head.getAs[Long]("dataSetId") shouldBe testDataSetId
  }

  it should "create time series with legacyName based on name if useLegacyName option is 'true'" taggedAs WriteTest in {
    val legacyNameUnit = s"legacy-name-${shortRandomString()}"
    implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()
    val project = writeClient.login.status.project
    val legacyName1 = shortRandomString()
    val externalId1 = shortRandomString()
    val externalId2 = shortRandomString()
    val before = the[CdpApiException] thrownBy writeClient.timeSeries.retrieveByExternalId(legacyName1)
    before.code shouldBe 400
    val before05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${legacyName1}")
      .header("api-key", writeApiKey)
      .contentType("application/json")
      .acceptEncoding("application/json")
      .send()
    before05.code shouldEqual 404
    spark
      .sql(s"""
              |select null as description,
              |'$legacyName1' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$externalId1' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime,
              |null as dataSetId
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyName")

    val after = writeClient.timeSeries.retrieveByExternalId(externalId1)
    after.externalId.get shouldBe externalId1

    val after05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${legacyName1}")
      .header("api-key", writeApiKey)
      .contentType("application/json")
      .acceptEncoding("application/json")
      .send()
    after05.code shouldEqual 200

    // Attempting to insert the same name should be an error when legacyName is set.
    // Note that we use a different externalId.
    disableSparkLogging() // Removing expected Spark executor Errors from the console
    val exception = the[SparkException] thrownBy spark
      .sql(s"""
              |select null as description,
              |'$legacyName1' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$externalId2' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime,
              |null as dataSetId
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyName")
    enableSparkLogging()
    assert(exception.getCause.isInstanceOf[IllegalArgumentException])

    val before2 = the[CdpApiException] thrownBy writeClient.timeSeries.retrieveByExternalId(externalId2)
    before2.code shouldBe 400

    // inserting without useLegacyName should work.
    spark
      .sql(s"""
              |select null as description,
              |'$legacyName1' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$externalId2' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime,
              |null as dataSetId
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    val after2 = writeClient.timeSeries.retrieveByExternalId(externalId1)
    after2.externalId.get shouldBe externalId1

    writeClient.timeSeries.deleteByExternalIds(Seq(externalId1, externalId2))
  }

  it should "create time series with legacyName from externalId if useLegacyName option is 'externalId'" taggedAs WriteTest in {
    val legacyNameUnit = s"legacy-name-${shortRandomString()}"
    implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()
    val project = writeClient.login.status.project
    val name = shortRandomString()
    val externalId = shortRandomString()
    val before = the[CdpApiException] thrownBy writeClient.timeSeries.retrieveByExternalId(externalId)
    before.code shouldBe 400
    val before05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${externalId}")
      .header("api-key", writeApiKey)
      .contentType("application/json")
      .acceptEncoding("application/json")
      .send()
    before05.code shouldEqual 404
    spark
      .sql(s"""
              |select null as description,
              |'$name' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$externalId' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime,
              |null as dataSetId
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyNameFromExternalId")

    val after = writeClient.timeSeries.retrieveByExternalId(externalId)
    after.externalId.get shouldBe externalId

    val after05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${externalId}")
      .header("api-key", writeApiKey)
      .contentType("application/json")
      .acceptEncoding("application/json")
      .send()
    after05.code shouldEqual 200

    writeClient.timeSeries.deleteByExternalIds(Seq(externalId))
  }

  it should "upsert time series that conflict on legacyName when useLegacyName = externalId" in {
    val testUnit = s"legacy-name-upsert-${shortRandomString()}"
    cleanUpTimeSeriesTestDataByUnit(testUnit)
    retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where unit = '$testUnit'").collect,
      rows => rows.length > 0
    )

    val externalId = shortRandomString()

    spark
      .sql(s"""
              |select null as description,
              |'beforeUpsert' as name,
              |false as isString,
              |null as metadata,
              |'$testUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$externalId' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime,
              |null as dataSetId
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyNameFromExternalId")

    val beforeUpsert = writeClient.timeSeries.retrieveByExternalId(externalId)
    beforeUpsert.name.value shouldBe "beforeUpsert"

    spark
      .sql(s"""
              |select null as description,
              |'afterUpsert' as name,
              |false as isString,
              |null as metadata,
              |'$testUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$externalId' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime,
              |null as dataSetId
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyNameFromExternalIdOnConflictUpsert")

    val afterUpsert = writeClient.timeSeries.retrieveByExternalId(externalId)
    afterUpsert.name.value shouldBe "afterUpsert"
  }

  it should "correctly parse useLegacyName options" in {
    LegacyNameSource.fromSparkOption(None) shouldBe LegacyNameSource.None
    LegacyNameSource.fromSparkOption(Some("false")) shouldBe LegacyNameSource.None

    LegacyNameSource.fromSparkOption(Some("true")) shouldBe LegacyNameSource.Name
    LegacyNameSource.fromSparkOption(Some("name")) shouldBe LegacyNameSource.Name

    LegacyNameSource.fromSparkOption(Some("externalId")) shouldBe LegacyNameSource.ExternalId
    LegacyNameSource.fromSparkOption(Some("ExternalID")) shouldBe LegacyNameSource.ExternalId

    assertThrows[IllegalArgumentException] {
      LegacyNameSource.fromSparkOption(Some("bogus"))
    }
  }

  it should "successfully both update and insert time series" taggedAs WriteTest in {
    val metricsPrefix = s"update-and-insert-${shortRandomString()}"
    val initialDescription = s"post-testing-${shortRandomString()}"
    val updatedDescription = s"upsert-testing-${shortRandomString()}"
    val insertTestUnit = s"insert-test${shortRandomString()}"

    cleanUpTimeSeriesTestDataByUnit(insertTestUnit)

    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    df.createOrReplaceTempView("destinationTimeSeriesInsertAndUpdate")

    a [NoSuchElementException] should be thrownBy getNumberOfRowsCreated(metricsPrefix, "timeseries")
    a [NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "timeseries")

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

    a [NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "timeseries")
    val rowsCreated = getNumberOfRowsCreated(metricsPrefix, "timeseries")
    assert(rowsCreated == 5)

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Int](
      (for (_ <- 1 to 5)
        yield spark
          .sql(s"""select * from destinationTimeSeriesInsertAndUpdate where description = '$initialDescription'""")
          .collect
          .length).min,
      length => length < 5)
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
        .sql(s"""select * from destinationTimeSeriesInsertAndUpdate where description = '$updatedDescription'""")
        .collect,
      df => df.length < 5
    )
    assert(updatedDescriptionsAfterUpdate.length == 5)

    val initialDescriptionsAfterUpdate = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationTimeSeriesInsertAndUpdate where description = '$initialDescription'""")
        .collect,
      df => df.length > 0)
    assert(initialDescriptionsAfterUpdate.length == 0)
  }

  it should "support abort in savemode" taggedAs WriteTest in {
    val metricsPrefix = s"abort-savemode-${shortRandomString()}"
    val abortDescription = s"spark-test-abort-${shortRandomString()}"
    val abortUnit = s"insert-abort-${shortRandomString()}"
    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(abortUnit)

    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$abortUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

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

    a [NoSuchElementException] should be thrownBy getNumberOfRowsUpdated(metricsPrefix, "timeseries")
    val rowsCreated = getNumberOfRowsCreated(metricsPrefix, "timeseries")
    assert(rowsCreated == 7)

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$abortDescription'").collect,
      df => df.length < 7
    )
    assert(dfWithDescriptionInsertTest.length == 7)

    // Trying to insert existing rows should throw a CdpApiException
    disableSparkLogging() // Removing expected Spark executor Errors from the console
    val insertError = intercept[SparkException] {
      spark
        .sql(s"""
           |select description,
           |isString,
           |name,
           |metadata,
           |unit,
           |assetId,
           |isStep,
           |securityCategories,
           |id,
           |externalId,
           |createdTime,
           |lastUpdatedTime
           |from destinationTimeSeries
           |where unit = '$abortUnit'
     """.stripMargin)
        .write
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
    enableSparkLogging()
  }

  it should "support partial update in savemode" taggedAs WriteTest in {
    val metricsPrefix = s"partial-update${shortRandomString()}"
    val insertDescription = s"spark-insert-test-${shortRandomString()}"
    val updateDescription = s"spark-update-test-${shortRandomString()}"
    val partialUpdateUnit = s"partial-update-${shortRandomString()}"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(partialUpdateUnit)
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$partialUpdateUnit'""").collect
    }, df => df.length > 0
    )
    assert(testTimeSeriesAfterCleanup.length == 0)

    val randomSuffix = shortRandomString()
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
        yield spark
          .sql(s"""select * from destinationTimeSeries where description = '$insertDescription' and unit = '$partialUpdateUnit'""".stripMargin)
          .collect
          .length).min,
      length => length < 5)
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
          .sql(s"select * from destinationTimeSeries where description = '$updateDescription'")
          .collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionUpdateTest.length == 5)

    // Trying to update non-existing Time Series should throw a CdpApiException
    disableSparkLogging() // Removing expected Spark executor Errors from the console
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
    enableSparkLogging()
    val rowsUpdatedAfterFailedUpdate = getNumberOfRowsUpdated(metricsPrefix, "timeseries")
    assert(rowsUpdatedAfterFailedUpdate == 5)
  }

  it should "support upsert in savemode" taggedAs WriteTest in {
    val insertDescription = s"spark-insert-test-savemode-${shortRandomString()}"
    val upsertDescription = s"spark-upsert-test-savemode-${shortRandomString()}"
    val upsertUnit = s"upsert-save-${shortRandomString()}"

    val metricsPrefix = "timeserie.test.upsert.save"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(upsertUnit)
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$upsertUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    val randomSuffix = shortRandomString()
    val defaultInsertTs = TimeSeriesUpsertSchema(
      None,
      None,
      None,
      Some(Map("foo" -> null, "bar" -> "test")), // scalastyle:off null
      Some(upsertUnit),
      None,
      Some(insertDescription),
      Some(Seq()),
      isStep = Some(false),
      isString = Some(false))
    val insertData = Seq(
      defaultInsertTs.copy(name = Some("TEST_A")),
      defaultInsertTs.copy(name = Some("TEST_B"), isStep = Some(true)),
      defaultInsertTs.copy(name = Some("TEST_C"), isString = Some(true)),
      defaultInsertTs.copy(name = Some("TEST_D"), metadata = None),
      defaultInsertTs.copy(name = Some("TEST_E"))
    ).map(x => x.copy(externalId = Some(x.name.get + "_upsert_savemode_" + randomSuffix)))
    spark
      .sparkContext.parallelize(insertData).toDF()
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    assert(getNumberOfRowsCreated(metricsPrefix, "timeseries") == 5)

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$insertDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Test upserts
    val existingTimeSeriesDf =
      spark.sparkContext.parallelize(
        insertData.map(ts =>
          ts.copy(
            description = Some(upsertDescription),
            unit = Some(upsertUnit + "-ex")
          )
        )
      ).toDF()

    val nonExistingTimeSeriesDf =
      spark.sparkContext.parallelize(
        insertData.map(ts =>
          ts.copy(
            description = Some(upsertDescription),
            name = Some("test-upserts"),
            unit = Some(upsertUnit + "-non"),
            externalId = None,
            id = None
          )
        )
      ).toDF()

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
      spark.sql(s"select * from destinationTimeSeries where description = '$upsertDescription'").collect,
      df => {
        df.length < 10
      }
    )
    assert(dfWithDescriptionUpsertTest.length == 10)

    // Attempting to upsert (insert in this case) multiple time series
    // with the same name should be an error when useLegacyName is true.
    disableSparkLogging()
    val exception = the[SparkException] thrownBy nonExistingTimeSeriesDf
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .option("useLegacyName", "true")
      .save()
    assert(exception.getCause.isInstanceOf[IllegalArgumentException])
    enableSparkLogging()

    val nonExistingTimeSeriesWithLegacyNameDf =
      spark.sql(
        s"""
           |select '$upsertDescription' as description,
           |isString,
           |concat('test-upserts-${shortRandomString()}', id) as name,
           |map("foo", null, "bar", "test") as metadata,
           |'${upsertUnit + "-non"}' as unit,
           |assetId,
           |securityCategories,
           |null as id,
           |null as externalId,
           |createdTime,
           |lastUpdatedTime
           |from destinationTimeSeries
           |where unit = '$upsertUnit'
     """.stripMargin)
        .cache()

    implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()
    val project = writeClient.login.status.project
    val legacyNamesToCreate = nonExistingTimeSeriesWithLegacyNameDf.select("name").collect().map(_.getString(0))
    for (legacyName <- legacyNamesToCreate) {
      val before05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${legacyName}")
        .header("api-key", writeApiKey)
        .contentType("application/json")
        .acceptEncoding("application/json")
        .send()
      before05.code shouldEqual 404
    }

    nonExistingTimeSeriesWithLegacyNameDf
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .option("useLegacyName", "true")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    for (legacyName <- legacyNamesToCreate) {
      val after05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${legacyName}")
        .header("api-key", writeApiKey)
        .contentType("application/json")
        .acceptEncoding("application/json")
        .send()
      after05.code shouldEqual 200
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
    cleanUpTimeSeriesTestDataByUnit(updateTestUnit)
    retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where unit = '$updateTestUnit'").collect,
      rows => rows.length > 0
    )

    spark.sql(s"""
                 |select 'foo' as description,
                 |isString,
                 |name,
                 |map() as metadata,
                 |'$updateTestUnit' as unit,
                 |assetId,
                 |isStep,
                 |securityCategories,
                 |null as id,
                 |string(id) as externalId,
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


    // Check if upsert worked
    val descriptionsAfterUpdate = retryWhile[Array[Row]](
      spark
        .sql(
          s"select description from destinationTimeSeries where unit = '$updateTestUnit' and description = 'bar'")
        .collect,
      df => df.length < 5)
    assert(descriptionsAfterUpdate.length == 5)
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
    disableSparkLogging() // Removing expected Spark executor Errors from the console
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
    enableSparkLogging()
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  def cleanUpTimeSeriesTestDataByUnit(unit: String): Unit = {
    spark.sql(s"""select * from destinationTimeSeries where unit = '$unit'""")
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "delete")
        .save()
  }
}
