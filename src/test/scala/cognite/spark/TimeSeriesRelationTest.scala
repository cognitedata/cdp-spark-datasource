package cognite.spark

import com.cognite.sdk.scala.common.{ApiKeyAuth, CdpApiException}
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException

class TimeSeriesRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_READ"))
  val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  val sourceDf = spark.read
    .format("cognite.spark")
    .option("apiKey", readApiKey.apiKey)
    .option("type", "timeseries")
    .load()
  sourceDf.createOrReplaceTempView("sourceTimeSeries")

  val destinationDf = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .load()
  destinationDf.createOrReplaceTempView("destinationTimeSeries")

  val testDataUnit = "time-series-test-data"

  it should "handle pushdown filters on assetId with multiple assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"assetId In(6191827428964450, 3424990723231138, 3047932288982463)")
    assert(df.count == 87)
    val timeSeriesRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(timeSeriesRead == 87)
  }

  it should "handle pushdown filters on assetId on nonexisting assetId" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds.nonexisting"
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", readApiKey.apiKey)
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
    cleanUpTimeSeriesTestDataByUnit(testDataUnit)
    val description = "no name"
    // Insert new time series test data
    spark
      .sql(s"""
              |select '$description' as description,
              |null as name,
              |isString,
              |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
              |'$testDataUnit' as unit,
              |null as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |'no-name-time-series' as externalId,
              |createdTime,
              |lastUpdatedTime
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
  }

  it should "successfully both update and insert time series" taggedAs WriteTest in {
    val initialDescription = "post testing"
    val updatedDescription = "upsert testing"

    cleanUpTimeSeriesTestDataByUnit(testDataUnit)

    // Insert new time series test data
    spark
      .sql(s"""
         |select '$initialDescription' as description,
         |concat('TEST', name) as name,
         |isString,
         |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
         |'$testDataUnit' as unit,
         |null as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |id,
         |id as externalId,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |limit 5
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationTimeSeries where description = '$initialDescription'""")
        .collect,
      df => df.length < 5)
    assert(initialDescriptionsAfterPost.length == 5)

    // Upsert time series data, but wait 5 seconds to make sure
    Thread.sleep(5000)
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
            |lastUpdatedTime
            |from destinationTimeSeries
            |where description = '$initialDescription'
          """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if upsert worked
    val updatedDescriptionsAfterUpsert = retryWhile[Array[Row]](
        spark
        .sql(s"""select * from destinationTimeSeries where description = '$updatedDescription'""")
        .collect,
      df => df.length < 5
    )
    assert(updatedDescriptionsAfterUpsert.length == 5)

    val initialDescriptionsAfterUpsert = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationTimeSeries where description = '$initialDescription'""")
        .collect,
      df => df.length > 0)
    assert(initialDescriptionsAfterUpsert.length == 0)
  }

  it should "support abort in savemode" taggedAs WriteTest in {
    val insertDescription = "spark-insert-test"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(testDataUnit)

    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$testDataUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
         |select '$insertDescription' as description,
         |isString,
         |concat('TEST_', name) as name,
         |map("foo", null, "bar", "test") as metadata,
         |'$testDataUnit' as unit,
         |NULL as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |id,
         |string(id) as externalId,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |limit 5
     """.stripMargin)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$insertDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Trying to insert existing rows should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
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
           |where unit = '$testDataUnit'
     """.stripMargin)
        .write
        .format("cognite.spark")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "timeseries")
        .save()
    }
    insertError.getCause shouldBe a[CdpApiException]
    val insertCdpApiException = insertError.getCause.asInstanceOf[CdpApiException]
    assert(insertCdpApiException.code == 409)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "support partial update in savemode" taggedAs WriteTest in {
    val insertDescription = "spark-insert-test"
    val updateDescription = "spark-update-test"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(testDataUnit)
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$testDataUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
              |select '$insertDescription' as description,
              |isString,
              |concat('TEST_', name) as name,
              |map("foo", null, "bar", "test") as metadata,
              |'$testDataUnit' as unit,
              |NULL as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |string(id) as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |limit 5
     """.stripMargin)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$insertDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Update data with a new description, but wait 5 seconds to make sure
    Thread.sleep(5000)
    spark
      .sql(s"""
              |select '$updateDescription' as description,
              |id,
              |map("bar", "test") as metadata,
              |name
              |from destinationTimeSeries
              |where unit = '$testDataUnit'
     """.stripMargin)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .option("onconflict", "update")
      .save()

    val dfWithDescriptionUpdateTest = retryWhile[Array[Row]](
      {
        spark
          .sql(s"select * from destinationTimeSeries where description = '$updateDescription'")
          .collect
      },
      df => df.length < 5
    )
    assert(dfWithDescriptionUpdateTest.length == 5)

    // Trying to update nonexisting Time Series should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
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
                   |where unit = '$testDataUnit'
     """.stripMargin)
        .write
        .format("cognite.spark")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "timeseries")
        .option("onconflict", "update")
        .save()
    }
    updateError.getCause shouldBe a[CdpApiException]
    val updateCdpApiException = updateError.getCause.asInstanceOf[CdpApiException]
    assert(updateCdpApiException.code == 400)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "support upsert in savemode" taggedAs WriteTest in {
    val insertDescription = "spark-insert-test"
    val upsertDescription = "spark-upsert-test"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(testDataUnit)
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$testDataUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
              |select '$insertDescription' as description,
              |isString,
              |concat('TEST_', name) as name,
              |map("foo", null, "bar", "test") as metadata,
              |'$testDataUnit' as unit,
              |NULL as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |string(id) as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |limit 5
     """.stripMargin)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$insertDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Test upserts
    val existingTimeSeriesDf =
      spark.sql(s"""
              |select '$upsertDescription' as description,
              |isString,
              |name,
              |metadata,
              |unit,
              |assetId,
              |isStep,
              |securityCategories,
              |id,
              |string(id) as externalId,
              |createdTime,
              |lastUpdatedTime
              |from destinationTimeSeries
              |where unit = '$testDataUnit'
     """.stripMargin)

    val nonExistingTimeSeriesDf =
      spark.sql(s"""
             |select '$upsertDescription' as description,
             |isString,
             |concat('UPSERTS_', name) as name,
             |map("foo", null, "bar", "test") as metadata,
             |unit,
             |assetId,
             |isStep,
             |securityCategories,
             |id + 10,
             |string(id + 10) as externalId,
             |createdTime,
             |lastUpdatedTime
             |from destinationTimeSeries
             |where unit = '$testDataUnit'
     """.stripMargin)

    existingTimeSeriesDf
      .union(nonExistingTimeSeriesDf)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .save()

    val dfWithDescriptionUpsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$upsertDescription'").collect,
      df => df.length < 10
    )
    assert(dfWithDescriptionUpsertTest.length == 10)
  }

  it should "check for null ids on time series update" taggedAs WriteTest in {
    val initialDescription = "post testing"
    val testUnit = "test data"

    // Clean up any old test data
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      cleanUpTimeSeriesTestDataByUnit(testUnit)
      spark.sql(s"""select * from destinationTimeSeries where unit = '$testUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    val wdf = spark
      .sql(s"""
         |select '$initialDescription' as description,
         |concat('TEST', name) as name,
         |isString,
         |metadata,
         |'$testUnit' as unit,
         |assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |null as id,
         |string(id) as externalId,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |limit 50
     """.stripMargin)

    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console

    val e = intercept[SparkException] {
      wdf.write
        .format("cognite.spark")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "timeseries")
        .option("onconflict", "update")
        .save()
    }
    e.getCause shouldBe a[IllegalArgumentException]
    spark.sparkContext.setLogLevel("WARN")
  }

  def cleanUpTimeSeriesTestDataByUnit(unit: String): Unit = {
    Thread.sleep(2000)
    spark.sql(s"""select * from destinationTimeSeries where unit = '$unit'""")
        .write
        .format("cognite.spark")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "timeseries")
        .option("onconflict", "delete")
        .save()
  }
}
