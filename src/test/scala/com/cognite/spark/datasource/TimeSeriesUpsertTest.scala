package com.cognite.spark.datasource

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions.col
import com.softwaremill.sttp._
import org.apache.spark.SparkException

class TimeSeriesUpsertTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_READ"))
  val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))
  val sourceDf = spark.read
    .format("com.cognite.spark.datasource")
    .option("apiKey", writeApiKey.apiKey)
    .option("type", "timeseries")
    .load()
  sourceDf.createOrReplaceTempView("sourceTimeSeries")

  it should "successfully both update and insert time series" taggedAs WriteTest in {
    val initialDescription = "'post testing'"
    val updatedDescription = "'upsert testing'"
    val testUnit = "test data"

    // Clean up any old test data
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      cleanupTestData(testUnit)
      spark.sql(s"""select * from sourceTimeSeries where unit = '$testUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
         |select $initialDescription as description,
         |concat('TEST', name) as name,
         |isString,
         |metadata,
         |'$testUnit' as unit,
         |'' as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |null as id,
         |null as createdTime,
         |null as lastUpdatedTime
         |from sourceTimeSeries
         |where unit = 'publicdata'
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("sourceTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark.sql(s"""select * from sourceTimeSeries where description = $initialDescription""").collect,
      df => df.length < 5)
    assert(initialDescriptionsAfterPost.length == 5)

    val updatedDescriptionsAfterUpsert = retryWhile[Array[Row]]({
      // Upsert time series data
      spark
        .sql(s"""
                |select $updatedDescription as description,
                |name,
                |isString,
                |metadata,
                |'test data' as unit,
                |'' as assetId,
                |isStep,
                |securityCategories,
                |id,
                |null as createdTime,
                |lastUpdatedTime
                |from sourceTimeSeries
                |where description = $initialDescription
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("sourceTimeSeries")
      // Check if upsert worked
      spark.sql(s"""select * from sourceTimeSeries where description = $updatedDescription""").collect
    }, df => df.length < 5)
    assert(updatedDescriptionsAfterUpsert.length == 5)

    val initialDescriptionsAfterUpsert = retryWhile[Array[Row]](
      spark.sql(s"""select * from sourceTimeSeries where description = $initialDescription""").collect,
      df => df.length > 0)
    assert(initialDescriptionsAfterUpsert.length == 0)

    cleanupTestData(testUnit)
  }

  it should "support abort in savemode" taggedAs WriteTest in {
    val insertDescription = "spark-insert-test"
    val saveModeUnit = "spark-savemode-test"

    // Clean up any old test data
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      cleanupTestData(saveModeUnit)
      spark.sql(s"""select * from sourceTimeSeries where unit = '$saveModeUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark.sql(
      s"""
         |select '$insertDescription' as description,
         |isString,
         |concat('TEST_', name) as name,
         |metadata,
         |'$saveModeUnit' as unit,
         |NULL as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |where unit = 'publicdata'
     """.stripMargin)
      .write
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from sourceTimeSeries where description = '$insertDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Trying to insert existing rows should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val insertError = intercept[SparkException] {
      spark.sql(
        s"""
           |select description,
           |isString,
           |name,
           |metadata,
           |unit,
           |assetId,
           |isStep,
           |securityCategories,
           |createdTime,
           |lastUpdatedTime
           |from sourceTimeSeries
           |where unit = '$saveModeUnit'
     """.stripMargin)
        .write
        .format("com.cognite.spark.datasource")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "timeseries")
        .save()
    }
    insertError.getCause shouldBe a[CdpApiException]
    val insertCdpApiException = insertError.getCause.asInstanceOf[CdpApiException]
    assert(insertCdpApiException.code == 400)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "support partial update in savemode" taggedAs WriteTest in {
    val updateDescription = "spark-update-test"
    val saveModeUnit = "spark-savemode-test"

    val dfWithDescriptionUpdateTest = retryWhile[Array[Row]]({
      // Update data with a new description
      spark.sql(
        s"""
           |select '$updateDescription' as description,
           |id,
           |name
           |from sourceTimeSeries
           |where unit = '$saveModeUnit'
     """.stripMargin)
        .write
        .format("com.cognite.spark.datasource")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "timeseries")
        .option("onconflict", "update")
        .save()
      spark.sql(s"select * from sourceTimeSeries where description = '$updateDescription'").collect
    }, df => df.length < 5)
    assert(dfWithDescriptionUpdateTest.length == 5)

    // Trying to update nonexisting Time Series should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val updateError = intercept[SparkException] {
      spark.sql(s"""
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
                   |from sourceTimeSeries
                   |where unit = '$saveModeUnit'
     """.stripMargin)
        .write
        .format("com.cognite.spark.datasource")
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
    val upsertDescription = "spark-upsert-test"
    val saveModeUnit = "spark-savemode-test"
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
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |where unit = '$saveModeUnit'
     """.stripMargin)

  val nonExistingTimeSeriesDf =
    spark.sql(s"""
             |select '$upsertDescription' as description,
             |isString,
             |concat('UPSERTS_', name) as name,
             |metadata,
             |unit,
             |assetId,
             |isStep,
             |securityCategories,
             |id + 10,
             |createdTime,
             |lastUpdatedTime
             |from sourceTimeSeries
             |where unit = '$saveModeUnit'
     """.stripMargin)


    existingTimeSeriesDf.union(nonExistingTimeSeriesDf)
      .write
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .save()

    val dfWithDescriptionUpsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from sourceTimeSeries where description = '$upsertDescription'").collect,
      df => df.length < 10
    )
    assert(dfWithDescriptionUpsertTest.length == 10)

    cleanupTestData(saveModeUnit)
  }

  def cleanupTestData(testName: String): Unit = {
    val namesDf =
      spark.sql(s"""select name from sourceTimeSeries where unit = '$testName'""")
    cleanupTimeSeries(namesDf.rdd.map(r => r.getAs[String](0)).collect())
  }

  def cleanupTimeSeries(names: Array[String]): Unit = {
    val config = getDefaultConfig(writeApiKey)
    for (name <- names) {
      try {
        delete(
          config,
          uri"${Constants.DefaultBaseUrl}/api/0.5/projects/${config.project}/timeseries/$name"
        ).unsafeRunSync()
      } catch {
        case CdpApiException(_, 404, _) => // ignore exceptions about already deleted items
      }
    }
  }
}
