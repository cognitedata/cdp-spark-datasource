package com.cognite.spark.datasource

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec
import org.apache.spark.sql.functions.col
import com.softwaremill.sttp._

class TimeSeriesUpsertTest extends FlatSpec with SparkTest {
  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  it should "successfully both update and insert time series" taggedAs WriteTest in {
    val initialDescription = "'post testing'"
    val updatedDescription = "'upsert testing'"

    val sourceDf = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    sourceDf.createOrReplaceTempView("sourceTimeSeries")

    // Clean up any old test data
    val namesDf =
      spark.sql(s"""select name from sourceTimeSeries where unit = 'test data'""")
    val namesList = namesDf.rdd.map(r => r.getAs[String](0)).collect().toList
    cleanupTimeSeries(namesList)

    val testTimeSeriesAfterCleanup = retryWhile[DataFrame](
      spark.sql(s"""select * from sourceTimeSeries where unit = 'test data'"""),
      df => df.count > 0)
    assert(testTimeSeriesAfterCleanup.count == 0)

    // Insert new time series test data
    spark
      .sql(s"""
         |select $initialDescription as description,
         |concat('TEST', name) as name,
         |isString,
         |metadata,
         |'test data' as unit,
         |'' as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |id+1 as id,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |where unit = 'publicdata'
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("sourceTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[DataFrame](
      spark.sql(s"""select * from sourceTimeSeries where description = $initialDescription"""),
      df => df.count < 5)
    assert(initialDescriptionsAfterPost.count == 5)

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
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |where description = $initialDescription
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("sourceTimeSeries")

    // Check if upsert worked
    val updatedDescriptionsAfterUpsert = retryWhile[DataFrame](
      spark.sql(s"""select * from sourceTimeSeries where description = $updatedDescription"""),
      df => df.count < 5)
    assert(updatedDescriptionsAfterUpsert.count == 5)

    val initialDescriptionsAfterUpsert = retryWhile[DataFrame](
      spark.sql(s"""select * from sourceTimeSeries where description = $initialDescription"""),
      df => df.count > 0)
    assert(initialDescriptionsAfterUpsert.count == 0)
  }

  def cleanupTimeSeries(names: List[String]): Unit = {
    val project = getProject(writeApiKey, Constants.DefaultMaxRetries, Constants.DefaultBaseUrl)

    for (name <- names) {
      delete(
        writeApiKey,
        uri"${Constants.DefaultBaseUrl}/api/0.5/projects/$project/timeseries/$name",
        maxRetries = 5
      ).unsafeRunSync()
    }
  }
}
