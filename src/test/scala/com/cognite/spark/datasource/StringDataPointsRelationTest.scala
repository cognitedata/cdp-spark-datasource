package com.cognite.spark.datasource

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.scalatest.{FlatSpec, Matchers}

class StringDataPointsRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  val valhallTimeSeries = "'VAL_23-PIC-96153:MODE'"

  "StringDataPointsRelation" should "use our own schema for data points" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .load()
      .where(s"name = $valhallTimeSeries")
    assert(df.schema.length == 3)

    assert(df.schema.fields.sameElements(Array(
      StructField("name", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("value", StringType, nullable = false))))
  }

  it should "fetch all data we expect" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .load()
      .where(s"timestamp >= 1395666380600 and timestamp <= 1552604342000 and name = $valhallTimeSeries")
    assert(df.count() == 1925)

    val df2 = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .load()
      .where(s"timestamp <= 1552604342000 and name = $valhallTimeSeries")
    assert(df2.count() == 1925)
  }

  it should "iterate over period longer than limit" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "40")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp > 0 and timestamp < 1790902000001 and name = $valhallTimeSeries")
    assert(df.count() == 100)
  }

  it should "handle initial data set below batch size" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"name = $valhallTimeSeries")
    assert(df.count() == 100)
  }

  it should "apply limit to each partition" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("partitions", "2")
      .load()
      .where(s"timestamp >= 1395666380607 and timestamp <= 1557485862500 and name = $valhallTimeSeries")
    assert(df.count() == 200)

    val df2 = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("partitions", "3")
      .load()
      .where(s"timestamp >= 1395666380607 and timestamp <= 1425187835353 and name = $valhallTimeSeries")
    assert(df2.count() == 300)
  }

  it should "handle initial data set with the same size as the batch size" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "100")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp >= 0 and timestamp <= 1790902000001 and name = $valhallTimeSeries")
    assert(df.count() == 100)
  }

  it should "test that start/stop time are handled correctly for data points" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("partitions", "1")
      .load()
      .where(s"timestamp >= 1395666380607 and timestamp <= 1395892204873 and name = $valhallTimeSeries")
    assert(df.count() == 9)
  }

  it should "handle start/stop time without duplicates when using multiple partitions" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("partitions", "7")
      .load()
      .where(s"timestamp >= 1395666380607 and timestamp <= 1395892204873 and name = $valhallTimeSeries")
    assert(df.count() == 9)
  }

  it should "be an error to specify an invalid (time series) name" taggedAs WriteTest in {
    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "stringdatapoints")
      .load()
    destinationDf.createTempView("destinationStringDatapoints")

    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
      spark.sql(s"""
                   |select "timeseries_does_not_exist" as name,
                   |bigint(123456789) as timestamp,
                   |"somevalue" as value
      """.stripMargin)
        .select(destinationDf.columns.map(col): _*)
        .write
        .insertInto("destinationStringDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 404)
    spark.sparkContext.setLogLevel("WARN")
  }
}
