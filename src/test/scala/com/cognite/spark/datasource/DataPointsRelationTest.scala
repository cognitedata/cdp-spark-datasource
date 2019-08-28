package com.cognite.spark.datasource

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.SparkException
import org.apache.spark.sql.Row

class DataPointsRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  val valhallTimeSeries = "'VAL_23-FT-92537-04:X.Value'"

  "DataPointsRelation" should "use our own schema for data points" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(s"name = $valhallTimeSeries")
    assert(df.schema.length == 5)

    assert(
      df.schema.fields.sameElements(Array(
        StructField("name", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("aggregation", StringType, nullable = true),
        StructField("granularity", StringType, nullable = true)
      )))
  }

  it should "iterate over period longer than limit" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "40")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp > 0 and timestamp < 1790902000001 and name = $valhallTimeSeries")
    assert(df.count() == 100)

    val df2 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "40")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp < 1790902000001 and name = $valhallTimeSeries")
    assert(df2.count() == 100)
  }

  it should "handle initial data set below batch size" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"name = $valhallTimeSeries")
    assert(df.count() == 100)
  }

  it should "apply limit to each partition" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("partitions", "2")
      .load()
      .where(
        s"timestamp >= 1509528850000 and timestamp <= 1557485862500 and name = $valhallTimeSeries")
    assert(df.count() == 200)

    val df2 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .option("partitions", "3")
      .load()
      .where(
        s"timestamp >= 1509528850000 and timestamp <= 1557485862500 and name = $valhallTimeSeries")
    assert(df2.count() == 300)
  }

  it should "handle initial data set with the same size as the batch size" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp >= 0 and timestamp <= 1790902000001 and name = $valhallTimeSeries")
    assert(df.count() == 100)
  }

  it should "test that start/stop time are handled correctly for data points" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("partitions", "1")
      .load()
      .where(
        s"timestamp > 1509528850000 and timestamp < 1509528860000 and name = $valhallTimeSeries")
    assert(df.count() == 9)
  }

  it should "handle start/stop time without duplicates when using multiple partitions" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("partitions", "7")
      .load()
      .where(
        s"timestamp > 1509528850000 and timestamp < 1509528860000 and name = $valhallTimeSeries")
    assert(df.count() == 9)
  }

  it should "support aggregations" taggedAs (ReadTest) in {
    val df1 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("partitions", "1")
      .load()
      .where(
        s"timestamp >= 1509490000000 and timestamp <= 1510358400000 and aggregation = 'min' and granularity = '1d' and name = $valhallTimeSeries")
    assert(df1.count() == 10)
    val df1Partitions = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= 1509490000000 and timestamp <= 1510358400000 and aggregation = 'min' and granularity = '1d' and name = $valhallTimeSeries")
    assert(df1Partitions.count() == 10)
    val df2 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= 1520035200000 and timestamp <= 1561507200000 and aggregation = 'avg' and granularity = '60d' and name = $valhallTimeSeries")
    assert(df2.count() == 8)
    val df3 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= 1514592000000 and timestamp <= 1540512000000 and aggregation = 'avg' and granularity = '60d' and name = $valhallTimeSeries")
    val result = df3.collect()
    assert(result.length == 5)
    assert(result(0).getLong(1) == 1514592000000L)
  }

  it should "shift non-aligned aggregates to correct timestamps" taggedAs ReadTest in {
    val df1 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("partitions", "1")
      .load()
      .sort("timestamp")
      .where(
        s"timestamp >= 1509490001000 and timestamp <= 1510358400000 and aggregation = 'min' and granularity = '1d' and name = $valhallTimeSeries")
    val results1 = df1.collect()
    assert(results1.length == 10)
    assert(results1(0).getLong(1) == 1509494400000L)
    val df2 = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("partitions", "3")
      .load()
      .sort("timestamp")
      .where(
        s"timestamp >= 1509490001000 and timestamp <= 1510358400000 and aggregation = 'min' and granularity = '1d' and name = $valhallTimeSeries")
    val results2 = df1.collect()
    assert(results2.length == 10)
    assert(results2(0).getLong(1) == 1509494400000L)
  }

  it should "be possible to specify multiple aggregation types in one query" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "1")
      .option("partitions", "1")
      .load()
      .where(
        s"timestamp >= 1508889600000 and timestamp <= 1511481600000 and aggregation in ('min', 'avg', 'max') and granularity = '30d' and name = $valhallTimeSeries")
    assert(df.count() == 3)
    val results: Array[Row] = df.collect()
    val Array(min, avg, max) = results
    val timeSeriesName = valhallTimeSeries.replace("'", "")
    assert(min.getString(0) == timeSeriesName)
    assert(avg.getString(0) == timeSeriesName)
    assert(max.getString(0) == timeSeriesName)
    assert(min.getLong(1) == 1508889600000L)
    assert(max.getLong(1) == 1508889600000L)
    assert(min.getDouble(2) < avg.getDouble(2))
    assert(avg.getDouble(2) < max.getDouble(2))
  }

  it should "be an error to specify an aggregation without specifying a granularity" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(s"aggregation in ('min') and name = $valhallTimeSeries")
    a[RuntimeException] should be thrownBy df.count()
  }

  it should "be an error to specify a granularity without specifying an aggregation" taggedAs (ReadTest) in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(s"granularity = '30d' and name = $valhallTimeSeries")
    a[RuntimeException] should be thrownBy df.count()
  }

  it should "be an error to specify an invalid granularity" taggedAs (ReadTest) in {
    for (granularity <- Seq("30", "dd", "d30", "1", "0", "1.2d", "1.4y", "1.4seconds")) {
      val df = spark.read
        .format("com.cognite.spark.datasource")
        .option("apiKey", readApiKey)
        .option("type", "datapoints")
        .load()
        .where(
          s"aggregation in ('min') and granularity = '$granularity' and name = $valhallTimeSeries")
      a[RuntimeException] should be thrownBy df.count()
    }
  }

  it should "accept valid granularity specifications" taggedAs (ReadTest) in {
    for (granularity <- Seq(
        "d",
        "day",
        "h",
        "hour",
        "m",
        "minute",
        "1hour",
        "2h",
        "20d",
        "13day",
        "7m",
        "7minute")) {
      val df = spark.read
        .format("com.cognite.spark.datasource")
        .option("apiKey", readApiKey)
        .option("type", "datapoints")
        .option("batchSize", "1")
        .option("limit", "1")
        .option("partitions", "1")
        .load()
        .where(
          s"timestamp > 1409490000000 and aggregation in ('min') and granularity = '$granularity' and name = $valhallTimeSeries")
      assert(df.count() == 1)
    }
  }

  it should "be an error to specify an invalid (time series) name" taggedAs (WriteTest) in {
    val destinationDf = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "datapoints")
      .load()
    destinationDf.createTempView("destinationDatapoints")

    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
      spark
        .sql(s"""
                   |select "timeseries_does_not_exist" as name,
                   |bigint(123456789) as timestamp,
                   |double(1) as value,
                   |"aggregation" as aggregation,
                   |"granularity" as granularity
      """.stripMargin)
        .select(destinationDf.columns.map(col): _*)
        .write
        .insertInto("destinationDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 404)
    spark.sparkContext.setLogLevel("WARN")
  }
}
