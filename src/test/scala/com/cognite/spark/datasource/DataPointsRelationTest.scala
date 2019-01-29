package com.cognite.spark.datasource

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField}
import org.scalatest.{FlatSpec, FunSuite, Matchers}
import org.apache.spark.SparkException
import com.cognite.spark.datasource.CdpApiException

class DataPointsRelationTest extends FlatSpec with Matchers with SparkTest {
  val apiKey = System.getenv("TEST_API_KEY")
  "DataPointsRelation" should "use our own schema for data points" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .load()
      .where("name = 'Bitbay USD'")
    assert(df.schema.length == 5)

    assert(df.schema.fields.sameElements(Array(
      StructField("name", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("value", DoubleType, nullable = false),
      StructField("aggregation", StringType, nullable = true),
      StructField("granularity", StringType, nullable = true))))
  }

  it should "iterate over period longer than limit" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "40")
      .option("limit", "100")
      .load()
      .where("timestamp > 0 and timestamp < 1790902000001 and name = 'Bitbay USD'")
    assert(df.count() == 100)
  }

  it should "handle initial data set below batch size" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "2000")
      .option("limit", "100")
      .load()
      .where("name = 'Bitbay USD'")
    assert(df.count() == 100)
  }

  it should "handle initial data set with the same size as the batch size" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "100")
      .load()
      .where("timestamp >= 0 and timestamp <= 1790902000001 and name = 'Bitbay USD'")
    assert(df.count() == 100)
  }

  it should "test that start/stop time are handled correctly for data points" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "100")
      .load()
      .where("name = 'stopTimeTest'")
    assert(df.count() == 2)
  }

  it should "support aggregations" in {
    val df1 = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "100")
      .load()
      .where("timestamp >= 0 and timestamp <= 1790902000001" +
        " and aggregation = 'avg' and granularity = '30d' and name = 'Bitbay USD'")
    assert(df1.count() == 49)
    val df2 = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "100")
      .load()
      .where("timestamp >= 0 and timestamp <= 1790902000001" +
        " and aggregation = 'avg' and granularity = '60d' and name = 'Bitbay USD'")
    assert(df2.count() == 25)
  }

  it should "be possible to specify multiple aggregation types in one query" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .option("batchSize", "100")
      .option("limit", "1")
      .load()
      .where("timestamp >= 0 and timestamp <= 1790902000001" +
        " and aggregation in ('min', 'avg', 'max') and granularity = '30d' and name = 'Bitbay USD'")
    assert(df.count() == 3)
    val results = df.collect()
    val Array(min, avg, max) = results
    assert(min.getString(0) == "Bitbay USD")
    assert(avg.getString(0) == "Bitbay USD")
    assert(max.getString(0) == "Bitbay USD")
    assert(min.getLong(1) == 1399680000000L)
    assert(avg.getLong(1) == 1399680000000L)
    assert(max.getLong(1) == 1399680000000L)
    assert(min.getDouble(2) < avg.getDouble(2))
    assert(avg.getDouble(2) < max.getDouble(2))
  }

  it should "be an error to specify an aggregation without specifying a granularity" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .load()
      .where("aggregation in ('min') and name = 'Bitbay USD'")
    a[RuntimeException] should be thrownBy df.count()
  }

  it should "be an error to specify a granularity without specifying an aggregation" in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .load()
      .where("granularity = '30d' and name = 'Bitbay USD'")
    a[RuntimeException] should be thrownBy df.count()
  }

  it should "be an error to specify an invalid granularity" in {
    for (granularity <- Seq("30", "dd", "d30", "1", "0", "1.2d", "1.4y", "1.4seconds")) {
      val df = spark.read.format("com.cognite.spark.datasource")
        .option("project", "jetfiretest2")
        .option("apiKey", apiKey)
        .option("type", "datapoints")
        .load()
        .where(s"aggregation in ('min') and granularity = '$granularity' and name = 'Bitbay USD'")
      a[RuntimeException] should be thrownBy df.count()
    }
  }

  it should "accept valid granularity specifications" in {
    for (granularity <- Seq("d", "day", "h", "hour", "m", "minute", "1hour", "2h", "20d", "13day", "7m", "7minute")) {
      val df = spark.read.format("com.cognite.spark.datasource")
        .option("project", "jetfiretest2")
        .option("apiKey", apiKey)
        .option("type", "datapoints")
        .option("batchSize", "1")
        .option("limit", "1")
        .load()
        .where(s"aggregation in ('min') and granularity = '$granularity' and name = 'Bitbay USD'")
      assert(df.count() == 1)
    }
  }

  it should "be an error to specify an invalid (timeseries) name" in {
    val destinationDf = spark.read.format("com.cognite.spark.datasource")
      .option("project", "jetfiretest2")
      .option("apiKey", apiKey)
      .option("type", "datapoints")
      .load()
    destinationDf.createTempView("destinationDatapoints")

    val e = intercept[SparkException] {
      spark.sql(s"""
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
  }
}
