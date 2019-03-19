package com.cognite.spark.datasource

import org.scalatest.FunSuite

class BasicUseTest extends FunSuite with SparkTest with CdpConnector {

  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  test("smoke test time series metadata", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("limit", "100")
      .load()
    assert(df.count() == 100)
  }

  test("smoke test assets", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limit", "1000")
      .load()

    df.createTempView("assets")
    val res = spark.sql("select * from assets")
      .collect()
    assert(res.length == 1000)
  }

  test("assets with very small batchSize", ReadTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("batchSize", "1")
      .option("limit", "10")
      .load()

    assert(df.count() == 10)
  }
  //@TODO This uses jetfire2 until we have tables in publicdata, thus tagged WriteTest even if only reading
  test("smoke test raw", WriteTest) {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "raw")
      .option("batchSize", "100")
      .option("limit", "1000")
      .option("database", "testdb")
      .option("table", "cryptoAssets")
      .option("inferSchema", "true")
      .option("inferSchemaLimit", "100")
      .load()

    df.createTempView("raw")
    val res = spark.sqlContext.sql("select * from raw")
        .collect()
    assert(res.length == 1000)
  }
}
