package com.cognite.spark.datasource

import org.scalatest.{FlatSpec, Matchers}

class FilesRelationTest extends FlatSpec with Matchers with SparkTest {
  private val readApiKey = System.getenv("TEST_API_KEY_READ")

  "FilesRelation" should "read files" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .load()

    // as of 2019-02-19 there are 11 files, but more might be added in the future,
    // which should not be allowed to break this test.
    assert(df.count() >= 11)
  }

  it should "respect the limit option" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("limit", "5")
      .load()

    assert(df.count() == 5)
  }

  it should "use cursors when necessary" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("batchSize", "2")
      .load()

    assert(df.count() >= 11)
  }
}
