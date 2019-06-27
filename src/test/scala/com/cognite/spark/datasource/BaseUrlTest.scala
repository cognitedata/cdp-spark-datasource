package com.cognite.spark.datasource

import org.scalatest.FlatSpec

class BaseUrlTest extends FlatSpec with SparkTest {
  private val greenfieldApiKey = System.getenv("TEST_API_KEY_GREENFIELD")
  private val readApiKey = System.getenv("TEST_API_KEY_READ")

  it should "read different files metadata from greenfield and api" taggedAs GreenfieldTest in {

    val dfGreenfield = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", greenfieldApiKey)
      .option("type", "files")
      .option("baseUrl", "https://greenfield.cognitedata.com")
      .load()

    val dfApi = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .load()

    assert(dfGreenfield.count != dfApi.count)
  }
}
