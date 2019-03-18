package com.cognite.spark.datasource

import org.scalatest.{FlatSpec, Matchers}

class EventsRelationTest  extends FlatSpec with Matchers with SparkTest {
  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")
  import spark.implicits._

  it should "apply pushdown filters on type" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "events")
      .load()
      .where(s"type = '***' and subtype = 'Val'")
    df.show
    assert(df.count > 0)

  }
}
