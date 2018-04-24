package com.cognite.spark.connector

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BasicUseTest extends FunSuite with DataFrameSuiteBase {
  val apiKey = System.getenv("COGNITE_API_KEY")
  test("Use our own custom format for timeseries") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .load("00ADD0002/B1/5mMid")
    assert(df.schema.length == 3)

    assert(df.schema.fields.sameElements(Array(StructField("tagId", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("value", DoubleType, nullable = false))))
  }

  test("Iterate over period longer than limit") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "100")
      .option("limit", "1000")
      .load("00ADD0002/B1/5mMid")
      .where("timestamp > 0 and timestamp < 1390902000001")
    assert(df.count() == 1000)
  }

  test("test that we handle initial data set below batch size.") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "2000")
      .option("limit", "1000")
      .load("00ADD0002/B1/5mMid")
    assert(df.count() == 1000)
  }

  test("test that we handle initial data set with the same size as the batch size.") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("type", "timeseries")
      .option("batchSize", "1000")
      .option("limit", "1000")
      .load("00ADD0002/B1/5mMid")
      .where("timestamp >= 0 and timestamp <= 1390902000001")
    assert(df.count() == 1000)
  }

}
