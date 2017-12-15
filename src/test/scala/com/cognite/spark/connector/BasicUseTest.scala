package com.cognite.spark.connector

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BasicUseTest extends FunSuite with DataFrameSuiteBase {
  test("Use our own custom format for timeseries") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
        .option("project", "akerbp")
        .option("apiKey", apiKey)
        .load("00ADD0002/B1/5mMid")
    assert(df.schema.length == 3)

    assert(df.schema.fields.sameElements(Array(StructField("tagId", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("value", DoubleType, nullable = false))))
  }

  // NB: We will change this to use .where() instead of options (i.e pushdowns)
  test("Iterate over period longer than limit") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("batchsize", "100")
      .option("start", "0")
      .option("stop", "1390902000001")
      .load("00ADD0002/B1/5mMid")
    assert(df.count() == 1000)
  }

  test("test that we handle initial data set below batch size.") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("batchsize", "2000")
      .option("start", "0")
      .option("stop", "1390902000001")
      .load("00ADD0002/B1/5mMid")
    assert(df.count() == 1000)
  }

  test("test that we handle initial data set with the same size as the batch size.") {
    val df = sqlContext.read.format("com.cognite.spark.connector")
      .option("project", "akerbp")
      .option("apiKey", apiKey)
      .option("batchsize", "1000")
      .option("start", "0")
      .option("stop", "1390902000001")
      .load("00ADD0002/B1/5mMid")
    assert(df.count() == 1000)
  }

}
