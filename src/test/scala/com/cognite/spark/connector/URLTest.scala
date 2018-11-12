package com.cognite.spark.connector

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class URLTest extends FunSuite {
  test("verify path encoding of base url") {
    assert("https://api.cognitedata.com/api/0.5/projects/stat%C3%B8il/timeseries/data" == TimeSeriesRelation.baseTimeSeriesURL("statøil").build().toString)
  }
}

