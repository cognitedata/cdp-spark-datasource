package com.cognite.spark.connector

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class URLTests extends FunSuite {
  test("verify path encoding of base url") {
    assert("https://api.cognitedata.com/api/0.4/projects/stat%C3%B8il/timeseries/data?limit=100" == FullScanRelations.baseTimeSeriesURL("statøil").build().toString)
  }
}

