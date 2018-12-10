package com.cognite.spark.datasource

import org.scalatest.FunSuite

class URLTest extends FunSuite {
  test("verify path encoding of base url") {
    assert("https://api.cognitedata.com/api/0.5/projects/stat%C3%B8il/timeseries/data" == TimeSeriesRelation.baseTimeSeriesURL("statøil").toString)
  }
}

