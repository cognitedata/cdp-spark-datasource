package com.cognite.spark.datasource

import org.scalatest.FunSuite

class URLTest extends FunSuite with SparkTest with CdpConnector {
  test("verify path encoding of base url") {
    val timeseriesRelation = new TimeSeriesRelation("", "statøil", "path",
      None, None, None, "", false)(spark.sqlContext) // scalastyle:ignore null
    assert("https://api.cognitedata.com/api/0.5/projects/stat%C3%B8il/timeseries/data" == timeseriesRelation.baseTimeSeriesURL("statøil").toString)
  }
}

