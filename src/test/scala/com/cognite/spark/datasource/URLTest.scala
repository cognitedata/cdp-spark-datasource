package com.cognite.spark.datasource

import org.scalatest.FunSuite

class URLTest extends FunSuite with SparkTest with CdpConnector {
  test("verify path encoding of base url") {
    val dataPointsRelation = new DataPointsRelation("", "statøil", "path",
      None, None, None, "", false)(spark.sqlContext) // scalastyle:ignore null
    assert("https://api.cognitedata.com/api/0.5/projects/stat%C3%B8il/timeseries/data" == dataPointsRelation.baseDataPointsUrl("statøil").toString)
  }
}

