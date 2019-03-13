package com.cognite.spark.datasource

import org.scalatest.FunSuite

class URLTest extends FunSuite with SparkTest with CdpConnector {

  val readApiKey = System.getenv("TEST_API_KEY_READ")

  test("verify path encoding of base url") {
    val dataPointsRelation = new DataPointsRelation(RelationConfig("", "statøil", Some(100), None, 10,
      collectMetrics = false, "","https://api.cognitedata.com"), 1, None)(spark.sqlContext)
    assert("https://api.cognitedata.com/api/0.5/projects/stat%C3%B8il/timeseries/data"
      == dataPointsRelation.baseDataPointsUrl("statøil").toString)
  }

  test("verify that correct project is retrieved from TEST_API_KEY"){
    val project = getProject(readApiKey, Constants.DefaultMaxRetries, Constants.DefaultBaseUrl)
    assert(project == "publicdata")
  }
}

