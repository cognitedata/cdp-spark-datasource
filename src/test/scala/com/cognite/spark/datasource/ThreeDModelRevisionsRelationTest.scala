package com.cognite.spark.datasource

import org.scalatest.FlatSpec

class ThreeDModelRevisionsRelationTest extends FlatSpec with SparkTest  {
  private val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val (modelId, _) = getThreeDModelIdAndRevisionId(writeApiKey)

    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "3dmodelrevisions")
      .option("modelid", modelId)
      .load()
    assert(df.count == 1)
  }
}
