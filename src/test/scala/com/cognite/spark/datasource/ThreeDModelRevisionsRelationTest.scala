package com.cognite.spark.datasource

import org.scalatest.FlatSpec

class ThreeDModelRevisionsRelationTest extends FlatSpec with SparkTest  {
  private val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val (modelId, _) = getThreeDModelIdAndRevisionId(writeApiKey)

    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "3dmodelrevisions")
      .option("modelid", modelId)
      .load()
    assert(df.count == 1)
  }
}
