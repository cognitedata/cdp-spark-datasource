package com.cognite.spark.datasource

import org.scalatest.FlatSpec

class ThreeDModelRevisionSectorsRelationTest extends FlatSpec with SparkTest {
  private val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  "ThreeDModelRevisionSectorsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val (modelId, revisionId) = getThreeDModelIdAndRevisionId(writeApiKey)

    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "3dmodelrevisionsectors")
      .option("modelid", modelId)
      .option("revisionid", revisionId)
      .load()
    assert(df.count == 1)

  }
}
