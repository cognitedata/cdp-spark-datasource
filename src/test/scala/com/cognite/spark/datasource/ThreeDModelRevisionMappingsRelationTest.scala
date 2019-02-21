package com.cognite.spark.datasource

import org.scalatest.FlatSpec

class ThreeDModelRevisionMappingsRelationTest extends FlatSpec with SparkTest {
  private val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val (modelId, revisionId) = getThreeDModelIdAndRevisionId(writeApiKey)

    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "3dmodelrevisionmappings")
      .option("modelid", modelId)
      .option("revisionid", revisionId)
      .load()
    assert(df.count == 1)

  }
}
