package cognite.spark.v1

import org.scalatest.FlatSpec

class ThreeDModelsRelationTest extends FlatSpec with SparkTest {
  private val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  "ThreeDModelsRelation" should "pass a smoke test" taggedAs WriteTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "3dmodels")
      .option("limit", 5)
      .load()
    assert(df.count == 5)
  }
}
