package cognite.spark.v1

import org.scalatest.FlatSpec

class ThreeDModelsRelationTest extends FlatSpec with SparkTest {
  "ThreeDModelsRelation" should "pass a smoke test" taggedAs WriteTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "3dmodels")
      .option("limitPerPartition", 5)
      .load()
    assert(df.count == 5)
  }
}
