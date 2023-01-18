package cognite.spark.v1

import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelsRelationTest extends FlatSpec with ParallelTestExecution with SparkTest {
  "ThreeDModelsRelation" should "pass a smoke test" taggedAs WriteTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .useOIDCWrite
      .option("type", "3dmodels")
      .option("limitPerPartition", 5)
      .load()
    assert(df.count() == 5)
  }
}
