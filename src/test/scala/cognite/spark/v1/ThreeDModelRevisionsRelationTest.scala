package cognite.spark.v1

import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelRevisionsRelationTest extends FlatSpec with ParallelTestExecution with SparkTest  {
  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val model = writeClient.threeDModels.list().compile.toList.head

    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "3dmodelrevisions")
      .option("modelid", model.id)
      .load()
    assert(df.count == 1)
  }
}
