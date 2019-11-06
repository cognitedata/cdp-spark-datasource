package cognite.spark.v1

import com.cognite.sdk.scala.common.ApiKeyAuth
import org.scalatest.FlatSpec

class ThreeDModelRevisionNodesRelationTest extends FlatSpec with SparkTest  {
  "ThreeDModelRevisionNodesRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val model = writeClient.threeDModels.list().compile.toList.head
    val revision = writeClient.threeDRevisions(model.id).list().compile.toList.head

    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "3dmodelrevisionnodes")
      .option("modelid", model.id)
      .option("revisionid", revision.id)
      .option("limit", "10")
      .load()
    assert(df.count == 10)

  }
}
