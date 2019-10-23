package cognite.spark.v1

import com.cognite.sdk.scala.common.ApiKeyAuth
import org.scalatest.FlatSpec

class ThreeDModelRevisionNodesRelationTest extends FlatSpec with SparkTest  {
  private val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  "ThreeDModelRevisionNodesRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val (modelId, revisionId) = getThreeDModelIdAndRevisionId(writeApiKey)

    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "3dmodelrevisionnodes")
      .option("modelid", modelId)
      .option("revisionid", revisionId)
      .option("limit","10")
      .load()
    assert(df.count == 10)

  }
}
