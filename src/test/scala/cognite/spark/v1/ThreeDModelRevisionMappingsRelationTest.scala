package cognite.spark.v1

import com.cognite.sdk.scala.common.ApiKeyAuth
import org.scalatest.FlatSpec

class ThreeDModelRevisionMappingsRelationTest extends FlatSpec with SparkTest {
  private val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest ignore {
    val (modelId, revisionId) = getThreeDModelIdAndRevisionId(writeApiKey)

    implicit val auth: ApiKeyAuth = writeApiKey

    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "3dmodelrevisionmappings")
      .option("modelid", modelId)
      .option("revisionid", revisionId)
      .load()
    assert(df.count == 1)
  }
}
