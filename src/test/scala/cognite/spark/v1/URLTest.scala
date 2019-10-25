package cognite.spark.v1

import com.cognite.sdk.scala.common.ApiKeyAuth
import org.scalatest.FlatSpec

class URLTest extends FlatSpec with SparkTest {
  private val greenfieldApiKey = System.getenv("TEST_API_KEY_GREENFIELD")
  private val readApiKey = System.getenv("TEST_API_KEY_READ")

  it should "read different files metadata from greenfield and api" taggedAs GreenfieldTest in {

    val dfGreenfield = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", greenfieldApiKey)
      .option("type", "events")
      .option("baseUrl", "https://greenfield.cognitedata.com")
      .load()

    val dfApi = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .load()

    assert(dfGreenfield.count > 0)
    assert(dfGreenfield.count != dfApi.count)
  }

  it should "verify that correct project is retrieved from TEST_API_KEY" in {
    val project =
      getProject(ApiKeyAuth(readApiKey), Constants.DefaultMaxRetries, Constants.DefaultBaseUrl)
    assert(project == "publicdata")
  }
}
