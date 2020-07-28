package cognite.spark.v1

import org.scalatest.FlatSpec

class URLTest extends FlatSpec with SparkTest {
  private val greenfieldApiKey =
    Option(System.getenv("TEST_API_KEY_GREENFIELD"))
      .getOrElse(throw new CdfSparkException("Greenfield API key was not specified in the TEST_API_KEY_GREENFIELD env variable."))

  it should "read different files metadata from greenfield and api" taggedAs GreenfieldTest in {

    val dfGreenfield = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", greenfieldApiKey)
      .option("type", "files")
      .option("baseUrl", "https://greenfield.cognitedata.com")
      .load()

    val dfApi = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .load()

    assert(dfGreenfield.count > 0)
    assert(dfGreenfield.count != dfApi.count)
  }
}
