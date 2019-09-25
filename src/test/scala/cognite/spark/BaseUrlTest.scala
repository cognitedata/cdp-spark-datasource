package cognite.spark

import org.scalatest.FlatSpec

class BaseUrlTest extends FlatSpec with SparkTest {
  private val greenfieldApiKey = System.getenv("TEST_API_KEY_GREENFIELD")
  private val readApiKey = System.getenv("TEST_API_KEY_READ")

  it should "read different files metadata from greenfield and api" taggedAs GreenfieldTest ignore {

    val dfGreenfield = spark.read
      .format("cognite.spark")
      .option("apiKey", greenfieldApiKey)
      .option("type", "events")
      .option("baseUrl", "https://greenfield.cognitedata.com")
      .load()

    val dfApi = spark.read
      .format("cognite.spark")
      .option("apiKey", readApiKey)
      .option("type", "events")
      .load()

    assert(dfGreenfield.count != dfApi.count)
  }
}
