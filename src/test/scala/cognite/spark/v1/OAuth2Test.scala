package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class OAuth2Test
  extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
{
  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  it should "authenticate using client credentials" in {
    val df = (
      spark.read.format("cognite.spark.v1")
        .option("baseUrl", "https://bluefield.cognitedata.com")
        .option("type", "timeseries")
        .option("tokenUri", tokenUri)
        .option("clientId", clientId)
        .option("clientSecret", clientSecret)
        .option("scopes", "https://bluefield.cognitedata.com/.default")
        .load()
    )

    noException should be thrownBy df.head()
  }
}
