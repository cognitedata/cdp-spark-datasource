package cognite.spark.v1

import com.cognite.sdk.scala.common.InvalidAuthentication
import org.apache.spark.SparkException
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
        .option("project", "extractor-bluefield-testing")
        .option("scopes", "https://bluefield.cognitedata.com/.default")
        .load()
    )

    assert(df.head().size > 0)
  }

  ignore should "throw InvalidAuthentication when project is not provided" in {
    // login.status does not work for OIDC tokens anymore, it throws a 404.
    // It calls login.status if project is not provided so this test fails for OIDC tokens
    // Something to fix later on scala-sdk
    try{
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
      df.count()
      assert(false) // fail if it works
    }
    catch {
      case e: InvalidAuthentication => assert(true)
      case _: Throwable => assert(false)
    }
  }

  it should "throw SparkException when using invalid client credentials" in {
    val df = (
      spark.read.format("cognite.spark.v1")
        .option("baseUrl", "https://bluefield.cognitedata.com")
        .option("type", "timeseries")
        .option("tokenUri", tokenUri)
        .option("clientId", "1")
        .option("clientSecret", "1")
        .option("project", "extractor-bluefield-testing")
        .option("scopes", "https://bluefield.cognitedata.com/.default")
        .load()
      )
    try{
      df.count()
      assert(false) // fail if it works
    }
    catch {
      case e: SparkException => assert(true)
      case _: Throwable => assert(false)
    }
  }

}
