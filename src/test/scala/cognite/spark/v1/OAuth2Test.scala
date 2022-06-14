package cognite.spark.v1

import com.cognite.sdk.scala.common.InvalidAuthentication
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
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
        .option("limitPerPartition", "100")
        .load()
    )

    assert(df.count() > 0)
  }
  //TODO enable when we get a new set of Aize credentials
  ignore should "authenticate using client credentials in Aize" in {
    val aizeClientId = sys.env("AIZE_CLIENT_ID")
    val aizeClientSecret = sys.env("AIZE_CLIENT_SECRET")

    val df = (
      spark.read.format("cognite.spark.v1")
        .option("baseUrl", "https://api.cognitedata.com")
        .option("type", "assets")
        .option("tokenUri", "https://login.aize.io/oauth/token")
        .option("clientId", aizeClientId)
        .option("clientSecret", aizeClientSecret)
        .option("project", "aize")
        .option("audience", "https://twindata.io/cdf/T101014843")
        .option("limitPerPartition", "100")
        .load()
      )
    assert(df.count() > 0)
  }

  it should "throw InvalidAuthentication when project is not provided" in {
    intercept[Exception]{
      val df = (
        spark.read.format("cognite.spark.v1")
          .option("baseUrl", "https://bluefield.cognitedata.com")
          .option("type", "timeseries")
          .option("tokenUri", tokenUri)
          .option("clientId", clientId)
          .option("clientSecret", clientSecret)
          .option("scopes", "https://bluefield.cognitedata.com/.default")
          .option("limitPerPartition", "100")
          .load()
        )
      df.count()
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
        .option("limitPerPartition", "100")
        .load()
      )
    sparkIntercept{
      df.count()
    }
  }

}
