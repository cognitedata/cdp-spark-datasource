package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class OAuth2Test extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  val clientId = sys.env("TEST_CLIENT_ID")
  val clientSecret = sys.env("TEST_CLIENT_SECRET")
  val aadTenant = sys.env("TEST_AAD_TENANT")
  val cluster = sys.env("TEST_CLUSTER")
  val project = sys.env("TEST_PROJECT")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  it should "authenticate using client credentials" in {
    val df = (
      spark.read
        .format(DefaultSource.sparkFormatString)
        .option("baseUrl", s"https://${cluster}.cognitedata.com")
        .option("type", "timeseries")
        .option("tokenUri", tokenUri)
        .option("clientId", clientId)
        .option("clientSecret", clientSecret)
        .option("project", project)
        .option("scopes", s"https://${cluster}.cognitedata.com/.default")
        .option("limitPerPartition", "100")
        .load()
      )

    assert(df.take(1).length > 0)
  }

  it should "throw InvalidAuthentication when project is not provided" in {
    intercept[Exception] {
      val df = (
        spark.read
          .format(DefaultSource.sparkFormatString)
          .option("baseUrl", s"https://${cluster}.cognitedata.com")
          .option("type", "timeseries")
          .option("tokenUri", tokenUri)
          .option("clientId", clientId)
          .option("clientSecret", clientSecret)
          .option("scopes", s"https://${cluster}.cognitedata.com/.default")
          .option("limitPerPartition", "100")
          .load()
        )
      df.take(1).length
    }
  }

  it should "throw SparkException when using invalid client credentials" in {
    val df = (
      spark.read
        .format(DefaultSource.sparkFormatString)
        .option("baseUrl", s"https://${cluster}.cognitedata.com")
        .option("type", "timeseries")
        .option("tokenUri", tokenUri)
        .option("clientId", "1")
        .option("clientSecret", "1")
        .option("project", project)
        .option("scopes", s"https://${cluster}.cognitedata.com/.default")
        .option("limitPerPartition", "100")
        .load()
      )
    sparkIntercept {
      df.take(1).length
    }
  }

}
