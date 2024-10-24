package cognite.spark.v1.fdm.utils

import cats.effect.IO
import cognite.spark.v1.fdm.utils.FDMTestMetricOperations.getTestClient
import com.cognite.sdk.scala.v1.GenericClient

import java.util.UUID

object FDMTestConstants {

  val clientId: String = sys.env("TEST_CLIENT_ID")
  val clientSecret: String = sys.env("TEST_CLIENT_SECRET")
  val cluster: String = sys.env("TEST_CLUSTER")
  val project: String = sys.env("TEST_PROJECT")
  val tokenUri: String = sys.env
    .get("TEST_TOKEN_URL")
    .orElse(
      sys.env
        .get("TEST_AAD_TENANT")
        .map(tenant => s"https://login.microsoftonline.com/$tenant/oauth2/v2.0/token"))
    .getOrElse("https://sometokenurl")

  val audience = s"https://$cluster.cognitedata.com"
  val client: GenericClient[IO] = getTestClient()

  val spaceExternalId = "testSpaceForSparkDatasource"

  val viewVersion = "v1"

  def apiCompatibleRandomString(): String =
    UUID.randomUUID().toString.replaceAll("[_\\-x0]", "").substring(0, 5)

  def generateNodeExternalId: String = s"randomId${apiCompatibleRandomString()}"

}
