package cognite.spark.v1

import com.cognite.sdk.scala.common.{ApiKeyAuth, BearerTokenAuth, OAuth2, TicketAuth}
import org.scalatest.{Matchers, WordSpec}
import sttp.client3.UriContext

class DefaultSourceTest extends WordSpec with Matchers {

  "DefaultSource" should {
    "parseAuth and fall back in order" should {
      val fullParams = Map(
        "authTicket" -> "value-AuthTicket",
        "bearerToken" -> "value-BearerToken",
        "apiKey" -> "value-ApiKey",
        "scopes" -> "value-Scopes",
        "audience" -> "value-Audience",
        "tokenUri" -> "value-TokenUri",
        "clientId" -> "value-ClientId",
        "clientSecret" -> "value-ClientSecret",
        "project" -> "value-Project",
        "baseUrl" -> "https://bluefield.cognitedata.com",
        "sessionId" -> "123",
        "sessionKey" -> "value-SessionKey",
        "project" -> "value-Project",
        "tokenFromVault" -> "value-TokenFromVault"
      )
      "work for authTicket" in {
        DefaultSource.parseAuth(fullParams) shouldBe Some(
          CdfSparkAuth.Static(TicketAuth("value-AuthTicket"))
        )
      }

      "work for apiKey" in {
        val params = fullParams.filterKeys(!Set("authTicket").contains(_))
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.Static(ApiKeyAuth("value-ApiKey"))
        )
      }

      "work for bearerToken" in {
        val params = fullParams.filterKeys(!Set("authTicket", "apiKey").contains(_))
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.Static(BearerTokenAuth("value-BearerToken"))
        )
      }

      "work for session and use baseUrl from input params if it exists" in {
        val params =
          fullParams.filterKeys(!Set("authTicket", "apiKey", "bearerToken").contains(_))
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.OAuth2Sessions(
            OAuth2.Session(
              "https://bluefield.cognitedata.com",
              123,
              "value-SessionKey",
              "value-Project",
              "value-TokenFromVault"))
        )
      }

      "work for session and use default baseUrl if it does not exist in input params" in {
        val params =
          fullParams.filterKeys(!Set("authTicket", "apiKey", "bearerToken", "baseUrl").contains(_))
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.OAuth2Sessions(
            OAuth2.Session(
              Constants.DefaultBaseUrl,
              123,
              "value-SessionKey",
              "value-Project",
              "value-TokenFromVault"))
        )
      }

      "work for clientCredential" in {
        val params =
          fullParams.filterKeys(!Set("authTicket", "apiKey", "bearerToken", "sessionKey").contains(_))
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.OAuth2ClientCredentials(
            OAuth2.ClientCredentials(
              uri"value-TokenUri",
              "value-ClientId",
              "value-ClientSecret",
              List("value-Scopes"),
              "value-Project",
              Some("value-Audience")))
        )
      }
    }
    "parseAuth and return None" should {
      "work when input is empty" in {
        DefaultSource.parseAuth(Map()) shouldBe None
      }
      "work when input contains invalid value" in {
        DefaultSource.parseAuth(Map("toto" -> "1", "tata" -> "2")) shouldBe None
      }
    }
  }
}
