package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.common.{BearerTokenAuth, OAuth2, TicketAuth}
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteInternalId}
import org.scalatest.{Matchers, WordSpec}
import sttp.client3.{SttpBackend, UriContext}

class DefaultSourceTest extends WordSpec with Matchers {

  implicit val backend: SttpBackend[IO, Any] = CdpConnector.retryingSttpBackend(false, 3, 5)

  "DefaultSource" should {
    "parseAuth and fall back in order" should {
      val fullParams = Map(
        "authTicket" -> "value-AuthTicket",
        "bearerToken" -> "value-BearerToken",
        "scopes" -> "value-Scopes",
        "audience" -> "value-Audience",
        "tokenUri" -> "value-TokenUri",
        "clientId" -> "value-ClientId",
        "clientSecret" -> "value-ClientSecret",
        "project" -> "value-Project",
        "baseUrl" -> "https://value-field.cognitedata.com",
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

      "work for bearerToken" in {
        val params = fullParams.filter { case (key, _) => !Set("authTicket").contains(key) }
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.Static(BearerTokenAuth("value-BearerToken"))
        )
      }

      "work for session and use baseUrl from input params if it exists" in {
        val params =
          fullParams.filter {
            case (key, _) => !Set("authTicket", "bearerToken").contains(key)
          }
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.OAuth2Sessions(OAuth2
            .Session("https://value-field.cognitedata.com", 123, "value-SessionKey", "value-Project"))
        )
      }

      "work for session and use default baseUrl if it does not exist in input params" in {
        val params =
          fullParams.filter {
            case (key, _) => !Set("authTicket", "bearerToken", "baseUrl").contains(key)
          }
        DefaultSource.parseAuth(params) shouldBe Some(
          CdfSparkAuth.OAuth2Sessions(
            OAuth2.Session(Constants.DefaultBaseUrl, 123, "value-SessionKey", "value-Project")
          )
        )
      }

      "work for clientCredential" in {
        val params =
          fullParams.filter {
            case (key, _) => !Set("authTicket", "bearerToken", "sessionKey").contains(key)
          }
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

    "parseCogniteIds" should {
      "parse integers as internalIds" in {
        DefaultSource.parseCogniteIds("123") shouldBe List(CogniteInternalId(123))
      }
      "parse strings as externalIds" in {
        DefaultSource.parseCogniteIds("\"123\"") shouldBe List(CogniteExternalId("123"))
        DefaultSource.parseCogniteIds(""" "\"123\"" """) shouldBe List(CogniteExternalId("\"123\""))
        DefaultSource.parseCogniteIds("abc") shouldBe List(CogniteExternalId("abc"))
        DefaultSource.parseCogniteIds("{invalid json") shouldBe List(CogniteExternalId("{invalid json"))
        DefaultSource.parseCogniteIds("\"[123,456]\"") shouldBe List(CogniteExternalId("[123,456]"))
      }
      "parse arrays according to their contents" in {
        DefaultSource.parseCogniteIds("""[123, "123"]""") shouldBe List(
          CogniteInternalId(123),
          CogniteExternalId("123"))
      }
      "parse other valid json as externalId" in {
        DefaultSource.parseCogniteIds("""{"key": 123}""") shouldBe List(
          CogniteExternalId("""{"key": 123}"""))
        DefaultSource.parseCogniteIds("""[123, "123", {"abc": 0} ]""") shouldBe List(
          CogniteExternalId("""[123, "123", {"abc": 0} ]"""))
      }
    }
  }
}
