package cognite.spark.v1

import cats.effect._
import cats.effect.unsafe.IORuntime
import com.cognite.sdk.scala.common.OAuth2.OriginalToken
import com.cognite.sdk.scala.common.{Auth, AuthProvider, OAuth2}
import sttp.client3.SttpBackend

sealed trait CdfSparkAuth extends Serializable {
  def provider(
      implicit clock: Clock[IO],
      sttpBackend: SttpBackend[IO, Any],
      ioRuntime: IORuntime): IO[AuthProvider[IO]]
}

object CdfSparkAuth {
  final case class Static(auth: Auth) extends CdfSparkAuth {
    override def provider(
        implicit clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any],
        ioRuntime: IORuntime): IO[AuthProvider[IO]] =
      IO(AuthProvider(auth))
  }

  final case class OAuth2ClientCredentials(
      credentials: OAuth2.ClientCredentials,
      bearerToken: Option[String])
      extends CdfSparkAuth {
    override def provider(
        implicit clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any],
        ioRuntime: IORuntime): IO[AuthProvider[IO]] = {
      val originalToken = bearerToken.map { t =>
        val clock = Clock[IO].monotonic
          .unsafeRunSync()
          .toSeconds + 60 //VH TODO change it to expired time of real oidc token
        println(s"Create provider with clock = ${clock}") //VH TODO remove println
        OriginalToken(t, clock)
      }
      OAuth2.ClientCredentialsProvider[IO](credentials.copy(originalToken = originalToken))
    }
  }

  final case class OAuth2Sessions(session: OAuth2.Session, bearerToken: Option[String])
      extends CdfSparkAuth {
    override def provider(
        implicit clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any],
        ioRuntime: IORuntime): IO[AuthProvider[IO]] = {
      val originalToken = bearerToken.map { t =>
        val clock = Clock[IO].monotonic
          .unsafeRunSync()
          .toSeconds + 60 //VH TODO change it to expired time of real session token
        OriginalToken(t, clock)
      }
      OAuth2.SessionProvider[IO](session.copy(originalToken = originalToken))
    }
  }
}
