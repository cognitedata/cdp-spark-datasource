package cognite.spark.v1

import cats.effect._
import com.cognite.sdk.scala.common.{Auth, AuthProvider, OAuth2}
import sttp.client3.SttpBackend

sealed trait CdfSparkAuth extends Serializable {
  def provider(
      implicit cs: ContextShift[IO],
      clock: Clock[IO],
      sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]]
}

object CdfSparkAuth {
  final case class Static(auth: Auth) extends CdfSparkAuth {
    override def provider(
        implicit cs: ContextShift[IO],
        clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]] =
      IO(AuthProvider(auth))
  }

  final case class OAuth2ClientCredentials(credentials: OAuth2.ClientCredentials) extends CdfSparkAuth {
    override def provider(
        implicit cs: ContextShift[IO],
        clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]] =
      OAuth2.ClientCredentialsProvider[IO](credentials)
  }

  final case class OAuth2Sessions(session: OAuth2.Session) extends CdfSparkAuth {
    override def provider(
        implicit cs: ContextShift[IO],
        clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]] =
      OAuth2.SessionProvider[IO](session)
  }
}
