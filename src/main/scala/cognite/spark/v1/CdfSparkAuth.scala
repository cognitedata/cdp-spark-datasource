package cognite.spark.v1

import cats.effect._
import com.cognite.sdk.scala.common.{Auth, AuthProvider, OAuth2}
import sttp.client3.SttpBackend

sealed trait CdfSparkAuth extends Serializable {
  def provider(implicit clock: Clock[IO], sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]]
}

object CdfSparkAuth {
  final case class Static(auth: Auth) extends CdfSparkAuth {
    override def provider(
        implicit clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]] =
      IO(AuthProvider(auth))
  }

  import CdpConnector.ioRuntime

  final case class OAuth2ClientCredentials(credentials: OAuth2.ClientCredentials)(
      implicit sttpBackend: SttpBackend[IO, Any])
      extends CdfSparkAuth {
    private val cacheToken = credentials.getAuth[IO]().attempt.map(_.toOption).unsafeRunSync()

    override def provider(
        implicit clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]] =
      OAuth2.ClientCredentialsProvider[IO](credentials, maybeCacheToken = cacheToken)
  }

  final case class OAuth2Sessions(session: OAuth2.Session)(implicit sttpBackend: SttpBackend[IO, Any])
      extends CdfSparkAuth {

    private val refreshSecondsBeforeExpiration = 300

    private val cacheToken = session
      .getAuth[IO](refreshSecondsBeforeExpiration = refreshSecondsBeforeExpiration)
      .attempt
      .map(_.toOption)
      .unsafeRunSync()

    override def provider(
        implicit clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Any]): IO[AuthProvider[IO]] =
      OAuth2.SessionProvider[IO](
        session,
        refreshSecondsBeforeExpiration = refreshSecondsBeforeExpiration,
        maybeCacheToken = cacheToken
      )
  }
}
