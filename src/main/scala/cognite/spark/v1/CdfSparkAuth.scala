package cognite.spark.v1

import cats.effect._
import com.cognite.sdk.scala.common.{Auth, AuthProvider, OAuth2}
import sttp.client3.SttpBackend

sealed trait CdfSparkAuth extends Serializable {
  def provider(
      implicit clock: Clock[TracedIO],
      sttpBackend: SttpBackend[TracedIO, Any]): TracedIO[AuthProvider[TracedIO]]
}

object CdfSparkAuth {
  final case class Static(auth: Auth) extends CdfSparkAuth {
    override def provider(
        implicit clock: Clock[TracedIO],
        sttpBackend: SttpBackend[TracedIO, Any]): TracedIO[AuthProvider[TracedIO]] =
      TracedIO.liftIO(IO(AuthProvider[TracedIO](auth)))
  }

  final case class OAuth2ClientCredentials(credentials: OAuth2.ClientCredentials) extends CdfSparkAuth {

    override def provider(
        implicit clock: Clock[TracedIO],
        sttpBackend: SttpBackend[TracedIO, Any]): TracedIO[AuthProvider[TracedIO]] =
      OAuth2
        .ClientCredentialsProvider[TracedIO](credentials)
        .map(x => x)
  }

  final case class OAuth2Sessions(session: OAuth2.Session) extends CdfSparkAuth {

    private val refreshSecondsBeforeExpiration = 300L

    override def provider(
        implicit clock: Clock[TracedIO],
        sttpBackend: SttpBackend[TracedIO, Any]): TracedIO[AuthProvider[TracedIO]] =
      OAuth2
        .SessionProvider[TracedIO](
          session,
          refreshSecondsBeforeExpiration = refreshSecondsBeforeExpiration
        )
        .map(x => x)
  }
}
