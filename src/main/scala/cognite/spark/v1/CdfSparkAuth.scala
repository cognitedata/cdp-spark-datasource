package cognite.spark.v1

import cats.effect._
import com.cognite.sdk.scala.common.{Auth, AuthProvider, OAuth2}
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend

sealed trait CdfSparkAuth extends Serializable {
  def provider(
      implicit cs: ContextShift[IO],
      clock: Clock[IO],
      sttpBackend: SttpBackend[IO, Nothing]): IO[AuthProvider[IO]]
}

object CdfSparkAuth {
  final case class Static(auth: Auth) extends CdfSparkAuth {
    override def provider(
        implicit cs: ContextShift[IO],
        clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Nothing]): IO[AuthProvider[IO]] =
      IO(AuthProvider(auth))
  }

  final case class OAuth2ClientCredentials(credentials: OAuth2.ClientCredentials) extends CdfSparkAuth {
    override def provider(
        implicit cs: ContextShift[IO],
        clock: Clock[IO],
        sttpBackend: SttpBackend[IO, Nothing]): IO[AuthProvider[IO]] =
      OAuth2.ClientCredentialsProvider[IO](credentials)
  }
}
