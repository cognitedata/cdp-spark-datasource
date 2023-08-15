package cognite.spark.v1

import cats.effect.Sync
import cats.syntax.all._
import com.cognite.sdk.scala.common.{CdpApiException, SdkException}
import sttp.capabilities.Effect
import sttp.client3.{Request, Response, SttpBackend}
import sttp.model.StatusCode
import sttp.monad.MonadError

class MetricsBackend[F[_]: Sync, +S](
    delegate: SttpBackend[F, S],
    callback: Option[StatusCode] => F[Unit]
) extends SttpBackend[F, S] {

  override def send[T, R >: S with Effect[F]](
      request: Request[T, R]
  ): F[Response[T]] =
    delegate
      .send(request)
      .onError {
        case cdpError: CdpApiException => callback(Some(StatusCode(cdpError.code)))
        case SdkException(_, _, _, Some(code)) => callback(Some(StatusCode(code)))
        case _ =>
          callback(None)
      }
      .flatTap { response =>
        callback(Some(response.code))
      }

  override def close(): F[Unit] = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
