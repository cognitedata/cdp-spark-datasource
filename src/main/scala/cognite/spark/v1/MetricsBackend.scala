package cognite.spark.v1

import cats.effect.Sync
import cats.syntax.all._
import com.cognite.sdk.scala.common.{CdpApiException, SdkException}
import sttp.capabilities.Effect
import sttp.client3.{Request, Response, SttpBackend}
import sttp.model.StatusCode
import sttp.monad.MonadError

final case class RequestResponseInfo(
    sttpRequestTags: Map[String, Any],
    status: Option[StatusCode]
) {}

object RequestResponseInfo {
  def create[T, R](request: Request[T, R], status: Option[StatusCode]): RequestResponseInfo =
    RequestResponseInfo(request.tags, status)
}

class MetricsBackend[F[_]: Sync, +S](
    delegate: SttpBackend[F, S],
    callback: RequestResponseInfo => F[Unit]
) extends SttpBackend[F, S] {

  override def send[T, R >: S with Effect[F]](
      request: Request[T, R]
  ): F[Response[T]] = {
    def cb = (status: Option[StatusCode]) => callback(RequestResponseInfo.create(request, status))
    delegate
      .send(request)
      .onError {
        case cdpError: CdpApiException => cb(Some(StatusCode(cdpError.code)))
        case SdkException(_, _, _, Some(code), _) => cb(Some(StatusCode(code)))
        case _ =>
          cb(None)
      }
      .flatTap { response =>
        cb(Some(response.code))
      }
  }

  override def close(): F[Unit] = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
