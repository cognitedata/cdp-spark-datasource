package cognite.spark.v1

import cats.effect.Sync
import cats.syntax.all._
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.{CdpApiException, SdkException}
import sttp.capabilities.Effect
import sttp.client3.{Request, Response, SttpBackend}
import sttp.model.StatusCode
import sttp.monad.MonadError

import scala.language.higherKinds

class MetricsBackend[F[_]: Sync, +S](
    delegate: SttpBackend[F, S],
    metric: Counter,
    responseCounter: StatusCode => Option[Counter] = _ => None,
    failureCounter: => Option[Counter] = None
) extends SttpBackend[F, S] {
  private def incFailure() =
    Sync[F].delay(
      failureCounter.foreach(_.inc())
    )
  private def incResponse(code: StatusCode) =
    Sync[F].delay(
      responseCounter(code).foreach(_.inc())
    )

  override def send[T, R >: S with Effect[F]](
      request: Request[T, R]
  ): F[Response[T]] =
    Sync[F].delay(metric.inc()) *> delegate
      .send(request)
      .onError {
        case cdpError: CdpApiException => incResponse(StatusCode(cdpError.code))
        case SdkException(_, _, _, Some(code)) => incResponse(StatusCode(code))
        case _ =>
          incFailure()
      }
      .flatTap { response =>
        incResponse(response.code)
      }

  override def close(): F[Unit] = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
