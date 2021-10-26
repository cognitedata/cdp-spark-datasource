package cognite.spark.v1

import cats.effect.Sync
import cats.syntax.all._
import com.codahale.metrics.Counter
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
  override def send[T, R >: S with Effect[F]](
      request: Request[T, R]
  ): F[Response[T]] =
    Sync[F].delay(metric.inc()) *> delegate
      .send(request)
      .attempt
      .flatTap {
        case Right(response) =>
          Sync[F].delay(
            responseCounter(response.code).foreach(_.inc())
          )
        case Left(_) =>
          Sync[F].delay(
            failureCounter.foreach(_.inc())
          )
      }
      .rethrow

  override def close(): F[Unit] = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
