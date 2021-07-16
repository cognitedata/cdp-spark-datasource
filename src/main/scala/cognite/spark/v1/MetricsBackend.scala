package cognite.spark.v1

import cats.effect.Sync
import cats.syntax.all._
import com.codahale.metrics.Counter
import sttp.capabilities.Effect
import sttp.client3.{Request, Response, SttpBackend}
import sttp.monad.MonadError

import scala.language.higherKinds

class MetricsBackend[F[_]: Sync, P](
    delegate: SttpBackend[F, P],
    metric: Counter
) extends SttpBackend[F, P] {
  override def send[T, R >: P with Effect[F]](
      request: Request[T, R]
  ): F[Response[T]] =
    Sync[F].delay(metric.inc()) *> delegate.send(request)

  override def close(): F[Unit] = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
