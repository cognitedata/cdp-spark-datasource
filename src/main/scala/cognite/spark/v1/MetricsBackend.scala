package cognite.spark.v1

import cats.effect.Sync
import cats.syntax.all._
import com.codahale.metrics.Counter
import com.softwaremill.sttp.{MonadError, Request, Response, SttpBackend}

import scala.language.higherKinds

class MetricsBackend[F[_]: Sync, S](
    delegate: SttpBackend[F, S],
    metric: Counter
) extends SttpBackend[F, S] {
  override def send[T](
      request: Request[T, S]
  ): F[Response[T]] =
    Sync[F].delay(metric.inc()) *> delegate.send(request)

  override def close(): Unit = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
