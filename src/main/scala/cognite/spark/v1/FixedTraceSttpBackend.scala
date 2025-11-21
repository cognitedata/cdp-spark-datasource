package cognite.spark.v1

import natchez.Kernel
import sttp.capabilities
import sttp.client3.{Request, Response, SttpBackend}
import sttp.model.Header
import sttp.monad.MonadError

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class FixedTraceSttpBackend[F[_], +P](
    delegate: SttpBackend[F, P],
    kernel: Kernel,
    maxRequests: Option[Long] = None,
    maxTime: Option[Instant] = None)
    extends SttpBackend[F, P] {
  private val requestsMade: AtomicLong = new AtomicLong(0)

  private def shouldPropagate(): Boolean = {
    val reqNo = requestsMade.incrementAndGet()
    val now = Instant.now()
    maxRequests.forall(reqNo <= _) && maxTime.forall(now.compareTo(_) <= 0)
  }

  override def send[T, R >: P with capabilities.Effect[F]](request: Request[T, R]): F[Response[T]] =
    if (shouldPropagate()) {
      delegate.send(
        request.headers(
          kernel.toHeaders.map { case (k, v) => Header(k.toString, v) }.toSeq: _*
        )
      )
    } else {
      delegate.send(request)
    }

  override def close(): F[Unit] = delegate.close()

  override def responseMonad: MonadError[F] = delegate.responseMonad
}
