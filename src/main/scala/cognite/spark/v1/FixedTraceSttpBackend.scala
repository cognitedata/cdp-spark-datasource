package cognite.spark.v1

import natchez.Kernel
import sttp.capabilities
import sttp.client3.{Request, Response, SttpBackend}
import sttp.model.Header
import sttp.monad.MonadError

class FixedTraceSttpBackend[F[_], +P](delegate: SttpBackend[F, P], kernel: Kernel)
    extends SttpBackend[F, P] {
  override def send[T, R >: P with capabilities.Effect[F]](request: Request[T, R]): F[Response[T]] =
    delegate.send(
      request.headers(
        kernel.toHeaders.map { case (k, v) => Header(k.toString, v) }.toSeq: _*
      )
    )

  override def close(): F[Unit] = delegate.close()

  override def responseMonad: MonadError[F] = delegate.responseMonad
}
