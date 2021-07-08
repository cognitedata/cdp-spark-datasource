package cognite.spark.v1

import cats.effect.{Concurrent, ContextShift, IO}
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import com.softwaremill.sttp.{MonadError, Request, Response, SttpBackend}

import scala.language.higherKinds

class RateLimitingBackend[F[_], S](
    delegate: SttpBackend[F, S],
    semaphore: Semaphore[F]
) extends SttpBackend[F, S] {
  override def send[T](
      request: Request[T, S]
  ): F[Response[T]] =
    // This will allow only the specified number of request run in parallel
    // While it's not the smartest way to react to rate limiting, this will
    // actually respond to backpressure when used with the RetryingBackend:
    // The 503 rate limit response is retried, but with a delay. It will also
    // block the semaphore for new requests, so it will reduce the rate
    semaphore.withPermit(delegate.send(request))

  override def close(): Unit = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}

object RateLimitingBackend {
  def apply[S](
      delegate: SttpBackend[IO, S],
      maxParallelRequests: Int
  )(implicit cs: ContextShift[IO]): RateLimitingBackend[IO, S] =
    new RateLimitingBackend[IO, S](
      delegate,
      Semaphore[IO](maxParallelRequests).unsafeRunSync()
    )
}
