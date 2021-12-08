package cognite.spark.v1

import cats.Applicative
import cats.effect.{Concurrent, IO, Timer}
import cats.effect.concurrent.{MVar, MVar2}
import cats.syntax.all._
import com.cognite.sdk.scala.common.{CdpApiException, SdkException}
import sttp.capabilities.Effect
import sttp.client3.{Request, Response, SttpBackend}
import sttp.monad.MonadError

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

/** When 429 Too many requests or 503 Service Unavailable error is encountered, new requests are blocked for the specified duration */
class BackpressureThrottleBackend[F[_]: Concurrent: Timer, +S](
    delegate: SttpBackend[F, S],
    delay: FiniteDuration
) extends SttpBackend[F, S] {

  private val permit: MVar2[F, Unit] = MVar.in[IO, F, Unit](()).unsafeRunSync()

  private def processResponse(code: Int) =
    if (code == 429 || code == 503) {
      // try to take the permit and release it after the specified delay
      permit.tryTake.flatMap {
        case None => Applicative[F].unit
        case Some(_) =>
          Timer[F].sleep(delay) *>
            permit.tryPut(()).void
      }
    } else {
      Applicative[F].unit
    }

  override def send[T, R >: S with Effect[F]](
      request: Request[T, R]
  ): F[Response[T]] =
    permit.read *>
      delegate
        .send(request)
        .onError {
          case cdpError: CdpApiException => processResponse(cdpError.code)
          case SdkException(_, _, _, Some(code)) => processResponse(code)
        }
        .flatTap { response =>
          processResponse(response.code.code)
        }

  override def close(): F[Unit] = delegate.close()
  override def responseMonad: MonadError[F] = delegate.responseMonad
}
