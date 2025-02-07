package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import sttp.client3.impl.cats.implicits.asyncMonadError
import sttp.client3.testing.SttpBackendStub
import sttp.client3.{HttpError, SttpClientException, UriContext, basicRequest}
import sttp.model.StatusCode
import sttp.monad.MonadAsyncError

class SyncCursorCallbackTest extends FlatSpec with Matchers with EitherValues {
  it should "not throw on error response" in {
    val response = SyncCursorCallback
      .sendCursorCallbackRequest(
        "localhost",
        "name",
        "value",
        "job",
        SttpBackendStub(implicitly[MonadAsyncError[IO]]).whenAnyRequest.thenRespondServerError())
      .unsafeRunSync()

    response.left.value shouldBe (HttpError("Internal server error", StatusCode.InternalServerError))
  }

  it should "should not throw on connection error" in {
    val connectionException =
      new SttpClientException.ConnectException(basicRequest.get(uri"localhost"), new RuntimeException)
    val response = SyncCursorCallback
      .sendCursorCallbackRequest(
        "localhost",
        "name",
        "value",
        "job",
        SttpBackendStub(implicitly[MonadAsyncError[IO]]).whenAnyRequest
          .thenRespond(Left(connectionException))
      )
      .unsafeRunSync()

    response.left.value shouldBe (connectionException)
  }
}
