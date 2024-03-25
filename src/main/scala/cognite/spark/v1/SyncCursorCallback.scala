package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.sttp.GzipBackend
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.client3.{SttpBackend, basicRequest}
import sttp.client3.asynchttpclient.SttpClientBackendFactory
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import sttp.client3.circe.{asJson, circeBodySerializer}
import sttp.model.{MediaType, Uri}

case class IncrementalCursor(name: String, value: String)
case class IncrementalCursorResponse(name: String, value: String, updated: Boolean)

object SyncCursorCallback {
  @transient private lazy val sttpBackend: SttpBackend[IO, Any] =
    new GzipBackend[IO, Any](AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  implicit val incrementalCursorCodec: Codec[IncrementalCursor] = deriveCodec[IncrementalCursor]
  implicit val incrementalCursorResponseCodec: Codec[IncrementalCursorResponse] =
    deriveCodec[IncrementalCursorResponse]

  def lastCursorCallback(
      callbackUrl: String,
      cursorName: String,
      cursorValue: String,
      jobId: String): IO[IncrementalCursorResponse] = {
    val uri = Uri.parse(s"$callbackUrl/$jobId") match {
      case Left(value) => throw new IllegalArgumentException(s"Failed to parse URI '$value'")
      case Right(value) => value
    }

    basicRequest
      .followRedirects(false)
      .contentType(MediaType.ApplicationJson)
      .acceptEncoding(MediaType.ApplicationJson.toString())
      .body(IncrementalCursor(cursorName, cursorValue))
      .post(uri)
      .response(asJson[IncrementalCursorResponse])
      .mapResponse {
        // We do not fail the whole transformation on a non-submitted cursor, as it is
        // not critical for this job. Multiple failed runs so that the customer exceeds
        // its max age, will lead to a fallback to restart.
        case Left(_) => IncrementalCursorResponse(cursorName, cursorValue, updated = false)
        case Right(value) => value
      }
      .send(sttpBackend)
      .map(_.body)
  }
}
