package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
import com.cognite.sdk.scala.sttp.GzipBackend
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.log4s.getLogger
import sttp.client3.{SttpBackend, basicRequest}
import sttp.client3.asynchttpclient.SttpClientBackendFactory
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import sttp.client3.circe.{asJson, circeBodySerializer}
import sttp.model.{MediaType, Uri}

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

case class IncrementalCursor(name: String, value: String)
case class IncrementalCursorResponse(name: String, value: String, updated: Boolean)

object SyncCursorCallback {
  @transient private val logger = getLogger
  @transient private lazy val sttpBackend: SttpBackend[IO, Any] =
    new GzipBackend[IO, Any](AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  @transient private lazy val syncCursorCallbackExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(
        2,
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setUncaughtExceptionHandler(new UncaughtExceptionHandler {
            override def uncaughtException(t: Thread, e: Throwable): Unit =
              logger.warn(e)(
                s"Ignoring uncaught exception when submitting cursor callback: ${e.getMessage}")
          })
          .setNameFormat("Cursor-Callback-%d")
          .build()
      )
    )

  @transient private lazy val (blocking, _) =
    IORuntime.createDefaultBlockingExecutionContext("Cursor-Callback-blocking")
  @transient private lazy val (scheduler, _) =
    IORuntime.createDefaultScheduler("Cursor-Callback-scheduler")
  @transient lazy implicit val ioRuntime: IORuntime = IORuntime(
    syncCursorCallbackExecutionContext,
    blocking,
    scheduler,
    () => (),
    IORuntimeConfig()
  )

  implicit val incrementalCursorCodec: Codec[IncrementalCursor] = deriveCodec[IncrementalCursor]
  implicit val incrementalCursorResponseCodec: Codec[IncrementalCursorResponse] =
    deriveCodec[IncrementalCursorResponse]

  def lastCursorCallback(
      callbackUrl: String,
      cursorName: String,
      cursorValue: String,
      jobId: String): IncrementalCursorResponse = {
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
      .unsafeRunSync()
      .body
  }
}
