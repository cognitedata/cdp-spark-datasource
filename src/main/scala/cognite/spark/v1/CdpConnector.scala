package cognite.spark.v1

import java.util.concurrent.Executors
import cats.Parallel
import cats.effect.unsafe.{IORuntime, IORuntimeConfig, Scheduler}
import cats.effect.{IO, Temporal}
import com.cognite.sdk.scala.common.{GzipSttpBackend, RetryingBackend}
import com.cognite.sdk.scala.v1.GenericClient
import com.google.common.util.concurrent.ThreadFactoryBuilder
import sttp.client3._
import sttp.client3.asynchttpclient.SttpClientBackendFactory
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import org.log4s._

import java.lang.Thread.UncaughtExceptionHandler
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global

final case class Data[A](data: A)
final case class Items[A](items: Seq[A])
final case class CdpApiErrorPayload(code: Int, message: String)
final case class Error[A](error: A)
final case class Login(user: String, loggedIn: Boolean, project: String, projectId: Long)

object CdpConnector {
  @transient private val logger = getLogger
  // It's important that the threads made here are daemon threads
  // so that we don't hang applications using our library during exit.
  // See for more info https://github.com/cognitedata/cdp-spark-datasource/pull/415/files#r396774391
  @transient lazy val cdpConnectorExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(
        Math.max(Runtime.getRuntime.availableProcessors(), 4) * 2,
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setUncaughtExceptionHandler(new UncaughtExceptionHandler {
            override def uncaughtException(t: Thread, e: Throwable): Unit =
              // Log the exception, and move on.
              logger.warn(e)("Ignoring uncaught exception")
          })
          .setNameFormat("CDF-Spark-Datasource-%d")
          .build()
      )
    )
  private val sttpBackend: SttpBackend[IO, Any] =
    new GzipSttpBackend[IO, Any](
      AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  def retryingSttpBackend(maxRetries: Int, maxRetryDelaySeconds: Int): SttpBackend[IO, Any] =
    new RetryingBackend[IO, Any](
      sttpBackend,
      maxRetries = Some(maxRetries),
      maxRetryDelay = maxRetryDelaySeconds.seconds)

  def clientFromConfig(config: RelationConfig): GenericClient[IO] =
    new GenericClient(
      applicationName = config.applicationName.getOrElse(Constants.SparkDatasourceVersion),
      projectName = config.projectName,
      authProvider = config.auth.provider.unsafeRunSync(),
      baseUrl = config.baseUrl,
      apiVersion = None,
      clientTag = config.clientTag
    )(implicitly, retryingSttpBackend(config.maxRetries, config.maxRetryDelaySeconds))

  type DataItems[A] = Data[Items[A]]
  type CdpApiError = Error[CdpApiErrorPayload]
}
