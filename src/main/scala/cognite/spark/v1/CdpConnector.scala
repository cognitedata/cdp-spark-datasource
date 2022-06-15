package cognite.spark.v1

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.sttp.{
  BackpressureThrottleBackend,
  GzipBackend,
  RateLimitingBackend,
  RetryingBackend
}
import com.cognite.sdk.scala.v1.GenericClient
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.datasource.MetricsSource
import org.log4s._
import sttp.client3.SttpBackend
import sttp.client3.asynchttpclient.SttpClientBackendFactory
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.higherKinds

final case class Data[A](data: A)
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
          .setNameFormat("CDF-Spark-compute-%d")
          .build()
      )
    )
  @transient private val (blocking, _) =
    IORuntime.createDefaultBlockingExecutionContext("CDF-Spark-blocking")
  @transient private val (scheduler, _) = IORuntime.createDefaultScheduler("CDF-Spark-scheduler")
  @transient lazy implicit val ioRuntime: IORuntime = IORuntime(
    cdpConnectorExecutionContext,
    blocking,
    scheduler,
    () => (),
    IORuntimeConfig()
  )
  private val sttpBackend: SttpBackend[IO, Any] =
    new GzipBackend[IO, Any](AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  def retryingSttpBackend(
      maxRetries: Int,
      maxRetryDelaySeconds: Int,
      maxParallelRequests: Int = Constants.DefaultParallelismPerPartition,
      metricsPrefix: Option[String] = None
  ): SttpBackend[IO, Any] = {
    val metricsBackend =
      metricsPrefix.fold(sttpBackend)(
        metricsPrefix =>
          new MetricsBackend[IO, Any](
            sttpBackend,
            MetricsSource.getOrCreateCounter(metricsPrefix, "requests"),
            status =>
              if (status.code >= 400) {
                Some(
                  MetricsSource.getOrCreateCounter(metricsPrefix, s"requests.${status.code}.response"))
              } else {
                None
            },
            Some(MetricsSource.getOrCreateCounter(metricsPrefix, s"requests.response.failure"))
        ))
    // this backend throttles when rate limiting from the serivce is encountered
    val makeQueueOf1 = for {
      queue <- Queue.bounded[IO, Unit](1)
      _ <- queue.offer(())
    } yield queue
    val throttledBackend =
      new BackpressureThrottleBackend[IO, Any](
        metricsBackend,
        makeQueueOf1.unsafeRunSync(),
        800.milliseconds)
    val retryingBackend = new RetryingBackend[IO, Any](
      throttledBackend,
      maxRetries = maxRetries,
      maxRetryDelay = maxRetryDelaySeconds.seconds)
    // limit the number of concurrent requests
    val limitedBackend: SttpBackend[IO, Any] =
      RateLimitingBackend[Any](retryingBackend, maxParallelRequests)
    metricsPrefix.fold(limitedBackend)(
      metricsPrefix =>
        new MetricsBackend[IO, Any](
          limitedBackend,
          MetricsSource.getOrCreateCounter(metricsPrefix, "requestsWithoutRetries")))
  }

  def clientFromConfig(config: RelationConfig, cdfVersion: Option[String] = None): GenericClient[IO] = {
    val metricsPrefix = if (config.collectMetrics) {
      Some(config.metricsPrefix)
    } else {
      None
    }
    implicit val sttpBackend: SttpBackend[IO, Any] =
      retryingSttpBackend(
        config.maxRetries,
        config.maxRetryDelaySeconds,
        config.parallelismPerPartition,
        metricsPrefix)
    new GenericClient(
      applicationName = config.applicationName.getOrElse(Constants.SparkDatasourceVersion),
      projectName = config.projectName,
      authProvider = config.auth.provider.unsafeRunSync(),
      baseUrl = config.baseUrl,
      apiVersion = None,
      clientTag = config.clientTag,
      cdfVersion = cdfVersion
    )
  }

  type DataItems[A] = Data[Items[A]]
  type CdpApiError = Error[CdpApiErrorPayload]
}
