package cognite.spark.v1

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
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
import sttp.model.StatusCode

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.apache.spark.TaskContext

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

  implicit class ExtensionMethods[A](f: IO[A]) {
    // Run on unbound blocking thread pool
    // Useful to reduce chances of deadlocks in class initializers that are initiated
    // from non-cats-effect code or not being wrapped into IO().
    // Compute (bounded) pool may still be running .unsafeRunBlocking() indirectly and have compute
    // thread blocked until the run completes, but hopefully being requested to run on blocking
    // unbound pool it won't have a wait dependency on anything to be run on compute pool
    def unsafeRunBlocking(): A = f.evalOn(blocking).unsafeRunSync()(ioRuntime)
  }

  private val sttpBackend: SttpBackend[IO, Any] =
    new GzipBackend[IO, Any](AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  private def incMetrics(
      metricsTrackAttempts: Boolean,
      metricsPrefix: String,
      namePrefix: String,
      maybeStatus: Option[StatusCode]): IO[Unit] =
    IO.delay(
      MetricsSource
        .getOrCreateAttemptTrackingCounter(
          metricsTrackAttempts,
          metricsPrefix,
          s"${namePrefix}requests",
          Option(TaskContext.get()))
        .inc()) *>
      (maybeStatus match {
        case None =>
          IO.delay(
            MetricsSource
              .getOrCreateAttemptTrackingCounter(
                metricsTrackAttempts,
                metricsPrefix,
                s"${namePrefix}requests.response.failure",
                Option(TaskContext.get()))
              .inc()
          )
        case Some(status) if status.code >= 400 =>
          IO.delay(
            MetricsSource
              .getOrCreateAttemptTrackingCounter(
                metricsTrackAttempts,
                metricsPrefix,
                s"${namePrefix}requests.${status.code}" +
                  s".response",
                Option(TaskContext.get()))
              .inc()
          )
        case _ => IO.unit
      })

  private def throttlingSttpBackend(sttpBackend: SttpBackend[IO, Any]): SttpBackend[IO, Any] = {
    // this backend throttles all requests when rate limiting from the service is encountered
    val makeQueueOf1 = for {
      queue <- Queue.bounded[IO, Unit](1)
      _ <- queue.offer(())
    } yield queue
    new BackpressureThrottleBackend[IO, Any](
      sttpBackend,
      makeQueueOf1.unsafeRunBlocking(),
      800.milliseconds)
  }

  // package-private for tests
  private[v1] def retryingSttpBackend(
      metricsTrackAttempts: Boolean,
      maxRetries: Int,
      maxRetryDelaySeconds: Int,
      maxParallelRequests: Int = Constants.DefaultParallelismPerPartition,
      metricsPrefix: Option[String] = None,
      initialRetryDelayMillis: Int = Constants.DefaultInitialRetryDelay.toMillis.toInt,
      useSharedThrottle: Boolean = false
  ): SttpBackend[IO, Any] = {
    val metricsBackend =
      metricsPrefix.fold(sttpBackend)(
        metricsPrefix =>
          new MetricsBackend[IO, Any](
            sttpBackend, {
              case RequestResponseInfo(tags, maybeStatus) =>
                incMetrics(metricsTrackAttempts, metricsPrefix, "", maybeStatus) *>
                  tags
                    .get(GenericClient.RESOURCE_TYPE_TAG)
                    .map(
                      service =>
                        incMetrics(
                          metricsTrackAttempts,
                          metricsPrefix,
                          s"${service.toString}.",
                          maybeStatus))
                    .getOrElse(IO.unit)
            }
        ))
    val throttledBackend =
      if (!useSharedThrottle) {
        metricsBackend
      } else {
        throttlingSttpBackend(metricsBackend)
      }
    val retryingBackend = new RetryingBackend[IO, Any](
      throttledBackend,
      maxRetries = maxRetries,
      initialRetryDelay = initialRetryDelayMillis.milliseconds,
      maxRetryDelay = maxRetryDelaySeconds.seconds)
    // limit the number of concurrent requests
    val limitedBackend: SttpBackend[IO, Any] =
      RateLimitingBackend[IO, Any](retryingBackend, maxParallelRequests).unsafeRunBlocking()
    metricsPrefix.fold(limitedBackend)(
      metricsPrefix =>
        new MetricsBackend[IO, Any](
          limitedBackend,
          _ =>
            IO.delay(
              MetricsSource
                .getOrCreateAttemptTrackingCounter(
                  metricsTrackAttempts,
                  metricsPrefix,
                  "requestsWithoutRetries",
                  Option(TaskContext.get()))
                .inc()
          )
      )
    )
  }

  def clientFromConfig(config: RelationConfig, cdfVersion: Option[String] = None): GenericClient[IO] = {
    val metricsPrefix = if (config.collectMetrics) {
      Some(config.metricsPrefix)
    } else {
      None
    }

    import natchez.Trace.Implicits.noop // TODO: add full tracing

    //Use separate backend for auth, so we should not retry as much as maxRetries config
    val authSttpBackend =
      new FixedTraceSttpBackend(
        retryingSttpBackend(
          config.metricsTrackAttempts,
          maxRetries = 5,
          initialRetryDelayMillis = config.initialRetryDelayMillis,
          maxRetryDelaySeconds = config.maxRetryDelaySeconds,
          maxParallelRequests = config.parallelismPerPartition,
          metricsPrefix = metricsPrefix
        ),
        config.tracingConfig.tracingParent,
        config.tracingConfig.maxRequests,
        config.tracingConfig.maxTime,
      )
    val authProvider = config.auth.provider(implicitly, authSttpBackend).unsafeRunBlocking()

    val sttpBackend: SttpBackend[IO, Any] = {
      new FixedTraceSttpBackend(
        retryingSttpBackend(
          config.metricsTrackAttempts,
          maxRetries = config.maxRetries,
          initialRetryDelayMillis = config.initialRetryDelayMillis,
          maxRetryDelaySeconds = config.maxRetryDelaySeconds,
          maxParallelRequests = config.parallelismPerPartition,
          metricsPrefix = metricsPrefix
        ),
        config.tracingConfig.tracingParent,
        config.tracingConfig.maxRequests,
        config.tracingConfig.maxTime,
      )
    }

    new GenericClient(
      applicationName = config.applicationName.getOrElse(Constants.SparkDatasourceVersion),
      projectName = config.projectName,
      authProvider = authProvider,
      baseUrl = config.baseUrl,
      apiVersion = None,
      clientTag = config.clientTag,
      cdfVersion = cdfVersion,
      sttpBackend = sttpBackend,
      wrapSttpBackend = identity
    )
  }
}
