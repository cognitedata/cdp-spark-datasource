package cognite.spark.v1

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
import com.codahale.metrics.Counter
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

  private def getOrCreateCounter(metricsPrefix: String, name: String): Counter = {
    val (stageAttempt, taskAttempt) = {
      val ctx = org.apache.spark.TaskContext.get
      (ctx.stageAttemptNumber, ctx.attemptNumber)
    }
    MetricsSource
      .getOrCreateCounter(metricsPrefix, stageAttempt, taskAttempt, name)
  }

  private def incMetrics(
      metricsPrefix: String,
      namePrefix: String,
      maybeStatus: Option[StatusCode]): IO[Unit] =
    IO.delay(getOrCreateCounter(metricsPrefix, s"${namePrefix}requests").inc()) *>
      (maybeStatus match {
        case None =>
          IO.delay(getOrCreateCounter(metricsPrefix, s"${namePrefix}requests.response.failure").inc())
        case Some(status) if status.code >= 400 =>
          IO.delay(
            getOrCreateCounter(metricsPrefix, s"${namePrefix}requests.${status.code}" + s".response")
              .inc())
        case _ => IO.unit
      })

  def retryingSttpBackend(
      maxRetries: Int,
      maxRetryDelaySeconds: Int,
      maxParallelRequests: Int = Constants.DefaultParallelismPerPartition,
      metricsPrefix: Option[String] = None,
      initialRetryDelayMillis: Int = Constants.DefaultInitialRetryDelay.toMillis.toInt
  ): SttpBackend[IO, Any] = {
    val metricsBackend =
      metricsPrefix.fold(sttpBackend)(
        metricsPrefix =>
          new MetricsBackend[IO, Any](
            sttpBackend, {
              case RequestResponseInfo(tags, maybeStatus) =>
                incMetrics(metricsPrefix, "", maybeStatus) *>
                  tags
                    .get(GenericClient.RESOURCE_TYPE_TAG)
                    .map(service => incMetrics(metricsPrefix, s"${service.toString}.", maybeStatus))
                    .getOrElse(IO.unit)
            }
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
      initialRetryDelay = initialRetryDelayMillis.milliseconds,
      maxRetryDelay = maxRetryDelaySeconds.seconds)
    // limit the number of concurrent requests
    val limitedBackend: SttpBackend[IO, Any] =
      RateLimitingBackend[IO, Any](retryingBackend, maxParallelRequests).unsafeRunSync()
    metricsPrefix.fold(limitedBackend)(
      metricsPrefix =>
        new MetricsBackend[IO, Any](
          limitedBackend,
          _ => IO.delay(getOrCreateCounter(metricsPrefix, "requestsWithoutRetries").inc())
      )
    )
  }

  def clientFromConfig(config: RelationConfig, cdfVersion: Option[String] = None): GenericClient[IO] = {
    val metricsPrefix = if (config.collectMetrics) {
      Some(config.metricsPrefix)
    } else {
      None
    }

    //Use separate backend for auth, so we should not retry as much as maxRetries config
    val authSttpBackend =
      retryingSttpBackend(
        maxRetries = 5,
        initialRetryDelayMillis = config.initialRetryDelayMillis,
        maxRetryDelaySeconds = config.maxRetryDelaySeconds,
        maxParallelRequests = config.parallelismPerPartition,
        metricsPrefix = metricsPrefix
      )
    val authProvider = config.auth.provider(implicitly, authSttpBackend).unsafeRunSync()

    import natchez.Trace.Implicits.noop // TODO: add full tracing
    implicit val sttpBackend: SttpBackend[IO, Any] = {
      new FixedTraceSttpBackend(
        retryingSttpBackend(
          maxRetries = config.maxRetries,
          initialRetryDelayMillis = config.initialRetryDelayMillis,
          maxRetryDelaySeconds = config.maxRetryDelaySeconds,
          maxParallelRequests = config.parallelismPerPartition,
          metricsPrefix = metricsPrefix
        ),
        config.tracingParent
      )
    }

    new GenericClient(
      applicationName = config.applicationName.getOrElse(Constants.SparkDatasourceVersion),
      projectName = config.projectName,
      authProvider = authProvider,
      baseUrl = config.baseUrl,
      apiVersion = None,
      clientTag = config.clientTag,
      cdfVersion = cdfVersion
    )
  }
}
