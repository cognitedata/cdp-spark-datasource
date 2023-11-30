package cognite.spark.v1

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Sync
import cats.effect.std.Queue
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
import cats.implicits._
import com.cognite.sdk.scala.sttp.{
  BackpressureThrottleBackend,
  GzipBackend,
  RateLimitingBackend,
  RetryingBackend
}
import com.cognite.sdk.scala.v1.GenericClient
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.lightstep.opentelemetry.launcher.{OpenTelemetryConfiguration, Propagator}
import natchez.noop.NoopEntrypoint
import natchez.{EntryPoint, Span}
import natchez.opentelemetry.OpenTelemetry
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
  @transient lazy val tracingEntryPoint: EntryPoint[IO] = {
    sys.env.get("OTEL_COLLECTOR_URL") match {
      case Some(collectorUrl) => {
        val serviceName = sys.env.getOrElse("OTEL_SERVICE_NAME", "cdf-spark-connector")
        val serviceVersion = sys.env.getOrElse("OTEL_SERVICE_VERSION", "latest")
        val otel = OpenTelemetryConfiguration.newBuilder
          .setServiceName(serviceName)
          .setServiceVersion(serviceVersion)
          .setTracesEndpoint(collectorUrl)
          // setPropagator is actually addPropagator
          .setPropagator(Propagator.BAGGAGE)
          .setPropagator(Propagator.TRACE_CONTEXT)
          .buildOpenTelemetry()
          .getOpenTelemetrySdk
        OpenTelemetry.entryPointFor(otel)(Sync[IO]).unsafeRunSync()
      }
      case _ => {
        logger.info("OTEL_COLLECTOR_URL isn't set, using no-op tracing")
        NoopEntrypoint[IO]()
      }
    }
  }
  private val sttpBackend: SttpBackend[TracedIO, Any] = {
    new GzipBackend[TracedIO, Any](
      AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))
  }

  private def incMetrics(
      metricsPrefix: String,
      namePrefix: String,
      maybeStatus: Option[StatusCode]): TracedIO[Unit] =
    TracedIO.delay(MetricsSource.getOrCreateCounter(metricsPrefix, s"${namePrefix}requests").inc()) *>
      (maybeStatus match {
        case None =>
          TracedIO.delay(
            MetricsSource
              .getOrCreateCounter(metricsPrefix, s"${namePrefix}requests.response.failure")
              .inc()
          )
        case Some(status) if status.code >= 400 =>
          TracedIO.delay(
            MetricsSource
              .getOrCreateCounter(
                metricsPrefix,
                s"${namePrefix}requests.${status.code}" +
                  s".response")
              .inc()
          )
        case _ => TracedIO.unit
      })

  def retryingSttpBackend(
      maxRetries: Int,
      maxRetryDelaySeconds: Int,
      maxParallelRequests: Int = Constants.DefaultParallelismPerPartition,
      metricsPrefix: Option[String] = None
  ): TracedIO[SttpBackend[TracedIO, Any]] = {
    val metricsBackend =
      metricsPrefix.fold(sttpBackend)(
        metricsPrefix =>
          new MetricsBackend[TracedIO, Any](
            sttpBackend, {
              case RequestResponseInfo(tags, maybeStatus) =>
                incMetrics(metricsPrefix, "", maybeStatus) *>
                  tags
                    .get(GenericClient.RESOURCE_TYPE_TAG)
                    .map(service => incMetrics(metricsPrefix, s"${service.toString}.", maybeStatus))
                    .getOrElse(TracedIO.unit)
            }
        ))
    // this backend throttles when rate limiting from the serivce is encountered
    val makeQueueOf1 = (for {
      queue <- Queue.bounded[IO, Unit](1)
      _ <- queue.offer(())
    } yield queue).unsafeRunSync().mapK(Kleisli.liftK[IO, Span[IO]])
    val throttledBackend =
      new BackpressureThrottleBackend[TracedIO, Any](metricsBackend, makeQueueOf1, 800.milliseconds)
    val retryingBackend: SttpBackend[TracedIO, Any] = new RetryingBackend[TracedIO, Any](
      throttledBackend,
      maxRetries = maxRetries,
      maxRetryDelay = maxRetryDelaySeconds.seconds)
    // limit the number of concurrent requests
    val limitedBackend: TracedIO[SttpBackend[TracedIO, Any]] =
      RateLimitingBackend[TracedIO, Any](retryingBackend, maxParallelRequests)
        .map(x => x)
    metricsPrefix.fold(limitedBackend)(
      metricsPrefix =>
        limitedBackend.map(
          limitedBackend =>
            new MetricsBackend[TracedIO, Any](
              limitedBackend,
              _ =>
                TracedIO.delay(
                  MetricsSource.getOrCreateCounter(metricsPrefix, "requestsWithoutRetries").inc()
              )
          ))
    )
  }

  def clientFromConfig(
      config: RelationConfig,
      cdfVersion: Option[String] = None): GenericClient[TracedIO] = {
    val metricsPrefix = if (config.collectMetrics) {
      Some(config.metricsPrefix)
    } else {
      None
    }

    //Use separate backend for auth, so we should not retry as much as maxRetries config
    val authSttpBackend = config
      .trace("create retryingSttpBackend for auth")(
        retryingSttpBackend(
          5,
          config.maxRetryDelaySeconds,
          config.parallelismPerPartition,
          metricsPrefix)
      )
      .unsafeRunSync()
    val authProvider = tracingEntryPoint
      .root("authSttpBackend")
      .use(span => config.auth.provider(implicitly, authSttpBackend).run(span))
      .unsafeRunSync()

    implicit val sttpBackend: SttpBackend[TracedIO, Any] =
      config
        .trace("create retryingSttpBackend")(
          retryingSttpBackend(
            config.maxRetries,
            config.maxRetryDelaySeconds,
            config.parallelismPerPartition,
            metricsPrefix))
        .unsafeRunSync()
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
