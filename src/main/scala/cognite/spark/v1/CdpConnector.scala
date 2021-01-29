package cognite.spark.v1

import java.util.concurrent.Executors

import cats.Parallel
import cats.effect.{ContextShift, IO, Timer}
import com.cognite.sdk.scala.common.{GzipSttpBackend, RetryingBackend}
import com.cognite.sdk.scala.v1.GenericClient
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.SttpClientBackendFactory
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final case class Data[A](data: A)
final case class Items[A](items: Seq[A])
final case class CdpApiErrorPayload(code: Int, message: String)
final case class Error[A](error: A)
final case class Login(user: String, loggedIn: Boolean, project: String, projectId: Long)

object CdpConnector {
  // It's important that the threads made here are daemon threads
  // so that we don't hang applications using our library during exit.
  // See for more info https://github.com/cognitedata/cdp-spark-datasource/pull/415/files#r396774391
  @transient lazy val cdpConnectorExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(
        Math.max(Runtime.getRuntime.availableProcessors(), 4) * 2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CDF-Spark-Datasource-%d").build()
      )
    )
  @transient implicit lazy val cdpConnectorTimer: Timer[IO] = IO.timer(cdpConnectorExecutionContext)
  @transient implicit val cdpConnectorContextShift: ContextShift[IO] =
    IO.contextShift(cdpConnectorExecutionContext)
  @transient implicit lazy val cdpConnectorParallel: Parallel[IO] =
    IO.ioParallel(cdpConnectorContextShift)
  private val sttpBackend: SttpBackend[IO, Nothing] =
    new GzipSttpBackend[IO, Nothing](
      AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  def retryingSttpBackend(maxRetries: Int, maxRetryDelaySeconds: Int): SttpBackend[IO, Nothing] =
    new RetryingBackend[IO, Nothing](
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
