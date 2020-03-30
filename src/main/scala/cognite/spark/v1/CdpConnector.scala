package cognite.spark.v1

import java.util.concurrent.Executors

import cats.Parallel
import cats.effect.{ContextShift, IO, Timer}
import com.cognite.sdk.scala.common.{GzipSttpBackend, RetryingBackend}
import com.cognite.sdk.scala.v1.GenericClient
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend
import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.SttpClientBackendFactory

import scala.concurrent.ExecutionContext

case class Data[A](data: A)
case class Items[A](items: Seq[A])
case class CdpApiErrorPayload(code: Int, message: String)
case class Error[A](error: A)
case class Login(user: String, loggedIn: Boolean, project: String, projectId: Long)

object CdpConnector {
  // It's important that the threads made here are daemon threads
  // so that we don't hang applications using our library during exit.
  // See for more info https://github.com/cognitedata/cdp-spark-datasource/pull/415/files#r396774391
  @transient lazy val cdpConnectorExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(
        Math.max(Runtime.getRuntime().availableProcessors(), 4) * 2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CDF-Spark-Datasource-%d").build()
      )
    )
  @transient implicit lazy val cdpConnectorTimer: Timer[IO] = IO.timer(cdpConnectorExecutionContext)
  @transient implicit val cdpConnectorContextShift: ContextShift[IO] =
    IO.contextShift(cdpConnectorExecutionContext)
  @transient implicit lazy val cdpConnectorParallel: Parallel[IO, IO.Par] =
    IO.ioParallel(cdpConnectorContextShift)
  private val sttpBackend: SttpBackend[IO, Nothing] =
    new GzipSttpBackend[IO, Nothing](
      AsyncHttpClientCatsBackend.usingClient(SttpClientBackendFactory.create()))

  def retryingSttpBackend(maxRetries: Int): SttpBackend[IO, Nothing] =
    new RetryingBackend[IO, Nothing](sttpBackend, maxRetries = Some(maxRetries))

  def clientFromConfig(config: RelationConfig): GenericClient[IO, Nothing] =
    new GenericClient[IO, Nothing](
      Constants.SparkDatasourceVersion,
      config.projectName,
      config.auth,
      config.baseUrl)(implicitly, retryingSttpBackend(config.maxRetries))

  type DataItems[A] = Data[Items[A]]
  type CdpApiError = Error[CdpApiErrorPayload]

  // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
  // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
  // Since null keys don't make sense to CDP either, remove them as well.
  // Additionally, values are limited to 512 characters, yet we still have data where values have
  // more characters than that, so truncate them to the valid length if required: it's necessary for
  // copying existing data, and probably for upserts as well.
  def filterMetadata(metadata: Option[Map[String, String]]): Option[Map[String, String]] =
    metadata.map(_.filter { case (k, v) => k != null && v != null }
      .mapValues(_.slice(0, Constants.MetadataValuePostMaxLength)))
}
