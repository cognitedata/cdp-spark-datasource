package cognite.spark.v1

import cats.effect.IO
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.Auth
import com.cognite.sdk.scala.v1._
import com.softwaremill.sttp.SttpBackend
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.sources.BaseRelation

abstract class CdfRelation(config: RelationConfig, shortName: String)
    extends BaseRelation
    with Serializable {
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")
  @transient lazy protected val itemsCreated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.created")
  @transient lazy protected val itemsUpdated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.updated")

  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)

  @transient lazy val client: GenericClient[IO, Nothing] =
    CdpConnector.clientFromConfig(config)

  def incMetrics(counter: Counter, count: Int): IO[Unit] =
    IO(
      if (config.collectMetrics) {
        counter.inc(count)
      }
    )
}
