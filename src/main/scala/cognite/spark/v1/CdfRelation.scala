package cognite.spark.v1

import java.util.concurrent.Executor

import cats.effect.{Clock, IO}
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.Auth
import com.cognite.sdk.scala.v1._
import com.softwaremill.sttp.SttpBackend
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.sources.BaseRelation

import scala.concurrent.ExecutionContext

abstract class CdfRelation(config: RelationConfig, shortName: String)
    extends BaseRelation
    with Serializable {
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")
  @transient lazy protected val itemsCreated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.created")
  @transient lazy protected val itemsUpdated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.updated")
  @transient lazy protected val itemsDeleted: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.deleted")

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  def incMetrics(counter: Counter, count: Int): IO[Unit] =
    IO(
      if (config.collectMetrics) {
        counter.inc(count)
      }
    )
}
