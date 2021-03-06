package cognite.spark.v1

import cats.effect.IO
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.{Auth, SetNull, SetValue, Setter}
import com.cognite.sdk.scala.v1._
import com.softwaremill.sttp.SttpBackend
import io.scalaland.chimney.Transformer
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

  implicit def fieldToSetter[T: Manifest]: Transformer[OptionalField[T], Option[Setter[T]]] =
    new Transformer[OptionalField[T], Option[Setter[T]]] {
      override def transform(src: OptionalField[T]): Option[Setter[T]] = src match {
        case FieldSpecified(null) => // scalastyle:ignore null
          throw new Exception(
            "FieldSpecified(null) observed, that's bad. Please reach out to Cognite support.")
        case FieldSpecified(x) => Some(SetValue(x))
        case FieldNotSpecified => None
        case FieldNull if config.ignoreNullFields => None
        case FieldNull => Some(SetNull())
      }
    }
}
