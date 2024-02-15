package cognite.spark.v1

import cats.effect.IO
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.{NonNullableSetter, SetNull, SetValue, Setter}
import com.cognite.sdk.scala.v1._
import io.scalaland.chimney.Transformer
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.sources.BaseRelation

abstract class CdfRelation(config: RelationConfig, shortNameStr: String)
    extends BaseRelation
    with Serializable {
  protected val shortName: String = shortNameStr
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")
  @transient lazy protected val itemsCreated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.created")
  @transient lazy protected val itemsUpdated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.updated")
  @transient lazy protected val itemsDeleted: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.deleted")
  // We are not aware if it is `created` or `updated in flexible data modelling case
  @transient lazy protected val itemsUpserted: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.upserted")

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  def incMetrics(counter: Counter, count: Int): IO[Unit] =
    IO(
      if (config.collectMetrics) {
        counter.inc(count.toLong)
      }
    )

  // Needed for labels property when transforming from UpsertSchema to Update
  implicit def seqStrToCogIdSetter
    : Transformer[Option[Seq[String]], Option[NonNullableSetter[Seq[CogniteExternalId]]]] =
    new Transformer[Option[Seq[String]], Option[NonNullableSetter[Seq[CogniteExternalId]]]] {
      override def transform(
          src: Option[Seq[String]]): Option[NonNullableSetter[Seq[CogniteExternalId]]] = {
        val labels = src.map(l => l.map(CogniteExternalId(_)))
        NonNullableSetter.fromOption(labels)
      }
    }

  implicit def fieldToSetter[T]: Transformer[OptionalField[T], Option[Setter[T]]] =
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
