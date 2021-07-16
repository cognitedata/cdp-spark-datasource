package cognite.spark.v1

import cats.effect.IO
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.{Auth, NonNullableSetter, SdkException, SetNull, SetValue, Setter}
import com.cognite.sdk.scala.v1._
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

  // Until Scala SDK adds back support for Chimney
  implicit def fieldToSetter[T: Manifest]: Transformer[OptionalField[T], Option[Setter[T]]] = {
      case FieldSpecified(null) => // scalastyle:ignore null
        throw new Exception(
          "FieldSpecified(null) observed, that's bad. Please reach out to Cognite support.")
      case FieldSpecified(x) => Some(SetValue(x))
      case FieldNotSpecified => None
      case FieldNull if config.ignoreNullFields => None
      case FieldNull => Some(SetNull())
    }

  implicit def optionToSetter[T: Manifest]: Transformer[Option[T], Option[Setter[T]]] = {
      case Some(value: T) => Some(SetValue(value))
      case Some(badValue) =>
        throw new SdkException(
          s"Expected value of type ${manifest[T].toString} but got `${badValue.toString}` of type ${badValue.getClass.toString}"
        )
      case src => Setter.fromOption(src)
    }
  implicit def optionToNonNullableSetter[T]: Transformer[Option[T], Option[NonNullableSetter[T]]] =
    NonNullableSetter.fromOption
  implicit def anyToSetter[T]: Transformer[T, Option[Setter[T]]] =
    Setter.fromAny
  implicit def toNonNullableSetter[T]: Transformer[T, NonNullableSetter[T]] =
    NonNullableSetter.fromAny
  implicit def toOptionNonNullableSetter[T]: Transformer[T, Option[NonNullableSetter[T]]] =
    src => Some(SetValue(src))
}
