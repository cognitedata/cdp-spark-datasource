package cognite.spark.v1

import com.cognite.sdk.scala.common.{NonNullableSetter, Setter}
import com.cognite.sdk.scala.v1.{SequenceColumn, SequenceColumnCreate}
import io.scalaland.chimney.Transformer

object ChimneyConverters {
  implicit def optionToSetter[T: Manifest]: Transformer[Option[T], Option[Setter[T]]] =
    (src: Option[T]) => Setter.fromOption(src)

  implicit def anyToSetter[T]: Transformer[T, Option[Setter[T]]] =
    (src: T) => Setter.fromAny(src)

  implicit def anyToNonNullableSetter[T]: Transformer[T, NonNullableSetter[T]] =
    (src: T) => NonNullableSetter.fromAny(src)

  implicit val sequenceColumnToCreateTransformer: Transformer[SequenceColumn, SequenceColumnCreate] =
    Transformer
      .define[SequenceColumn, SequenceColumnCreate]
      .withFieldComputed(
        _.externalId,
        r => r.externalId.getOrElse(throw new RuntimeException("Missing externalId")))
      .buildTransformer
}
