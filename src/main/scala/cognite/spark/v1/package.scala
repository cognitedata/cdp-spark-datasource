package cognite.spark

import com.cognite.sdk.scala.common.{NonNullableSetter, SdkException, SetNull, SetValue, Setter}
import com.cognite.sdk.scala.v1.{SequenceColumn, SequenceColumnCreate}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.TransformerConfiguration
import io.scalaland.chimney.dsl.TransformerConfiguration.UpdateFlag
import io.scalaland.chimney.internal.runtime.TransformerFlags

package object v1 {
  implicit val chimneyConfiguration
    : UpdateFlag[TransformerFlags.Enable[TransformerFlags.DefaultValues, TransformerFlags.Default]] =
    TransformerConfiguration.default.enableDefaultValues

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.OptionPartial"
    )
  )
  implicit def optionToSetter[T: Manifest]: Transformer[Option[T], Option[Setter[T]]] =
    new Transformer[Option[T], Option[Setter[T]]] {
      override def transform(src: Option[T]) = src match {
        case null => Some(SetNull())
        case None => None
        case Some(null) => Some(SetNull())
        case Some(value: T) => Some(SetValue(value))
        case Some(badValue) =>
          throw new SdkException(
            s"Expected value of type ${manifest[T].toString} but got `${badValue.toString}` of type ${badValue.getClass.toString}"
          )
      }
    }

  implicit def anyToSetter[T]: Transformer[T, Option[Setter[T]]] =
    new Transformer[T, Option[Setter[T]]] {
      override def transform(src: T): Option[Setter[T]] = Setter.fromAny(src)
    }

  implicit def optionToNonNullableSetter[T]: Transformer[Option[T], Option[NonNullableSetter[T]]] =
    new Transformer[Option[T], Option[NonNullableSetter[T]]] {
      override def transform(src: Option[T]): Option[NonNullableSetter[T]] =
        NonNullableSetter.fromOption(src)
    }

  implicit def toNonNullableSetter[T]: Transformer[T, NonNullableSetter[T]] =
    new Transformer[T, NonNullableSetter[T]] {
      override def transform(value: T): NonNullableSetter[T] = NonNullableSetter.fromAny(value)
    }

  implicit def toOptionNonNullableSetter[T]: Transformer[T, Option[NonNullableSetter[T]]] =
    new Transformer[T, Option[NonNullableSetter[T]]] {
      override def transform(value: T): Option[NonNullableSetter[T]] =
        Some(NonNullableSetter.fromAny(value))
    }

  implicit def sequenceToSequenceColumnCreate: Transformer[SequenceColumn, SequenceColumnCreate] =
    new Transformer[SequenceColumn, SequenceColumnCreate] {
      override def transform(seq: SequenceColumn): SequenceColumnCreate =
        SequenceColumnCreate(
          name = seq.name,
          externalId = seq.externalId.getOrElse(throw new RuntimeException("Missing externalId")),
          description = seq.description,
          metadata = seq.metadata,
          valueType = seq.valueType
        )
    }
}
