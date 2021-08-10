package cognite.spark

import com.cognite.sdk.scala.common.{NonNullableSetter, SdkException, SetNull, SetValue, Setter}
import io.circe.{Encoder, Json}
import io.scalaland.chimney.Transformer
// scalastyle:off
package object v1 {
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.OptionPartial",
      "scalafix:DisableSyntax.null",
      "scalafix:DisableSyntax.!="
    )
  )
  implicit def optionToSetter[T: Manifest]: Transformer[Option[T], Option[Setter[T]]] =
    new Transformer[Option[T], Option[Setter[T]]] {
      override def transform(src: Option[T]) = src match {
        case null => Some(SetNull()) // scalastyle:ignore null
        case None => None
        case Some(null) => Some(SetNull()) // scalastyle:ignore null
        case Some(map: Map[_, _]) if map.isEmpty =>
          // Workaround for CDF-3540 and CDF-953
          None
        case Some(value: T) => Some(SetValue(value))
        case Some(badValue) =>
          throw new SdkException(
            s"Expected value of type ${manifest[T].toString} but got `${badValue.toString}` of type ${badValue.getClass.toString}"
          )
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null", "scalafix:DisableSyntax.null"))
  implicit def anyToSetter[T]: Transformer[T, Option[Setter[T]]] =
    new Transformer[T, Option[Setter[T]]] {
      override def transform(src: T): Option[Setter[T]] = src match {
        case null => Some(SetNull()) // scalastyle:ignore null
        case value => Some(SetValue(value))
      }
    }

  implicit def encodeSetter[T](implicit encodeT: Encoder[T]): Encoder[Setter[T]] =
    new Encoder[Setter[T]] {
      final def apply(a: Setter[T]): Json = a match {
        case SetValue(value) => Json.obj(("set", encodeT.apply(value)))
        case SetNull() => Json.obj(("setNull", Json.True))
      }
    }
  implicit def optionToNonNullableSetter[T]: Transformer[Option[T], Option[NonNullableSetter[T]]] =
    new Transformer[Option[T], Option[NonNullableSetter[T]]] {
      override def transform(src: Option[T]): Option[NonNullableSetter[T]] = src match {
        case None => None
        case Some(map: Map[_, _]) if map.isEmpty =>
          // Workaround for CDF-3540 and CDF-953
          None
        case Some(value) =>
          require(
            value != null,
            "Invalid null value for non-nullable field update"
          ) // scalastyle:ignore null
          Some(SetValue(value))
      }
    }

  implicit def toNonNullableSetter[T]: Transformer[T, NonNullableSetter[T]] =
    new Transformer[T, NonNullableSetter[T]] {
      override def transform(value: T): NonNullableSetter[T] = SetValue(value)
    }

  implicit def toOptionNonNullableSetter[T]: Transformer[T, Option[NonNullableSetter[T]]] =
    new Transformer[T, Option[NonNullableSetter[T]]] {
      override def transform(value: T): Option[NonNullableSetter[T]] = Some(SetValue(value))
    }

  implicit def encodeNonNullableSetter[T](
      implicit encodeT: Encoder[T]
  ): Encoder[NonNullableSetter[T]] = new Encoder[NonNullableSetter[T]] {
    final def apply(a: NonNullableSetter[T]): Json = a match {
      case SetValue(value) => Json.obj(("set", encodeT.apply(value)))
    }
  }
}
