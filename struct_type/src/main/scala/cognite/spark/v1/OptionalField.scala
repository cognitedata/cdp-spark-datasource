package cognite.spark.v1

import cats.Monad
import io.scalaland.chimney.Transformer

import scala.annotation.tailrec

sealed trait OptionalField[+T] {
  def toOption: Option[T]
  def flatMap[B](f: T => OptionalField[B]): OptionalField[B]
  def map[B](f: T => B): OptionalField[B]
  @inline
  final def getOrElse[B >: T](default: => B): B = toOption.getOrElse(default)
}
final case class FieldSpecified[+T](value: T) extends OptionalField[T] {
  override def toOption: Option[T] =
    if (value == null) { throw new Exception("FieldSpecified(null) observed, that's bad. Please reach out to Cognite support.") } else { Some(value) }
  override def flatMap[B](f: T => OptionalField[B]): OptionalField[B] = f(value)
  override def map[B](f: T => B): OptionalField[B] = FieldSpecified(f(value))
}
case object FieldNotSpecified extends OptionalField[Nothing] {
  override def toOption: Option[Nothing] = None
  override def flatMap[B](f: Nothing => OptionalField[B]): OptionalField[B] = this
  override def map[B](f: Nothing => B): OptionalField[B] = this
}
case object FieldNull extends OptionalField[Nothing] {
  override def toOption: Option[Nothing] = None
  override def flatMap[B](f: Nothing => OptionalField[B]): OptionalField[B] = this
  override def map[B](f: Nothing => B): OptionalField[B] = this
}

object OptionalField {
  implicit def fieldToOption[T]: Transformer[OptionalField[T], Option[T]] =
    new Transformer[OptionalField[T], Option[T]] {
      override def transform(src: OptionalField[T]): Option[T] = src.toOption
    }

  implicit val monad: Monad[OptionalField] = new Monad[OptionalField] {
    override def flatMap[A, B](fa: OptionalField[A])(f: A => OptionalField[B]): OptionalField[B] = fa.flatMap(f)

    @tailrec
    override def tailRecM[A, B](a: A)(f: A => OptionalField[Either[A, B]]): OptionalField[B] =
      f(a) match {
        case FieldNull => FieldNull
        case FieldNotSpecified => FieldNotSpecified
        case FieldSpecified(Right(x)) => pure(x)
        case FieldSpecified(Left(x)) => tailRecM(x)(f)
      }

    override def pure[A](x: A): OptionalField[A] = FieldSpecified(x)
  }
}

