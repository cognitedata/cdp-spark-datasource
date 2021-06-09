package cognite.spark.v1

import cats.Monad
import io.scalaland.chimney.Transformer

import scala.language.experimental.macros
import org.apache.spark.sql.types.{StructField, StructType}

import scala.annotation.tailrec
import scala.reflect.macros.blackbox.Context

trait StructTypeEncoder[T] {
  def structType(): StructType
}

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
  implicit def fieldToOption[T: Manifest]: Transformer[OptionalField[T], Option[T]] =
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

object StructTypeEncoder {
  implicit def defaultStructTypeEncoder[T]: StructTypeEncoder[T] = macro structTypeEncoder_impl[T]
  // scalastyle:off method.name
  def structTypeEncoder_impl[T: c.WeakTypeTag](c: Context): c.Expr[StructTypeEncoder[T]] = {
    import c.universe._
    val structField = symbolOf[StructField.type].asClass.module
    val structType = symbolOf[StructType.type].asClass.module

    val constructor = weakTypeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val structFields = constructor.paramLists.flatten.map((param: Symbol) => {
      val (dt, nullable) = typeToStructType(c)(param.typeSignature)
      val name = param.name.toString
      q"$structField($name, $dt, $nullable)"
    })
    val body = q"def structType() = $structType(Seq(..$structFields))"
    c.Expr[StructTypeEncoder[T]](q"new StructTypeEncoder[${weakTypeOf[T]}] {..$body}")
  }

  // scalastyle:off cyclomatic.complexity
  private def typeToStructType(c: Context)(t: c.Type): (c.Tree, Boolean) = {
    import c.universe._
    val nullable = t <:< weakTypeOf[Option[_]] || t <:< weakTypeOf[OptionalField[_]]
    val rt = if (nullable) {
      t.typeArgs.head // Fetch the type param of Option[_]
    } else {
      t
    }

    val dt = rt match {
      case x if x =:= weakTypeOf[Boolean] => q"DataTypes.BooleanType"
      case x if x =:= weakTypeOf[Byte] => q"DataTypes.ByteType"
      case x if x =:= weakTypeOf[Short] => q"DataTypes.ShortType"
      case x if x =:= weakTypeOf[Int] => q"DataTypes.IntegerType"
      case x if x =:= weakTypeOf[Long] => q"DataTypes.LongType"
      case x if x =:= weakTypeOf[Float] => q"DataTypes.FloatType"
      case x if x =:= weakTypeOf[Double] => q"DataTypes.DoubleType"
      case x if x =:= weakTypeOf[String] => q"DataTypes.StringType"
      case x if x <:< weakTypeOf[Map[_, _]] =>
        val (valueDt, valueNullable) = typeToStructType(c)(rt.typeArgs(1))
        val keyDt = typeToStructType(c)(rt.typeArgs.head)._1
        q"DataTypes.createMapType($keyDt, $valueDt, $valueNullable)"
      case x if x <:< weakTypeOf[Seq[_]] || x <:< weakTypeOf[Array[_]] =>
        val (valueDt, valueNullable) = typeToStructType(c)(rt.typeArgs.head)
        q"DataTypes.createArrayType($valueDt, $valueNullable)"
      case x if x <:< weakTypeOf[java.time.Instant] => q"DataTypes.TimestampType"
      case _ => q"implicitly[StructTypeEncoder[$rt]].structType()"
    }
    (dt, nullable)
  }
}
