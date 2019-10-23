package cognite.spark.v1

import scala.language.experimental.macros
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.macros.blackbox.Context

trait StructTypeEncoder[T] {
  def structType(): StructType
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
    val nullable = t <:< weakTypeOf[Option[_]]
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
