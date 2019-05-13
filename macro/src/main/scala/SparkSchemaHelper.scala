package com.cognite.spark.datasource

import scala.language.experimental.macros
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.reflect.macros.blackbox.Context

class SparkSchemaHelperImpl(val c: Context) {
  def asRow[T: c.WeakTypeTag](x: c.Expr[T]): c.Expr[Row] = {
    import c.universe._

    val constructor = weakTypeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val params = constructor.paramLists.flatten.map(param => q"$x.${param.name.toTermName}")

    val row = symbolOf[Row.type].asClass.module
    c.Expr[Row](q"$row(..$params)")
  }

  def fromRow[T: c.WeakTypeTag](r: c.Expr[Row]): c.Expr[T] = {
    import c.universe._

    val constructor = weakTypeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val params = constructor.paramLists.flatten.map((param: Symbol) => {
      val name = param.name.toString
      val innerType = if (param.typeSignature <:< typeOf[Option[_]]) {
        param.typeSignature.typeArgs.head
      } else {
        param.typeSignature
      }

      val isSequenceWithOptionalVal = innerType <:< typeOf[Seq[Option[_]]]
      val isMapWithOptionalVal = innerType <:< typeOf[Map[_, Option[_]]]
      val rowType = if (isSequenceWithOptionalVal) {
        typeOf[Seq[Any]]
      } else if (isMapWithOptionalVal) {
        typeOf[Map[Any, Any]]
      } else {
        innerType
      }

      val column = q"Option($r.getAs[$rowType]($name))"
      val (baseExpr, isOuterOption) = if (param.typeSignature <:< typeOf[Option[_]]) {
        (column, true)
      } else {
        (q"""$column.getOrElse(throw new IllegalArgumentException("Row is missing required column named '" + $name + "'."))""", false)
      }

      val resExpr = if (isMapWithOptionalVal) {
        if (isOuterOption) {
          q"$baseExpr.map(_.mapValues(Option(_)))"
        } else {
          q"$baseExpr.mapValues(Option(_))"
        }
      } else if (isSequenceWithOptionalVal) {
        if (isOuterOption) {
          q"$baseExpr.map(_.map(Option(_)))"
        } else {
          q"$baseExpr.map(Option(_))"
        }
      } else {
        baseExpr
      }
      q"$resExpr.asInstanceOf[${param.typeSignature}]"
    })
    c.Expr(q"new ${weakTypeOf[T]}(..$params)")
  }
}

object SparkSchemaHelper {
  def structType[T]()(implicit encoder: StructTypeEncoder[T]): StructType = {
    encoder.structType()
  }

  def asRow[T](x: T): Row = macro SparkSchemaHelperImpl.asRow[T]
  def fromRow[T](r: Row): T = macro SparkSchemaHelperImpl.fromRow[T]
}
