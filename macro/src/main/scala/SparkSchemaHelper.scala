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

  // scalastyle:off
  def fromRow[T: c.WeakTypeTag](row: c.Expr[Row]): c.Expr[T] = {
    import c.universe._
    val optionType = typeOf[Option[_]]
    val optionSeq = typeOf[Seq[Option[_]]]
    val optionMap = typeOf[Map[_, Option[_]]]
    val seqAny = typeOf[Seq[Any]]
    val mapAny = typeOf[Map[Any, Any]]
    val seqRow = typeOf[Seq[Row]]

    def fromRowRecurse(structType: Type, r: c.Expr[Row]): c.Tree = {
      val constructor = structType.decl(termNames.CONSTRUCTOR).asMethod
      val params = constructor.paramLists.flatten.map((param: Symbol) => {
        val name = param.name.toString
        val isOuterOption = param.typeSignature <:< optionType
        val innerType = if (isOuterOption) {
          param.typeSignature.typeArgs.head
        } else {
          param.typeSignature
        }
        val isSequenceWithOptionalVal = innerType <:< optionSeq
        val isMapWithOptionalVal = innerType <:< optionMap
        val isSequenceOfAny = innerType <:< seqAny
        val isMapOfAny = innerType <:< mapAny

        val rowType = if (isSequenceWithOptionalVal) {
          seqAny
        } else if (isSequenceOfAny) {
          innerType.typeArgs.head match {
            case x if x =:= typeOf[String] => seqAny
            case x if x =:= typeOf[Int] => seqAny
            case x if x =:= typeOf[Boolean] => seqAny
            case x if x =:= typeOf[Long] => seqAny
            case _ => seqRow
          }
        } else if (isMapOfAny || isMapWithOptionalVal) {
          mapAny
        } else {
          innerType
        }

        val column = q"scala.util.Try(Option($r.getAs[$rowType]($name))).toOption.flatten"
        val baseExpr =
          if (isOuterOption) { column } else {
            q"""$column.getOrElse(throw new IllegalArgumentException("Row is missing required column named '" + $name + "'."))"""
          }

        val resExpr =
          if (isMapWithOptionalVal) {
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
          } else if (rowType <:< seqRow) {
            val x = innerType.typeArgs.head
            if (isOuterOption) {
              q"$baseExpr.map(x => x.map(y => ${fromRowRecurse(x, c.Expr[Row](q"y"))}))"
            } else {
              q"$baseExpr.map(y => ${fromRowRecurse(x, c.Expr[Row](q"y"))})"
            }
          } else {
            baseExpr
          }
        q"$resExpr.asInstanceOf[${param.typeSignature}]"
      })
      q"new ${structType}(..$params)"
    }
    c.Expr[T](fromRowRecurse(weakTypeOf[T], row))
  }
  // scalastyle:on
}

object SparkSchemaHelper {
  def structType[T]()(implicit encoder: StructTypeEncoder[T]): StructType =
    encoder.structType()

  def asRow[T](x: T): Row = macro SparkSchemaHelperImpl.asRow[T]
  def fromRow[T](row: Row): T = macro SparkSchemaHelperImpl.fromRow[T]
}
