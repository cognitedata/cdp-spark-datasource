package cognite.spark.v1

import scala.language.experimental.macros
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import com.cognite.sdk.scala.common.{NonNullableSetter, Setter}

import scala.reflect.macros.blackbox.Context
import scala.util.{Failure, Success, Try}

class SparkSchemaHelperImpl(val c: Context) {
  def asRow[T: c.WeakTypeTag](x: c.Expr[T]): c.Expr[Row] = {
    import c.universe._

    val seqAny = typeOf[Seq[Any]]
    // says if the type can be handled by Spark on itself or we should "help" by recursively applying the asRow macro
    def isPrimitive(t: c.Type): Boolean = {
      if (t <:< seqAny) {
        isPrimitive(t.typeArgs.head)
      } else {
        val fullName = t.typeSymbol.fullName
        fullName.startsWith("scala.") || fullName.startsWith("java.")
      }
    }

    val constructor = weakTypeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val params = constructor.paramLists.flatten
      .map(param => {
        val baseExpr = q"$x.${param.name.toTermName}"
        def toSqlTimestamp(expr: c.Tree) = q"java.sql.Timestamp.from($expr)"
        val valueType = param.typeSignature
        if (valueType <:< weakTypeOf[java.time.Instant]) {
          q"${toSqlTimestamp(baseExpr)}"
        } else if (valueType <:< weakTypeOf[Option[java.time.Instant]]) {
          q"$baseExpr.map(a => ${toSqlTimestamp(q"a")}).orNull"
        } else if (valueType <:< weakTypeOf[Option[_]]) {
          q"$baseExpr.orNull"
        } else if (isPrimitive(valueType)) {
          q"$baseExpr"
        } else if (valueType <:< typeOf[Seq[Any]]) {
          q"$baseExpr.map(a => cognite.spark.v1.SparkSchemaHelper.asRow(a))"
        } else {
          q"cognite.spark.v1.SparkSchemaHelper.asRow($baseExpr)"
        }
      })

    val row = symbolOf[Row.type].asClass.module
    c.Expr[Row](q"$row(..$params)")
  }

  // scalastyle:off
  def fromRow[T: c.WeakTypeTag](row: c.Expr[Row]): c.Expr[T] = {
    import c.universe._
    val optionType = typeOf[Option[_]]
    val optionSeq = typeOf[Seq[Option[_]]]
    val optionSetter = typeOf[Option[Setter[_]]]
    val optionNonNullableSetter = typeOf[Option[NonNullableSetter[_]]]
    val setterType = symbolOf[Setter.type].asClass.module
    val nonNullableSetterType = symbolOf[NonNullableSetter.type].asClass.module
    val optionalFieldType = typeOf[OptionalField[_]]
    val seqAny = typeOf[Seq[Any]]
    val mapAny = typeOf[Map[Any, Any]]
    val mapString = typeOf[Map[String, String]]
    val seqRow = typeOf[Seq[Row]]

    def fromRowRecurse(structType: Type, r: c.Expr[Row]): c.Tree = {
      val constructor = structType.decl(termNames.CONSTRUCTOR).asMethod
      val params = constructor.paramLists.flatten.map((param: Symbol) => {
        val name = param.name.toString
        val paramType = param.typeSignature
        val isOuterOption = paramType <:< optionType
        if (paramType <:< optionNonNullableSetter || paramType <:< optionSetter) {
          throw new Exception(s"scala SDK Setters are no longer supported in Schema: $param : ${param.typeSignature}")
        }
        val isOptionalField = paramType <:< optionalFieldType

        val innerType = if (isOuterOption || isOptionalField) {
          paramType.typeArgs.head
        } else {
          paramType
        }
        val isSequenceWithOptionalVal = innerType <:< optionSeq
        val isSequenceOfAny = innerType <:< seqAny
        val isMapOfString = innerType <:< mapString
        if (!isMapOfString && innerType.getClass == classOf[Map[_, _]]) {
          throw new Exception(s"Only Map[String, String] is supported, not $innerType")
        }

        val rowType = if (isSequenceWithOptionalVal) {
          seqAny
        } else if (isMapOfString) {
          mapAny
        } else if (isSequenceOfAny) {
          innerType.typeArgs.head match {
            case x if x =:= typeOf[String] => seqAny
            case x if x =:= typeOf[Int] => seqAny
            case x if x =:= typeOf[Boolean] => seqAny
            case x if x =:= typeOf[Long] => seqAny
            case _ => seqRow
          }
        } else {
          innerType
        }

        val throwError =
          q"throw cognite.spark.v1.SparkSchemaHelperRuntime.badRowError($r, $name, ${innerType.toString}, ${structType.toString})"

        def handleFieldValue(value: Tree) =
          if (rowType == typeOf[Double]) {
            // do implicit conversion to double
            q"""($value match {
             case x: Double => x
             case x: Int => x.toDouble
             case x: Float => x.toDouble
             case x: Long => x.toDouble
             case x: BigDecimal => x.toDouble
             case x: BigInt => x.toDouble
             case x: java.math.BigDecimal => x.doubleValue
             case x: java.math.BigInteger => x.doubleValue
             case _ => $throwError
           })"""
          } else if (rowType == typeOf[Float]) {
            // do implicit conversion to float
            q"""($value match {
             case x: Double => x.toFloat
             case x: Int => x.toFloat
             case x: Float => x
             case x: Long => x.toFloat
             case x: BigDecimal => x.toFloat
             case x: BigInt => x.toFloat
             case x: java.math.BigDecimal => x.floatValue
             case x: java.math.BigInteger => x.floatValue
             case _ => $throwError
           })"""
          } else if (rowType == typeOf[Long]) {
            q"""($value match {
             case x: Long => x
             case x: Int => x: Long
             case _ => $throwError
           })"""
          } else if (rowType == typeOf[java.time.Instant]) {
            q"""($value match {
             case x: java.time.Instant => x
             case x: java.sql.Timestamp => x.toInstant()
             case _ => $throwError
           })"""
          } else if (rowType == mapAny) {
            q"""($value match {
             case x: scala.collection.immutable.Map[Any @unchecked, Any @unchecked] => x: scala.collection.immutable.Map[Any,Any]
             case _ => $throwError
           })"""
          } else {
            q"""($value match {
             case x: $rowType => x
             case _ => $throwError
           })"""
          }

        val column =
          q"""(scala.util.Try($r.getAs[Any]($name)) match {
             case scala.util.Success(null) => cognite.spark.v1.FieldNull: cognite.spark.v1.OptionalField[$rowType]
             case scala.util.Failure(_) => cognite.spark.v1.FieldNotSpecified: cognite.spark.v1.OptionalField[$rowType]
             case scala.util.Success(x) => cognite.spark.v1.FieldSpecified(${handleFieldValue(q"x")}): cognite.spark.v1.OptionalField[$rowType]
           })"""

        val mappedColumn =
          if (isMapOfString) {
            q"$column.map(cognite.spark.v1.SparkSchemaHelperRuntime.checkMetadataMap(_, $row))"
          } else if (isSequenceWithOptionalVal) {
            q"$column.map(_.map(Option(_)))"
          } else if (rowType <:< seqRow) {
            val x = innerType.typeArgs.head
            q"$column.map(x => x.map(y => ${fromRowRecurse(x, c.Expr[Row](q"y"))}))"
          } else {
            column
          }

        val resExpr =
          if (isOptionalField) {
            mappedColumn
          } else if (isOuterOption) {
            q"$mappedColumn.toOption"
          } else {
            q"$mappedColumn.getOrElse($throwError)"
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
