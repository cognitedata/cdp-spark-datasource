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

    val constructor = weakTypeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val params = constructor.paramLists.flatten
      .map(param => {
        val baseExpr = q"$x.${param.name.toTermName}"
        def toSqlTimestamp(expr: c.Tree) = q"java.sql.Timestamp.from($expr)"
        if (param.typeSignature <:< weakTypeOf[java.time.Instant]) {
          q"${toSqlTimestamp(baseExpr)}"
        } else if (param.typeSignature <:< weakTypeOf[Option[java.time.Instant]]) {
          q"$baseExpr.map(a => ${toSqlTimestamp(q"a")})"
        } else {
          q"$baseExpr"
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
    val optionMap = typeOf[Map[_, Option[_]]]
    val optionSetter = typeOf[Option[Setter[_]]]
    val optionNonNullableSetter = typeOf[Option[NonNullableSetter[_]]]
    val setterType = symbolOf[Setter.type].asClass.module
    val nonNullableSetterType = symbolOf[NonNullableSetter.type].asClass.module
    val seqAny = typeOf[Seq[Any]]
    val mapAny = typeOf[Map[Any, Any]]
    val seqRow = typeOf[Seq[Row]]

    def fromRowRecurse(structType: Type, r: c.Expr[Row]): c.Tree = {
      val constructor = structType.decl(termNames.CONSTRUCTOR).asMethod
      val params = constructor.paramLists.flatten.map((param: Symbol) => {
        val name = param.name.toString
        val isOuterOption = param.typeSignature <:< optionType
        val isOptionNonNullableSetter = param.typeSignature <:< optionNonNullableSetter

        val isOptionSetter = param.typeSignature <:< optionSetter
        val innerType = if (isOptionSetter || isOptionNonNullableSetter) {
          param.typeSignature.typeArgs.head.typeArgs.head
        } else if (isOuterOption) {
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

        val throwError =
          q"throw cognite.spark.v1.SparkSchemaHelper.badRowError($r, $name, ${rowType.toString}, ${structType.toString})"

        def handleFieldValue(value: Tree) =
          if (rowType <:< typeOf[Double]) {
            // do implicit conversion to double
            q"""($value match {
             case x:Double => Some(x)
             case x:Int => Some(x: Double)
             case x:Long => Some(x: Double)
             case x:BigDecimal => Some(x.toDouble)
             case x:BigInt => Some(x.toDouble)
             case _ => $throwError
           })"""
          } else if (rowType <:< typeOf[Long]) {
            q"""($value match {
             case x:Long => Some(x)
             case x:Int => Some(x: Long)
             case _ => $throwError
           })"""
          } else if (rowType <:< typeOf[java.time.Instant]) {
            q"""($value match {
             case x:java.time.Instant => Some(x)
             case x:java.sql.Timestamp => Some(x.toInstant())
             case _ => $throwError
           })"""
          } else {
            q"""($value match {
             case x: $rowType => Some(x)
             case _ => $throwError
           })"""
          }

        val column =
          q"""(scala.util.Try($r.getAs[Any]($name)) match {
             case scala.util.Success(null) => None
             case scala.util.Failure(_) => None
             case scala.util.Success(x) => ${handleFieldValue(q"x")}
           })"""
        val baseExpr =
          if (isOptionSetter) {
            q"$setterType.optionToSetter[$rowType].transform($column)"
          } else if (isOptionNonNullableSetter) {
            q"$nonNullableSetterType.optionToNonNullableSetter[$rowType].transform($column)"
          } else if (isOuterOption) {
            column
          } else {
            q"""$column.getOrElse($throwError)"""
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

  private def simplifyTypeName(name: String) =
    name match {
      case "java.time.Instant" => "Timestamp"
      case "java.lang.Integer" => "Int"
      case "java.lang.Long" => "Long"
      case "java.lang.Double" => "Double"
      case "java.lang.String" => "String"
      case x => x
    }

  def badRowError(row: Row, name: String, typeName: String, rowType: String): Throwable = {
    val columns = row.schema.fieldNames
    Try(row.getAs[Any](name)) match {
      case Failure(error) =>
        new IllegalArgumentException(
          s"Required column '$name' is missing on row [${columns.mkString(", ")}].")
      case Success(value) =>
        val rowIdentifier =
          if (columns.contains("externalId")) {
            s"with externalId='${row.getAs[Any]("externalId")}'"
          } else if (columns.contains("id")) {
            s"with id='${row.getAs[Any]("id")}'"
          } else if (columns.contains("name")) {
            s"with name='${row.getAs[Any]("name")}'"
          } else {
            row.toString
          }

        val hint = if (rowType == "cognite.spark.v1.AssetsIngestSchema" && name == "parentExternalId" && value == null) {
          " To mark the node as root, please use an empty string ('')."
        } else {
          ""
        }

        // this function is invoked only in case of an error -> we have some type issues
        val valueString =
          if (value == null) { "NULL" } else { s"value '$value' of type ${simplifyTypeName(value.getClass.getName)}" }
        new IllegalArgumentException(
          s"Column '$name' was expected to have type ${simplifyTypeName(typeName)}, but $valueString was found (on row $rowIdentifier).$hint")
    }

  }
}
