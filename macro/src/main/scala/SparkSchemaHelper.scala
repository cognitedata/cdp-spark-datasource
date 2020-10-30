package cognite.spark.v1

import cats.data.NonEmptyList

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

    val targetType = weakTypeOf[T]

    val SparkSchemaHelperRuntime = q"cognite.spark.v1.SparkSchemaHelperRuntime"

    val setterType = symbolOf[Setter.type].asClass.module
    val nonNullableSetterType = symbolOf[NonNullableSetter.type].asClass.module

    final case class PathSegment(
      description: String,
      key: c.Tree,
      expectedType: c.Type
    )

    def transformValue(value: c.Tree, ty: c.Type, path: NonEmptyList[PathSegment]): c.Tree = {
      lazy val pathArgs = path.map {
        case PathSegment(description, key, expectedType) =>
          q"""
            $SparkSchemaHelperRuntime.PathSegment(
              $description,
              $key match { case null => "NULL"; case x => x.toString },
              ${expectedType.toString}
            )
          """
      }.toList

      lazy val throwError =
        q"""
          throw $SparkSchemaHelperRuntime.fromRowError(
            rootRow,
            ${targetType.typeSymbol.fullName},
            cats.data.NonEmptyList.of(..$pathArgs),
            $value
          )
          """

      def transformFromSeq(intoCollection: c.Tree => c.Tree) = {
        val elementType = ty.typeArgs.head
        val seqName = c.freshName(TermName("seq"))
        val valueName = c.freshName(TermName("value"))
        val indexName = c.freshName(TermName("index"))
        val newPath = PathSegment("index", q"$indexName.toString", elementType) :: path
        q"""($value match {
            case $seqName: Seq[_] =>
              ${intoCollection(q"$seqName")}
                .zipWithIndex
                .map {
                  case ($valueName, $indexName) =>
                    ${transformValue(q"$valueName", elementType, newPath)}
                }
            case _ => $throwError
         })"""
      }

      if (ty <:< typeOf[Option[Any]]) {
        q"Option($value).map(x => ${transformValue(q"x", ty.typeArgs.head, path)})"
      }
      else if (ty <:< typeOf[Seq[Any]]) {
        transformFromSeq(identity)
      }
      else if (ty <:< typeOf[cats.data.NonEmptyList[Any]]) {
        transformFromSeq(seq => q"NonEmptyList.fromList($seq.toList).getOrElse($throwError)")
      }
      else if (ty <:< weakTypeOf[Map[_, _]]) {
        val List(keyType, valueType) = ty.typeArgs
        val keyName = c.freshName(TermName("key"))
        val valueName = c.freshName(TermName("value"))

        def quote(str: c.Tree): c.Tree = { q""" ("'" + $str + "'") """}

        val keyPath = PathSegment("key", q"$keyName", keyType) :: path
        val valuePath = PathSegment("value at key", quote(q"$keyName"), valueType) :: path

        q"""($value match {
            case map: Map[Any @unchecked, Any @unchecked] =>
              for {
                ($keyName, $valueName) <- map
                if $keyName != null && $valueName != null
              } yield (
                ${transformValue(q"$keyName", keyType, keyPath)},
                ${transformValue(q"$valueName", valueType, valuePath)}
              )
            case _ => $throwError
          })"""
      }
      else if (ty <:< typeOf[Setter[Any]]) {
        val innerType = ty.typeArgs.head
        q"$setterType.anyToSetter[$innerType].transform(${transformValue(value, innerType, path)})"
      }
      else if (ty <:< typeOf[NonNullableSetter[Any]]) {
        val innerType = ty.typeArgs.head
        q"$nonNullableSetterType.toNonNullableSetter[$innerType].transform(${transformValue(value, innerType, path)})"
      }
      else if (ty == typeOf[Double]) {
        q"""($value match {
            case x: Double => x
            case x: Float => x.toDouble
            case x: Byte => x.toFloat
            case x: Short => x.toFloat
            case x: Int => x.toDouble
            case x: Long => x.toDouble
            case x: BigDecimal => x.toDouble
            case x: BigInt => x.toDouble
            case x: java.math.BigDecimal => x.doubleValue
            case x: java.math.BigInteger => x.doubleValue
            case _ => $throwError
          })"""
      }
      else if (ty == typeOf[Float]) {
        q"""($value match {
            case x: Float => x
            case x: Double => x.toFloat
            case x: Byte => x.toFloat
            case x: Short => x.toFloat
            case x: Int => x.toFloat
            case x: Long => x.toFloat
            case x: BigDecimal => x.toFloat
            case x: BigInt => x.toFloat
            case x: java.math.BigDecimal => x.floatValue
            case x: java.math.BigInteger => x.floatValue
            case _ => $throwError
          })"""
      }
      else if (ty == typeOf[Long]) {
        q"""($value match {
            case x: Long => x
            case x: Int => x: Long
            case x: Short => x: Long
            case x: Byte => x: Long
            case x: BigInt => x.longValue
            case x: java.math.BigInteger => x.longValue
            case _ => $throwError
          })"""
      }
      else if (ty == typeOf[Int]) {
        q"""($value match {
            case x: Int => x
            case x: Short => x: Int
            case x: Byte => x: Int
            case x: BigInt => x.intValue
            case x: java.math.BigInteger => x.intValue
            case _ => $throwError
          })"""
      }
      else if (ty == typeOf[Short]) {
        q"""($value match {
            case x: Short => x
            case x: Byte => x: Short
            case x: java.math.BigInteger => x.shortValue
            case _ => $throwError
          })"""
      }
      else if (ty == typeOf[Byte]) {
        q"""($value match {
            case x: Byte => x
            case x: java.math.BigInteger => x.byteValue
            case _ => $throwError
          })"""
      }
      else if (
        ty == typeOf[String] ||
        ty == typeOf[Boolean]
      ) {
        q"""($value match {
            case x: $ty => x
            case _ => $throwError
          })"""
      }
      else if (ty == typeOf[java.time.Instant]) {
        q"""($value match {
            case x: java.time.Instant => x
            case x: java.sql.Timestamp => x.toInstant()
            case _ => $throwError
          })"""
      }
      else {
        q"""($value match {
            case row: ${typeOf[Row]} => ${transformRow(q"row", ty, path.toList)}
            case _ => $throwError
          })"""
      }
    }

    def transformRow(row: c.Tree, structType: c.Type, path: List[PathSegment] = List.empty): c.Tree = {
      val constructor = structType.decl(termNames.CONSTRUCTOR).asMethod
      val paramList = constructor.paramLists.head

      val params = paramList.map((param: Symbol) => {
        val paramName = param.name.toString
        val quotedParamName = s"`${paramName.replace("`", "``")}`"
        val paramType = param.typeSignature

        val column =
          q"""(scala.util.Try($row.getAs[Any]($paramName)) match {
             case scala.util.Failure(_) => null
             case scala.util.Success(x) => x
           })"""

        val newPath = NonEmptyList(
          PathSegment("column", q"$quotedParamName", paramType),
          path
        )

        val resExpr = transformValue(column, paramType, newPath)

        q"$resExpr.asInstanceOf[$paramType]"
      })
      q"new ${structType}(..$params)"
    }

    c.Expr[T](q"""{
      val rootRow = ${row.tree} // Assign a separate name to the root row, so we have a reference that won't be shadowed
      ${transformRow(q"rootRow", weakTypeOf[T])}
    }""")
  }
  // scalastyle:on
}

object SparkSchemaHelper {
  def structType[T]()(implicit encoder: StructTypeEncoder[T]): StructType =
    encoder.structType()

  def asRow[T](x: T): Row = macro SparkSchemaHelperImpl.asRow[T]
  def fromRow[T](row: Row): T = macro SparkSchemaHelperImpl.fromRow[T]
}
