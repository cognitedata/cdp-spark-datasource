package cognite.spark.v1.fdm.RelationUtils

import cats.implicits.{toBifunctorOps, toTraverseOps}
import cognite.spark.v1.CdfSparkException

import java.time._
import scala.util.Try

object Convertors {
  def skipNulls[T](seq: Seq[T]): Seq[T] =
    seq.filter(_ != null)

  val timezoneId: ZoneId = ZoneId.of("UTC")

  def tryAsDate(value: Any, propertyName: String): Either[CdfSparkException, LocalDate] =
    Try(value.asInstanceOf[java.sql.Date].toLocalDate)
      .orElse(Try(LocalDate.parse(String.valueOf(value))))
      .toEither
      .orElse(tryAsTimestamp(value, propertyName).map(_.toLocalDate))
      .leftMap { e =>
        new CdfSparkException(
          s"""Error parsing value of field '$propertyName' as a date: ${e.getMessage}""".stripMargin)
      }

  def tryAsDates(value: Seq[Any], propertyName: String): Either[CdfSparkException, Vector[LocalDate]] =
    skipNulls(value).toVector.traverse(tryAsDate(_, propertyName)).leftMap { e =>
      new CdfSparkException(
        s"""Error parsing value of field '$propertyName' as an array of dates: ${e.getMessage}""".stripMargin)
    }

  def tryAsTimestamp(value: Any, propertyName: String): Either[CdfSparkException, ZonedDateTime] =
    Try(
      ZonedDateTime
        .ofLocal(value.asInstanceOf[java.sql.Timestamp].toLocalDateTime, timezoneId, ZoneOffset.UTC))
      .orElse(Try(ZonedDateTime.parse(String.valueOf(value))))
      .orElse(Try(LocalDateTime.parse(String.valueOf(value)).atZone(timezoneId)))
      .toEither
      .leftMap { e =>
        new CdfSparkException(
          s"""Error parsing value of field '$propertyName' as a timestamp: ${e.getMessage}""".stripMargin)
      }

  def tryAsTimestamps(
      value: Seq[Any],
      propertyName: String): Either[CdfSparkException, Vector[ZonedDateTime]] =
    skipNulls(value).toVector.traverse(tryAsTimestamp(_, propertyName)).leftMap { e =>
      new CdfSparkException(
        s"""Error parsing value of field '$propertyName' as an array of timestamps: ${e.getMessage}""".stripMargin)
    }

  def safeConvertToLong(n: BigDecimal): Long =
    if (n.isValidLong) {
      n.longValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Long")
    }
  def safeConvertToInt(n: BigDecimal): Int =
    if (n.isValidInt) {
      n.intValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Int")
    }
  def safeConvertToDouble(n: BigDecimal): Double =
    if (n.isDecimalDouble) {
      n.doubleValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Double")
    }
  def safeConvertToFloat(n: BigDecimal): Float =
    if (n.isDecimalFloat) {
      n.floatValue
    } else {
      throw new IllegalArgumentException(s"'${String.valueOf(n)}' is not a valid Float")
    }

  def tryConvertNumber[T](
      n: Any,
      propertyName: String,
      expectedType: String,
      safeConvert: (BigDecimal) => T
  ): Either[CdfSparkException, T] =
    Try {
      val asBigDecimal = BigDecimal(String.valueOf(n))
      safeConvert(asBigDecimal)
    }.toEither
      .leftMap(exception =>
        new CdfSparkException(
          s"""Error parsing value for field '$propertyName'.
             |Expecting a $expectedType but found '${String.valueOf(n)}'
             |""".stripMargin,
          exception
      ))

  def tryConvertNumberSeq[T](
      ns: Seq[Any],
      propertyName: String,
      expectedType: String,
      safeConvert: (BigDecimal) => T): Either[CdfSparkException, Seq[T]] =
    Try {
      skipNulls(ns).map { n =>
        val asBigDecimal = BigDecimal(String.valueOf(n))
        safeConvert(asBigDecimal)
      }
    }.toEither
      .leftMap(exception => {
        val seqAsStr = ns.map(String.valueOf).mkString(",")
        new CdfSparkException(
          s"""Error parsing value for field '$propertyName'.
             |Expecting a Array[$expectedType] but found '[$seqAsStr]'
             |""".stripMargin,
          exception
        )
      })

}
