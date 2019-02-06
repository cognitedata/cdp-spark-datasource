// scalastyle:off
package com.cognite.spark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import SparkSchemaHelper._

case class TestTypeBasic(a: Int, b: Double, c: Byte, d: Float, x: Map[String, String], g: Long, f: Seq[Long], s: String)
case class TestTypeOption(a: Option[Int], b: Option[Double], c: Option[Byte],
                          d: Option[Float], x: Option[Map[String, Option[String]]],
                          g: Option[Long], f: Option[Seq[Option[Long]]], s: Option[String])

class SparkSchemaHelperTest extends FlatSpec with Matchers {
  "SparkSchemaHelper structType" should "generate StructType from basic case class" in {
    structType[TestTypeBasic] should be (StructType(Seq(
      StructField("a", DataTypes.IntegerType, false),
      StructField("b", DataTypes.DoubleType, false),
      StructField("c", DataTypes.ByteType, false),
      StructField("d", DataTypes.FloatType, false),
      StructField("x", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false), false),
      StructField("g", DataTypes.LongType, false),
      StructField("f", DataTypes.createArrayType(DataTypes.LongType, false), false),
      StructField("s", DataTypes.StringType, false)
    )))
  }

  it should "generate StructType from optional case class" in {
    structType[TestTypeOption] should be (StructType(Seq(
      StructField("a", DataTypes.IntegerType, true),
      StructField("b", DataTypes.DoubleType, true),
      StructField("c", DataTypes.ByteType, true),
      StructField("d", DataTypes.FloatType, true),
      StructField("x", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true),
      StructField("g", DataTypes.LongType, true),
      StructField("f", DataTypes.createArrayType(DataTypes.LongType, true), true),
      StructField("s", DataTypes.StringType, true)
    )))
  }

  "SparkSchemaHelper fromRow" should "construct type from Row" in {
    val r = new GenericRowWithSchema(Array(1, 2.toDouble, 3.toByte,
      4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    fromRow[TestTypeBasic](r) should be(TestTypeBasic(1, 2, 3, 4, Map("foo" -> "bar"), 5, Seq(10), "foo"))
  }

  it should "construct optional type from Row of null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, null, null, null, null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, None, None, None, None))
  }

  it should "construct optional type from Row of map values that are null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, Map("foo" -> null), null, null, null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, Some(Map("foo" -> None)), None, None, None))
  }

  it should "construct optional type from Row of map values that can be null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, Map("foo" -> "row", "bar" -> null), null, null, null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, Some(Map("foo" -> Some("row"), "bar" -> None)), None, None, None))
  }

  it should "construct optional type from Row of seq values that can be null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, null, null, Seq(20L, null), null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, None, None, Some(Seq(Some(20), None)), None))
  }

  "SparkSchemaHelper asRow" should "construct Row from type" in {
    asRow(TestTypeBasic(1, 2, 3, 4, Map("foo" -> "bar"), 5, Seq(10), "foo")) should
      be(Row(1, 2.toDouble, 3.toByte, 4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"))
  }

  it should "construct Row from optional map type" in {
    asRow(TestTypeOption(None, None, None, None, Some(Map("foo" -> Some("row"), "bar" -> None)), None, None, None)) should
      be(Row(None, None, None, None, Some(Map("foo" -> Some("row"), "bar" -> None)), None, None, None))
  }

  it should "construct Row from optional seq type" in {
    val x = TestTypeOption(None, None, None, None, None, None, Some(Seq(None, Some(10L))), None)
    asRow(x) should
      be(Row(None, None, None, None, None, None, Some(Seq(None, Some(10L))), None))
  }
}
