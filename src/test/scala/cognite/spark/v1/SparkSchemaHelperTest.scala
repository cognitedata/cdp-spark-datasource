// scalastyle:off
package cognite.spark.v1

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}
import SparkSchemaHelper._

final case class TestTypeBasic(a: Int, b: Double, c: Byte, d: Float, x: Map[String, String], g: Long, f: Seq[Long], s: String)
final case class TestTypeOption(a: Option[Int], b: Option[Double], c: Option[Byte],
                          d: Option[Float], x: Option[Map[String, String]],
                          g: Option[Long], f: Option[Seq[Option[Long]]], s: Option[String])

class SparkSchemaHelperTest extends FlatSpec with ParallelTestExecution with Matchers {
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
      StructField("x", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false), true),
      StructField("g", DataTypes.LongType, true),
      StructField("f", DataTypes.createArrayType(DataTypes.LongType, true), true),
      StructField("s", DataTypes.StringType, true)
    )))
  }

  "SparkSchemaHelper fromRow" should "construct type from Row" in {
    val r = new GenericRowWithSchema(Array(1, 2.toDouble, 3.toByte,
      4, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    fromRow[TestTypeBasic](r) should be(TestTypeBasic(1, 2, 3, 4, Map("foo" -> "bar"), 5, Seq(10), "foo"))
  }

  "SparkSchemaHelper fromRow" should "construct type from Row doing implicit conversions" in {
    val r = new GenericRowWithSchema(Array(1, 2, 3.toByte,
      4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    fromRow[TestTypeBasic](r) should be(TestTypeBasic(1, 2, 3, 4, Map("foo" -> "bar"), 5, Seq(10), "foo"))
  }

  "SparkSchemaHelper fromRow" should "construct type from Row doing implicit conversions 2" in {
    val r = new GenericRowWithSchema(Array(1, new java.math.BigDecimal(2).pow(100), 3.toByte,
      java.math.BigInteger.valueOf(4), Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    fromRow[TestTypeBasic](r) should be(TestTypeBasic(1, math.pow(2, 100), 3, 4, Map("foo" -> "bar"), 5, Seq(10), "foo"))
  }

  it should "construct optional type from Row of null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, null, null, null, null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, None, None, None, None))
  }

  it should "construct optional type from Row of map values that can be null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, Map("foo" -> "row", "bar" -> "a"), null, null, null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, Some(Map("foo" -> "row", "bar" -> "a")), None, None, None))
  }

  it should "construct optional type from Row of seq values that can be null" in {
    val r = new GenericRowWithSchema(Array(null, null, null, null, null, null, Seq(20L, null), null), structType[TestTypeOption])
    fromRow[TestTypeOption](r) should be(
      TestTypeOption(None, None, None, None, None, None, Some(Seq(Some(20), None)), None))
  }

  "SparkSchemaHelper asRow" should "construct Row from type" in {
    val x = TestTypeBasic(1, 2, 3, 4, Map("foo" -> "bar"), 5, Seq(10), "foo")
    asRow(x) should
      be(Row(1, 2.toDouble, 3.toByte, 4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"))
  }

  it should "construct Row from optional map type" in {
    val x = TestTypeOption(None, None, None, None, Some(Map("foo" -> "row", "bar" -> "a")), None, None, None)
    asRow(x) should
      be(Row(None, None, None, None, Some(Map("foo" -> "row", "bar" -> "a")), None, None, None))
  }

  it should "construct Row from optional seq type" in {
    val x = TestTypeOption(None, None, None, None, None, None, Some(Seq(None, Some(10L))), None)
    asRow(x) should
      be(Row(None, None, None, None, None, None, Some(Seq(None, Some(10L))), None))
  }

  it should "ignore null in map" in {
    val x = new GenericRowWithSchema(Array(null, null, null, null, Map("foo" -> "row", "bar" -> null), null, null, null), structType[TestTypeOption])
    val row = fromRow[TestTypeOption](x)
    row.x.get shouldBe Map("foo" -> "row")
  }

  it should "fail nicely on different type in map" in {
    val x = new GenericRowWithSchema(Array(null, null, null, null, Map("foo" -> "row", "bar" -> 1), null, null, null), structType[TestTypeOption])
    val ex = intercept[IllegalArgumentException] { fromRow[TestTypeOption](x) }
    ex.getMessage shouldBe "Map with string values was expected, but '1' of type Int was found (under key 'bar' on row [null,null,null,null,Map(foo -> row, bar -> 1),null,null,null])"
  }

  it should "fail nicely on type mismatch" in {
    val x = new GenericRowWithSchema(Array("shouldBeInt", 2.toDouble, 3.toByte,
      4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    val ex = intercept[IllegalArgumentException] { fromRow[TestTypeBasic](x) }
    ex.getMessage shouldBe "Column 'a' was expected to have type Int, but 'shouldBeInt' of type String was found (on row [shouldBeInt,2.0,3,4.0,Map(foo -> bar),5,List(10),foo])."
  }

  it should "fail nicely on unexpected NULL in int" in {
    val x = new GenericRowWithSchema(Array(null, 2.toDouble, 3.toByte,
      4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    val ex = intercept[IllegalArgumentException] { fromRow[TestTypeBasic](x) }
    ex.getMessage shouldBe "Column 'a' was expected to have type Int, but NULL was found (on row [null,2.0,3,4.0,Map(foo -> bar),5,List(10),foo])."
  }

  it should "fail nicely on unexpected NULL in string" in {
    val x = new GenericRowWithSchema(Array(1, 2.toDouble, 3.toByte,
      4.toFloat, Map("foo" -> "bar"), 5.toLong, Seq[Long](10), null), structType[TestTypeBasic])
    val ex = intercept[IllegalArgumentException] { fromRow[TestTypeBasic](x) }
    ex.getMessage shouldBe "Column 's' was expected to have type String, but NULL was found (on row [1,2.0,3,4.0,Map(foo -> bar),5,List(10),null])."
  }

  it should "fail nicely on unexpected NULL in map" in {
    val x = new GenericRowWithSchema(Array(1, 2.toDouble, 3.toByte,
      4.toFloat, null, 5.toLong, Seq[Long](10), "foo"), structType[TestTypeBasic])
    val ex = intercept[IllegalArgumentException] { fromRow[TestTypeBasic](x) }
    ex.getMessage shouldBe "Column 'x' was expected to have type Map[String,String], but NULL was found (on row [1,2.0,3,4.0,null,5,List(10),foo])."
  }
}
