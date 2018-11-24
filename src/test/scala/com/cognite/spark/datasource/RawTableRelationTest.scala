package com.cognite.spark.datasource

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class RawTableRelationTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  import RawTableRelation._
  import spark.implicits._

  private def collectToSet(df: DataFrame): Set[String] = {
    df.collect().map(_.getString(0)).toSet
  }

  private val dfWithoutKeySchema = StructType(Seq(
    StructField("notKey", StringType, false),
    StructField("value", IntegerType, false)))
  private val dfWithoutKeyData = Seq(
    ("key1", """{ "notKey": "k1", "value": 1 }"""),
    ("key2", """{ "notKey": "k2", "value": 2 }""")
  )
  private val dfWithKeySchema = StructType(Seq(
    StructField("key", StringType, false),
    StructField("value", IntegerType, false)))
  private val dfWithKeyData = Seq(
    ("key3", """{ "key": "k1", "value": 1 }"""),
    ("key4", """{ "key": "k2", "value": 2 }""")
  )
  private val dfWithManyKeysSchema = StructType(Seq(
    StructField("key", StringType, true),
    StructField("__key", StringType, false),
    StructField("___key", StringType, false),
    StructField("value", IntegerType, false)))
  private val dfWithManyKeysData = Seq(
    ("key5", """{ "___key": "___k1", "__key": "__k1", "value": 1 }"""),
    ("key6", """{ "___key": "___k2", "value": 2, "__key": "__k2", "key": "k2" }""")
  )

  "A RawTableRelation" should "allow data columns named key, _key etc. but rename them to _key, __key etc." in {
    val dfWithoutKey = dfWithoutKeyData.toDF("key", "columns")
    val processedWithoutKey = flattenAndRenameKeyColumns(sqlContext, dfWithoutKey, dfWithoutKeySchema)
    processedWithoutKey.schema.fieldNames.toSet should equal (Set("key", "notKey", "value"))
    collectToSet(processedWithoutKey.select($"key")) should equal (Set("key1", "key2"))

    val dfWithKey = dfWithKeyData.toDF("key", "columns")
    val processedWithKey = flattenAndRenameKeyColumns(sqlContext, dfWithKey, dfWithKeySchema)
    processedWithKey.schema.fieldNames.toSet should equal (Set("key", "_key", "value"))
    collectToSet(processedWithKey.select($"key")) should equal (Set("key3", "key4"))
    collectToSet(processedWithKey.select($"_key")) should equal (Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "columns")
    val processedWithManyKeys = flattenAndRenameKeyColumns(sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    processedWithManyKeys.schema.fieldNames.toSet should equal (Set("key", "____key", "___key", "_key", "value"))

    collectToSet(processedWithManyKeys.select($"key")) should equal (Set("key5", "key6"))
    collectToSet(processedWithManyKeys.select($"_key")) should equal (Set(null, "k2"))
    collectToSet(processedWithManyKeys.select($"___key")) should equal (Set("__k1", "__k2"))
    collectToSet(processedWithManyKeys.select($"____key")) should equal (Set("___k1", "___k2"))
  }

  it should "insert data with columns named _key, __key etc. as data columns key, _key, etc." in {
    val dfWithKey = dfWithKeyData.toDF("key", "columns")
    val processedWithKey = flattenAndRenameKeyColumns(sqlContext, dfWithKey, dfWithKeySchema)
    val (columnNames1, unRenamed1) = prepareForInsert(processedWithKey)
    columnNames1.toSet should equal (Set("key", "value"))
    collectToSet(unRenamed1.select("key")) should equal (Set("k1", "k2"))

    val dfWithManyKeys = dfWithManyKeysData.toDF("key", "columns")
    val processedWithManyKeys = flattenAndRenameKeyColumns(sqlContext, dfWithManyKeys, dfWithManyKeysSchema)
    val (columnNames2, unRenamed2) = prepareForInsert(processedWithManyKeys)
    columnNames2.toSet should equal (Set("key", "__key", "___key", "value"))
    collectToSet(unRenamed2.select("key")) should equal (Set(null, "k2"))
    collectToSet(unRenamed2.select("__key")) should equal (Set("__k1", "__k2"))
    collectToSet(unRenamed2.select("___key")) should equal (Set("___k1", "___k2"))
  }
}
