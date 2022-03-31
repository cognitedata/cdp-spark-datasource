package org.apache.spark.sql.cognite.tests

import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JsonInferSchema}
import org.apache.spark.sql.types.{DataType, StructType}

/** allows us to access spark's package private classes. Used only for testing */
object SparkInternalsHack {
  def sameTypes(a: DataType, b: DataType): Boolean =
    a.sameType(b)
}
