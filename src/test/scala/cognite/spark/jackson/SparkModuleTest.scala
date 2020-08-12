package cognite.spark.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}

class SparkModuleTest extends FlatSpec with Matchers {
  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(SparkModule)

  it should "serialize rows with schema to simple JSON objects" in {
    val row = new GenericRowWithSchema(
      values = Array[Any](123L),
      schema = StructType.fromDDL("foo BIGINT")
    )

    val json = mapper.writeValueAsString(row)
    val tree = mapper.readTree(json)

    assert(tree.isObject)
    assert(tree.size() == 1)
    assert(tree.get("foo").asLong == 123L)
  }

  it should "serialize schemaless rows as arrays" in {
    val row = Row(123L)

    val json = mapper.writeValueAsString(row)
    val tree = mapper.readTree(json)

    assert(tree.isArray)
    assert(tree.size() == 1)
    assert(tree.get(0).asLong == 123L)
  }
}
