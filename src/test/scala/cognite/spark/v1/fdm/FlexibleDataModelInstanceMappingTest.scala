package cognite.spark.v1.fdm

import com.cognite.sdk.scala.v1.fdm.common.{DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceDefinition, InstancePropertyValue}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class FlexibleDataModelInstanceMappingTest extends FlatSpec with Matchers {
  import FlexibleDataModelInstanceMapping._

  private val relationStruct = StructType(
    Seq(StructField("space", StringType, false), StructField("externalId", StringType, false)))

  private def field(propertyType: PropertyType, nullable: Boolean = true): StructField =
    propertyField("p", propertyType, nullable)

  "propertyField" should "map scalar types" in {
    field(TextProperty()) shouldBe StructField("p", StringType, true)
    field(PrimitiveProperty(PrimitivePropType.Int64), nullable = false) shouldBe
      StructField("p", LongType, false)
    field(PrimitiveProperty(PrimitivePropType.Timestamp)) shouldBe StructField("p", TimestampType, true)
    field(PrimitiveProperty(PrimitivePropType.Json)) shouldBe StructField("p", StringType, true)
    field(EnumProperty(Map.empty, None)) shouldBe StructField("p", StringType, true)
    field(TimeSeriesReference()) shouldBe StructField("p", StringType, true)
  }

  it should "map lists to arrays, using nullable for the element type too" in {
    field(TextProperty(list = Some(true))) shouldBe StructField("p", ArrayType(StringType, true), true)
    field(PrimitiveProperty(PrimitivePropType.Date, Some(true)), nullable = false) shouldBe
      StructField("p", ArrayType(DateType, false), false)
    field(FileReference(Some(true))) shouldBe StructField("p", ArrayType(StringType, true), true)
  }

  it should "map direct relations to a struct of space and externalId" in {
    field(DirectNodeRelationProperty(None, None)) shouldBe StructField("p", relationStruct, true)
    field(DirectNodeRelationProperty(None, None, Some(true))) shouldBe
      StructField("p", ArrayType(relationStruct), true)
  }

  "usageBasedSchemaAttributes" should "return the node columns" in {
    usageBasedSchemaAttributes(Usage.Node).map(_.name).toSeq shouldBe Seq(
      "space",
      "externalId",
      "node.version",
      "node.createdTime",
      "node.lastUpdatedTime",
      "node.deletedTime",
      "_type")
  }

  it should "return the edge columns" in {
    usageBasedSchemaAttributes(Usage.Edge).map(_.name).toSeq shouldBe Seq(
      "space",
      "externalId",
      "edge.version",
      "edge.createdTime",
      "edge.lastUpdatedTime",
      "edge.deletedTime",
      "type",
      "_type",
      "startNode",
      "endNode")
  }

  private def plain(values: Array[Any]): Seq[Any] =
    values.toSeq.map {
      case a: Array[_] => a.toSeq
      case other => other
    }

  "toProjectedInstance" should "project a node by column name" in {
    val node = InstanceDefinition.NodeDefinition(
      space = "space",
      externalId = "node",
      createdTime = 1L,
      lastUpdatedTime = 2L,
      deletedTime = None,
      version = Some(3L),
      properties = Some(
        Map(
          "space" -> Map(
            "view/v1" -> Map(
              "name" -> InstancePropertyValue.String("a name"),
              "count" -> InstancePropertyValue.Int64(5L))))),
      `type` = Some(DirectRelationReference("typeSpace", "typeId"))
    )
    val dataTypes = Map("name" -> StringType, "count" -> LongType)

    val projected = toProjectedInstance(
      node,
      cursor = None,
      Array("name", "count", "externalId", "node.version", "node.deletedTime", "_type", "absent"),
      dataTypes)

    projected.space shouldBe "space"
    projected.externalId shouldBe "node"
    val expected: Seq[Any] = Seq("a name", 5L, "node", 3L, 0L, Seq("typeSpace", "typeId"), null)
    plain(projected.properties) shouldBe expected
  }

  it should "project an edge by column name" in {
    val edge = InstanceDefinition.EdgeDefinition(
      `type` = DirectRelationReference("typeSpace", "edgeType"),
      space = "space",
      externalId = "edge",
      createdTime = 1L,
      lastUpdatedTime = 2L,
      deletedTime = Some(9L),
      version = None,
      properties = None,
      startNode = DirectRelationReference("a", "start"),
      endNode = DirectRelationReference("b", "end")
    )

    val projected = toProjectedInstance(
      edge,
      cursor = None,
      Array("startNode", "endNode", "type", "edge.version", "edge.deletedTime"),
      Map.empty[String, DataType])

    val expected: Seq[Any] =
      Seq(Seq("a", "start"), Seq("b", "end"), Seq("typeSpace", "edgeType"), -1L, 9L)
    plain(projected.properties) shouldBe expected
  }

  "toRow" should "turn relation references into nested rows" in {
    val row = toRow(
      Array[Any](
        "plain",
        Array[Any]("space", "single"),
        Array[Array[Any]](Array[Any]("space", "first"), Array[Any]("space", "second"))))

    row.getString(0) shouldBe "plain"
    row.get(1) match {
      case r: Row => r.toSeq shouldBe Seq("space", "single")
      case other => fail(s"expected a Row, got $other")
    }
    row.get(2) match {
      case rows: Array[_] => rows.toSeq.collect { case r: Row => r.toSeq } shouldBe Seq(
        Seq("space", "first"),
        Seq("space", "second"))
      case other => fail(s"expected an array of Rows, got $other")
    }
  }
}
