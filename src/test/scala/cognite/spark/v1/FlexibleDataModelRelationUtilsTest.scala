package cognite.spark.v1

import cognite.spark.v1.FlexibleDataModelRelationUtils.{createEdges, createNodes, createNodesOrEdges}
import cognite.spark.v1.utils.fdm.FDMViewPropertyTypes._
import com.cognite.sdk.scala.v1.fdm.instances.InstancePropertyValue
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

// scalastyle:off null
class FlexibleDataModelRelationUtilsTest extends FlatSpec with Matchers {

  private val destRef = ViewReference("space", "viewExtId1", "viewV1")

  private val relationRefSchema: StructType = StructType(
    Array(
      StructField("space", StringType, nullable = false),
      StructField("externalId", StringType, nullable = false)
    )
  )

  it should "fail to create nodes when externalId is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithoutDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true)
      )
    )

    val values = Array[Array[Any]](Array("str1"), Array("str2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("Couldn't find required string property 'externalId'") shouldBe true
  }

  it should "fail to create nodes when externalId is null" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true)
        )
      )

    val values = Array[Array[Any]](
      Array("stringProp1", "extId1", null),
      Array(null, null, 5)
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("'externalId' cannot be null") shouldBe true
  }

  it should "fail to create nodes when required a property is null" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true)
        )
      )

    val values = Array[Array[Any]](Array("stringProp1", "extId1", 1), Array(null, "extId1", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("cannot be null") shouldBe true
  }

  it should "fail to create nodes when required a property is missing" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true)
        )
      )

    val values = Array[Array[Any]](Array("extId1", 1), Array("extId2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("Could not find required properties") shouldBe true
  }

  it should "fail to create nodes when required a property is nullable" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("externalId", StringType, nullable = false),
          StructField("stringProp", StringType, nullable = true),
          StructField("intProp", IntegerType, nullable = true)
        )
      )

    val values =
      Array[Array[Any]](Array("extId1", "stringProp1", 1), Array("extId2", "stringProp2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("cannot contain null values") shouldBe true
  }

  it should "successfully create nodes with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", StringType, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Array[Array[Any]](
        Array("stringProp1", "extId1", null, Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(5),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create nodes with only required properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values = Array[Array[Any]](
      Array("stringProp1", "extId1", Seq(1.1, 1.2, null)),
      Array("stringProp2", "extId2", Array(2.1, 2.2)))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create nodes when there are unrelated properties in Rows" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true),
          StructField("unrelatedProp", StringType, nullable = false),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Array[Array[Any]](
        Array("stringProp1", "extId1", 1, "unrelatedProp1", Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", null, "unrelatedProp2", Array(2.1, 2.2), null)
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "intProp" -> InstancePropertyValue.Int32(1),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "fail to create edges when externalId is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true)
      )
    )

    val values = Array[Array[Any]](Array("str1", 1), Array("str2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("Couldn't find required string property 'externalId'") shouldBe true
  }

  it should "fail to create edges when type is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false)
      )
    )

    val values = Array[Array[Any]](Array("str1", null, "externalId1"), Array("str2", 2, "externalId2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("Could not find required property 'type'") shouldBe true
  }

  it should "fail to create edges when startNode is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "str1",
        null,
        "externalId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema)),
      Array(
        "str2",
        2,
        "externalId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema))
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("Could not find required property 'startNode'") shouldBe true
  }

  it should "fail to create edges when endNode is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "str1",
        null,
        "externalId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema)
      ),
      Array(
        "str2",
        2,
        "externalId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("Could not find required property 'endNode'") shouldBe true
  }

  it should "fail to create edges when type.space is null" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "str1",
        null,
        "externalId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema)
      ),
      Array(
        "str2",
        2,
        "externalId2",
        new GenericRowWithSchema(Array(null, "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("(Edge type) cannot contain null values") shouldBe true
  }

  it should "successfully create edges with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq(1.1, 1.2, null),
        Array(2.1, null)
      ),
      Array(
        "stringProp2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2),
        null
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(2),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create edges with only required properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq(1.1, 1.2, null)
      ),
      Array(
        "stringProp2",
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create edges when there are unrelated properties in Rows" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false),
        StructField("unrelatedProp", IntegerType, nullable = false),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq(1.1, 1.2, null),
        Array(2.1, null)
      ),
      Array(
        "stringProp2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        "unrelatedProp2",
        Array(2.1, 2.2),
        null
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(2),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "fail to create nodesOrEdges or edges when externalId is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithoutDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true)
      )
    )

    val values = Array[Array[Any]](Array("str1"), Array("str2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage
      .contains("Couldn't find required string property 'externalId'") shouldBe true
  }

  it should "fail to create nodesOrEdges or edges when externalId is null" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true)
        )
      )

    val values = Array[Array[Any]](
      Array("stringProp1", "extId1", null),
      Array(null, null, 5)
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("'externalId' cannot be null") shouldBe true
  }

  it should "fail to create nodesOrEdges or edges when required a property is null" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true)
        )
      )

    val values = Array[Array[Any]](Array("stringProp1", "extId1", 1), Array(null, "extId1", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("cannot be null") shouldBe true
  }

  it should "successfully create nodesOrEdges in createNodesOrEdges with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", StringType, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Array[Array[Any]](
        Array("stringProp1", "extId1", null, Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get.asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(5),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create nodesOrEdges in createNodesOrEdges with only required properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values = Array[Array[Any]](
      Array("stringProp1", "extId1", Seq(1.1, 1.2, null)),
      Array("stringProp2", "extId2", Array(2.1, 2.2)))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get.asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create nodesOrEdges in createNodesOrEdges when there are unrelated properties in Rows" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true),
          StructField("unrelatedProp", StringType, nullable = false),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Array[Array[Any]](
        Array("stringProp1", "extId1", 1, "unrelatedProp1", Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", null, "unrelatedProp2", Array(2.1, 2.2), null)
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get.asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "intProp" -> InstancePropertyValue.Int32(1),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create edges in createNodesOrEdges with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq(1.1, 1.2, null),
        Array(2.1, null)
      ),
      Array(
        "stringProp2",
        5,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2),
        null
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get.asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(5),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create edges in createNodesOrEdges with only required properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("type", relationRefSchema, nullable = true),
          StructField("startNode", relationRefSchema, nullable = true),
          StructField("endNode", relationRefSchema, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq(1.1, 1.2, null)
      ),
      Array(
        "stringProp2",
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get.asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "successfully create edges in createNodesOrEdges when there are unrelated properties in Rows" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true),
          StructField("externalId", StringType, nullable = false),
          StructField("type", relationRefSchema, nullable = true),
          StructField("startNode", relationRefSchema, nullable = true),
          StructField("endNode", relationRefSchema, nullable = true),
          StructField("unrelatedProp", StringType, nullable = false),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq(1.1, 1.2, null),
        Array(2.1, null)
      ),
      Array(
        "stringProp2",
        5,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        "unrelatedProp1",
        Array(2.1, 2.2),
        null
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get.asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(5),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
  }

  it should "fail create nodes Or edges in createNodesOrEdges when a type is missing" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "str1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema)
      ),
      Array(
        "str2",
        2,
        "extId2",
        null,
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema)
      ),
      Array(
        "str3",
        3,
        "extId3"
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("Only found: 'externalId', 'startNode', 'endNode'") shouldBe true
  }

  it should "fail create nodes Or edges in createNodesOrEdges when a startNode is missing" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "str1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        null,
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema)
      ),
      Array(
        "str2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema)
      ),
      Array(
        "str3",
        3,
        "extId3"
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("Only found: 'externalId', 'type', 'endNode'") shouldBe true
  }

  it should "fail create nodes Or edges in createNodesOrEdges when a endNode is missing" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "str1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema)
      ),
      Array(
        "str2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        null
      ),
      Array(
        "str3",
        3,
        "extId3"
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe false
    result.left.get.getMessage.contains("Only found: 'externalId', 'type', 'startNode'") shouldBe true
  }

  it should "successfully create both nodesOrEdges & edges in createNodesOrEdges with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq(1.1, 1.2, null),
        Array(2.1, null)
      ),
      Array(
        "stringProp2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2),
        null
      ),
      Array(
        "stringProp3",
        3,
        "extId3",
        null,
        null,
        null,
        Array(3.1, 3.2),
        null
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodesOrEdges = result.right.get
    nodesOrEdges.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2", "extId3")
    val extIdPropsMap = nodesOrEdges.map {
      case n: NodeWrite =>
        n.externalId -> n.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
      case e: EdgeWrite =>
        e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    extIdPropsMap.contains("extId3") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(2),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp3"),
        "intProp" -> InstancePropertyValue.Int32(3),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(3.1, 3.2))
      ))
  }

  it should "successfully create both nodesOrEdges & edges in createNodesOrEdges with only required properties" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq(1.1, 1.2, null)
      ),
      Array(
        "stringProp2",
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2)
      ),
      Array(
        "stringProp3",
        "extId3",
        null,
        null,
        null,
        Seq(3.1, 3.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodesOrEdges = result.right.get
    nodesOrEdges.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2", "extId3")
    val extIdPropsMap = nodesOrEdges.map {
      case n: NodeWrite =>
        n.externalId -> n.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
      case e: EdgeWrite =>
        e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    extIdPropsMap.contains("extId3") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp3"),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(3.1, 3.2))
      ))
  }

  it should "successfully create both nodesOrEdges & edges in createNodesOrEdges when there are unrelated properties in Rows" in {
    val propertyMap = Map(
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = true),
        StructField("startNode", relationRefSchema, nullable = true),
        StructField("endNode", relationRefSchema, nullable = true),
        StructField("unrelatedProp", StringType, nullable = false),
        StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
        StructField("floatListProp", ArrayType(FloatType), nullable = true)
      )
    )

    val values = Array[Array[Any]](
      Array(
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq(1.1, 1.2, null),
        Array(2.1, null)
      ),
      Array(
        "stringProp2",
        5,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        "unrelatedProp2",
        Array(2.1, 2.2)
      ),
      Array(
        "stringProp3",
        6,
        "extId3",
        null,
        null,
        null,
        "unrelatedProp3",
        Array(3.1, 3.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodesOrEdges = result.right.get
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2", "extId3")
    val extIdPropsMap = nodesOrEdges.map {
      case n: NodeWrite =>
        n.externalId -> n.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
      case e: EdgeWrite =>
        e.externalId -> e.sources.flatMap(s => s.properties.getOrElse(Map.empty)).toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    extIdPropsMap.contains("extId3") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp1"),
        "doubleListProp" -> InstancePropertyValue.Float64List(List(1.1, 1.2)),
        "floatListProp" -> InstancePropertyValue.Float32List(List(2.1F))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp2"),
        "intProp" -> InstancePropertyValue.Int32(5),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(2.1, 2.2))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp3"),
        "intProp" -> InstancePropertyValue.Int32(6),
        "doubleListProp" -> InstancePropertyValue.Float64List(ArrayBuffer(3.1, 3.2))
      ))
  }
}
// scalastyle:on null
