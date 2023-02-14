package cognite.spark.v1

import cognite.spark.v1.FlexibleDataModelRelationUtils.{createEdges, createNodes, createNodesOrEdges}
import cognite.spark.v1.utils.fdm.FDMViewPropertyTypes._
import com.cognite.sdk.scala.v1.fdm.instances.InstancePropertyValue
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{Assertion, FlatSpec, Matchers}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}

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

    val values = Seq[Array[Any]](Array("str1"), Array("str2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema)).toSeq

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "Couldn't find required string property 'externalId'")
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

    val values = Seq[Array[Any]](
      Array("stringProp1", "extId1", null),
      Array(null, null, 5)
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "'externalId' cannot be null")
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

    val values = Seq[Array[Any]](Array("stringProp1", "extId1", 1), Array(null, "extId1", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "cannot be null")
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

    val values = Seq[Array[Any]](Array("extId1", 1), Array("extId2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "Could not find required properties")
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
      Seq[Array[Any]](
        Array("stringProp1", "extId1", null, Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
      Array("stringProp1", "extId1", Seq(1.1, 1.2, null)),
      Array("stringProp2", "extId2", Array(2.1, 2.2)))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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
      Seq[Array[Any]](
        Array("stringProp1", "extId1", 1, "unrelatedProp1", Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", null, "unrelatedProp2", Array(2.1, 2.2), null)
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](Array("str1", 1), Array("str2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "Couldn't find required string property 'externalId'")
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

    val values = Seq[Array[Any]](Array("str1", null, "externalId1"), Array("str2", 2, "externalId2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "Could not find required property 'type'")
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

    val values = Seq[Array[Any]](
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
    verifyErrorMessage(result, "Could not find required property 'startNode'")
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

    val values = Seq[Array[Any]](
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
    verifyErrorMessage(result, "Could not find required property 'endNode'")
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

    val values = Seq[Array[Any]](
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
    verifyErrorMessage(result, "(Edge type) cannot contain null values")
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

    val values = Seq[Array[Any]](
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

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
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

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
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

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
      ))
  }

  it should "fail to create nodes or edges when externalId is not present" in {
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

    val values = Seq[Array[Any]](Array("str1"), Array("str2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "Couldn't find required string property 'externalId'")
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

    val values = Seq[Array[Any]](
      Array("stringProp1", "extId1", null),
      Array(null, null, 5)
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "'externalId' cannot be null")
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

    val values = Seq[Array[Any]](Array("stringProp1", "extId1", 1), Array(null, "extId1", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    verifyErrorMessage(result, "cannot be null")
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
      Seq[Array[Any]](
        Array("stringProp1", "extId1", null, Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
      Array("stringProp1", "extId1", Seq(1.1, 1.2, null)),
      Array("stringProp2", "extId2", Array(2.1, 2.2)))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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
      Seq[Array[Any]](
        Array("stringProp1", "extId1", 1, "unrelatedProp1", Seq(1.1, 1.2, null), Array(2.1, null)),
        Array("stringProp2", "extId2", null, "unrelatedProp2", Array(2.1, 2.2), null)
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
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

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
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

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
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

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
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

    val values = Seq[Array[Any]](
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
    verifyErrorMessage(result, "Only found: 'externalId', 'startNode', 'endNode'")
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

    val values = Seq[Array[Any]](
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
    verifyErrorMessage(result, "Only found: 'externalId', 'type', 'endNode'")
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

    val values = Seq[Array[Any]](
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
    verifyErrorMessage(result, "Only found: 'externalId', 'type', 'startNode'")
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

    val values = Seq[Array[Any]](
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

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp3"),
        "intProp" -> InstancePropertyValue.Int32(3),
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(3.1, 3.2))
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

    val values = Seq[Array[Any]](
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

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp3"),
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(3.1, 3.2))
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

    val values = Seq[Array[Any]](
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

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
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
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(2.1, 2.2))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> InstancePropertyValue.String("stringProp3"),
        "intProp" -> InstancePropertyValue.Int32(6),
        "doubleListProp" -> InstancePropertyValue.Float64List(Seq(3.1, 3.2))
      ))
  }

  it should "successfully handle properties with date & timestamp values" in {
    val propertyMap = Map(
      "timestampProp1" -> TimestampNonListWithDefaultValueNonNullable,
      "timestampProp2" -> TimestampNonListWithDefaultValueNonNullable,
      "timestampProp3" -> TimestampNonListWithDefaultValueNonNullable,
      "timestampListProp1" -> TimestampListWithoutDefaultValueNonNullable,
      "timestampListProp2" -> TimestampListWithoutDefaultValueNonNullable,
      "timestampListProp3" -> TimestampListWithoutDefaultValueNonNullable,
      "dateProp1" -> DateNonListWithDefaultValueNonNullable,
      "dateProp2" -> DateNonListWithDefaultValueNonNullable,
      "dateProp3" -> DateNonListWithDefaultValueNonNullable,
      "dateProp4" -> DateNonListWithDefaultValueNonNullable,
      "dateListProp1" -> DateListWithoutDefaultValueNonNullable,
      "dateListProp2" -> DateListWithoutDefaultValueNonNullable,
      "dateListProp3" -> DateListWithoutDefaultValueNonNullable,
    )
    val schema = StructType(
      Array(
        StructField("externalId", IntegerType, nullable = false),
        StructField("timestampProp1", StringType, nullable = false),
        StructField("timestampProp2", StringType, nullable = false),
        StructField("timestampProp3", TimestampType, nullable = true),
        StructField("timestampListProp1", ArrayType(StringType), nullable = false),
        StructField("timestampListProp2", ArrayType(StringType), nullable = false),
        StructField("timestampListProp3", ArrayType(TimestampType), nullable = false),
        StructField("dateProp1", StringType, nullable = false),
        StructField("dateProp2", StringType, nullable = false),
        StructField("dateProp3", DateType, nullable = true),
        StructField("dateProp4", DateType, nullable = true),
        StructField("dateListProp1", ArrayType(StringType), nullable = false),
        StructField("dateListProp2", ArrayType(StringType), nullable = false),
        StructField("dateListProp3", ArrayType(DateType), nullable = false),
      )
    )

    val values = Seq[Array[Any]](
      Array(
        "extId1",
        // timestamps
        s"${ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}",
        s"${ZonedDateTime.now(ZoneId.of("Asia/Colombo")).format(InstancePropertyValue.Timestamp.formatter)}",
        java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))),
        Array(
          s"${LocalDateTime.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}",
          s"${LocalDateTime.now().atZone(ZoneId.of("Europe/Berlin")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}"
        ),
        Seq(
          s"${LocalDateTime.now().atZone(ZoneId.of("UTC")).format(InstancePropertyValue.Timestamp.formatter)}",
          s"${LocalDateTime.now().atZone(ZoneId.of("Asia/Bangkok")).format(InstancePropertyValue.Timestamp.formatter)}"
        ),
        Seq(java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC")))),
        // dates
        s"${LocalDate.now().format(DateTimeFormatter.ISO_DATE)}",
        s"${LocalDate.now().plusDays(7).format(InstancePropertyValue.Date.formatter)}",
        java.sql.Date.valueOf(LocalDate.now()),
        java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))),
        Array(
          s"${LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)}",
          s"${LocalDate.now().plusDays(7).format(DateTimeFormatter.ISO_LOCAL_DATE)}"
        ),
        Seq(
          s"${LocalDate.now().format(InstancePropertyValue.Date.formatter)}",
          s"${LocalDate.now().plusDays(7).format(InstancePropertyValue.Date.formatter)}"
        ),
        Seq(
          java.sql.Date.valueOf(LocalDate.now()),
          java.sql.Date.valueOf(LocalDate.now().plusDays(7))
        )
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1")
    val props = nodesOrEdges
      .collect {
        case n: NodeWrite => n.sources.flatMap(_.properties.getOrElse(Map.empty))
      }
      .flatten
      .toMap

    val justBeforeNow = LocalDateTime.now(ZoneId.of("UTC")).minusMinutes(10)
    val today = LocalDate.now()
    val nextWeek = LocalDate.now().plusDays(7)

    propertyMap.keys.map { prop =>
      props(prop) match {
        case t: InstancePropertyValue.Timestamp =>
          justBeforeNow.isBefore(t.value.toLocalDateTime)
        case ts: InstancePropertyValue.TimestampList =>
          ts.value.forall { t =>
            justBeforeNow.isBefore(t.toLocalDateTime)
          }
        case d: InstancePropertyValue.Date =>
          d.value.isEqual(today) || d.value.isEqual(nextWeek)
        case ds: InstancePropertyValue.DateList =>
          ds.value.forall(d => d.isEqual(today) || d.isEqual(nextWeek))
        case _ => false
      }
    }.toVector shouldBe Vector(true)
  }

  private def verifyErrorMessage[A](result: Either[Exception, A], errorMsg: String): Assertion =
    result match {
      case Left(err) if err.getMessage.contains(errorMsg) => succeed
      case Left(err) =>
        fail(s"expecting error to contain '${err.getMessage}' fail but found: ${err.getMessage}")
      case Right(value) => fail(s"expecting to fail but found: ${value.toString}")
    }
}
// scalastyle:on null
