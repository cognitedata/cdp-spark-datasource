package cognite.spark.v1.fdm

import cognite.spark.v1.fdm.RelationUtils.FlexibleDataModelRelationUtils._
import cognite.spark.v1.fdm.RelationUtils.RowDataExtractors.extractInstancePropertyValue
import cognite.spark.v1.fdm.utils.FDMViewPropertyDefinitions._
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.instances.InstancePropertyValue.{ViewDirectNodeRelation, ViewDirectNodeRelationList}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceDeletionRequest, InstancePropertyValue}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{Assertion, FlatSpec, Matchers}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}

class FlexibleDataModelRelationUtilsTest extends FlatSpec with Matchers {

  private val destRef = ViewReference("space", "viewExtId1", "viewV1")

  private val relationRefSchema: StructType = StructType(
    Array(
      StructField("spaceExternalId", StringType, nullable = false),
      StructField("externalId", StringType, nullable = false)
    )
  )

  private val relationRefWithoutSpaceSchema: StructType = StructType(
    Array(
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

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
      Array[Any]("stringProp1", "extId1", null),
      Array[Any](null, null, 5)
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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

    val values = Seq[Array[Any]](Array[Any]("stringProp1", "extId1", 1), Array[Any](null, "extId1", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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

    val values = Seq[Array[Any]](Array[Any]("extId1", 1), Array[Any]("extId2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    verifyErrorMessage(result, "Could not find required properties")
  }

  it should "successfully create nodes with instance space extracted from data" in {
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
          StructField("space", StringType, nullable = false),
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", StringType, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Seq[Array[Any]](
        Array[Any]("space1", "stringProp1", "extId1", null, Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("space1", "stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), None)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct shouldBe Vector("space1")
  }

  it should "successfully create edges with instance space extracted from data" in {
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
        StructField("space", StringType, nullable = false),
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
      Array[Any](
        "space1",
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array(
        "space1",
        "stringProp2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeExtId2"), relationRefWithoutSpaceSchema),
        new GenericRowWithSchema(Array("startNodeExtId2"), relationRefWithoutSpaceSchema),
        new GenericRowWithSchema(Array("endNodeExtId2"), relationRefWithoutSpaceSchema),
        Array(2.1, 2.2),
        null
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createEdges(rows, schema, propertyMap, Some(destRef), None)
    result.isRight shouldBe true

    val edges = result.toOption.getOrElse(Vector.empty)
    (edges.map(_.space).distinct should contain).theSameElementsAs(Vector("space1"))
    (edges.map(_.`type`.space).distinct should contain).theSameElementsAs(Vector("space1", "typeSpace1"))
    (edges.map(_.startNode.space).distinct should contain)
      .theSameElementsAs(Vector("space1", "startNodeSpace1"))
    (edges.map(_.endNode.space).distinct should contain)
      .theSameElementsAs(Vector("space1", "endNodeSpace1"))
  }

  it should "successfully create connection instances with space extracted from data" in {
    val schema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
        StructField("externalId", IntegerType, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false),
      )
    )

    val values = Seq[Array[Any]](
      Array(
        "space1",
        "extId1",
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeExtId1"), relationRefWithoutSpaceSchema)
      ),
      Array(
        "space1",
        "extId2",
        new GenericRowWithSchema(Array("startNodeExtId2"), relationRefWithoutSpaceSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createConnectionInstances(
      DirectRelationReference(space = "edgeTypeSpace", externalId = "edgeTypeExternalId"),
      schema,
      rows,
      None)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    (nodes.map(_.startNode.space).distinct should contain)
      .theSameElementsAs(Vector("space1", "startNodeSpace1"))
    (nodes.map(_.endNode.space).distinct should contain)
      .theSameElementsAs(Vector("space1", "endNodeSpace1"))
    nodes.map(_.space).distinct shouldBe Vector("space1")
    nodes.map(_.`type`.space).distinct shouldBe Vector("edgeTypeSpace")
  }

  it should "successfully create connection instances with instanceSpace" in {
    val schema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
        StructField("externalId", IntegerType, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false),
      )
    )

    val values = Seq[Array[Any]](
      Array[Any](
        null,
        "extId1",
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeExtId1"), relationRefWithoutSpaceSchema)
      ),
      Array(
        "space1",
        "extId2",
        new GenericRowWithSchema(Array("startNodeExtId2"), relationRefWithoutSpaceSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createConnectionInstances(
      DirectRelationReference(space = "edgeTypeSpace", externalId = "edgeTypeExternalId"),
      schema,
      rows,
      instanceSpace = Some("space2"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    (nodes.map(_.startNode.space).distinct should contain)
      .theSameElementsAs(Vector("space2", "startNodeSpace1"))
    (nodes.map(_.endNode.space).distinct should contain)
      .theSameElementsAs(Vector("space2", "endNodeSpace1"))
    nodes.map(_.space).distinct shouldBe Vector("space2")
    nodes.map(_.`type`.space).distinct shouldBe Vector("edgeTypeSpace")
  }

  it should "successfully create nodes with direct relation references and list of direct relation references" in {
    val propertyMap = Map(
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "dr" -> DirectNodeRelationPropertyNonListWithoutDefaultValueNullable,
      "ldr" -> DirectNodeRelationViewPropertyListWithoutDefaultValueNullable
    )
    val schema =
      StructType(
        Array(
          StructField("externalId", StringType, nullable = false),
          StructField(
            "dr",
            relationRefSchema,
            nullable = false
          ),
          StructField(
            "ldr",
            ArrayType(relationRefSchema),
            nullable = false
          )
        )
      )

    val values =
      Seq[Array[Any]](
        Array(
          1999,
          new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
          Array(new GenericRowWithSchema(Array("startNodeSpace2", "startNodeExtId2"), relationRefSchema))
        )
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.get("1999") shouldNot be(None)
    extIdPropsMap.get("1999").map(_.contains("dr")) shouldBe Some(true)
    extIdPropsMap.get("1999").map(_.contains("ldr")) shouldBe Some(true)
    extIdPropsMap.get("1999").flatMap(_.get("dr")).flatten shouldBe
      Some(ViewDirectNodeRelation(Some(DirectRelationReference("startNodeSpace1", "startNodeExtId1"))))
    extIdPropsMap.get("1999").flatMap(_.get("ldr")).flatten shouldBe
      Some(
        ViewDirectNodeRelationList(Seq(DirectRelationReference("startNodeSpace2", "startNodeExtId2"))))
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
        Array[Any]("stringProp1", "extId1", null, Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(5)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any]("stringProp1", "extId1", Seq[Any](1.1, 1.2, null)),
      Array("stringProp2", "extId2", Array(2.1, 2.2)))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
        Array[Any]("stringProp1", "extId1", 1, "unrelatedProp1", Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("stringProp2", "extId2", null, "unrelatedProp2", Array(2.1, 2.2), null)
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "intProp" -> Some(InstancePropertyValue.Int32(1)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
      ))
  }

  it should "fail to create nodes space is not provided" in {
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
          StructField("space", StringType, nullable = false),
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", StringType, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Seq[Array[Any]](
        Array[Any]("space1", "stringProp1", "extId1", null, Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any](null, "stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes(rows, schema, propertyMap, Some(destRef), None)
    result match {
      case Left(err) =>
        err.getMessage.contains(
          "There's no 'instanceSpace' specified to be used as default space and could not extract 'space' from data") shouldBe true
      case Right(_) => fail("Expecting to fail but succeeded")
    }
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

    val values = Seq[Array[Any]](Array[Any]("str1", 1), Array[Any]("str2", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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

    val values = Seq[Array[Any]](Array[Any]("str1", null, "externalId1"), Array[Any]("str2", 2, "externalId2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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

    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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

    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
        StructField("space", StringType, nullable = false),
        StructField("stringProp", StringType, nullable = false),
        StructField("intProp", IntegerType, nullable = true),
        StructField("externalId", IntegerType, nullable = false),
        StructField("type", relationRefSchema, nullable = false),
        StructField("startNode", relationRefSchema, nullable = false),
        StructField("endNode", relationRefSchema, nullable = false)
      )
    )

    val values = Seq[Array[Any]](
      Array[Any](
        "space1",
        "str1",
        null,
        "externalId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema)
      ),
      Array[Any](
        null,
        "str2",
        2,
        "externalId2",
        new GenericRowWithSchema(Array(null, "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges(rows, schema, propertyMap, Some(destRef), None)
    verifyErrorMessage(result, "'space' cannot be null")
  }

  it should "successfully create edges with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "enumProp" -> EnumPropertyNonListWithoutDefaultValueNullable,
      "stringProp" ->
        TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" ->
        Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
      "doubleListProp" -> Float64ListWithoutDefaultValueNonNullable,
      "floatListProp" -> Float32ListWithoutDefaultValueNullable
    )
    val schema = StructType(
      Array(
        StructField("enumProp", StringType, nullable = false),
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
      Array[Any](
        "VAL1",
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
        "VAL1",
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
    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "enumProp" -> Some(InstancePropertyValue.Enum("VAL1")),
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "enumProp" -> Some(InstancePropertyValue.Enum("VAL1")),
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(2)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any](
        "stringProp1",
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null)
      ),
      Array[Any](
        "stringProp2",
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any](
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
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

    val result = createEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(2)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
      Array[Any]("stringProp1", "extId1", null),
      Array[Any](null, null, 5)
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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

    val values = Seq[Array[Any]](Array("stringProp1", "extId1", 1), Array[Any](null, "extId1", null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
        Array[Any]("stringProp1", "extId1", null, Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("stringProp2", "extId2", 5, Array(2.1, 2.2), null))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(5)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any]("stringProp1", "extId1", Seq[Any](1.1, 1.2, null)),
      Array[Any]("stringProp2", "extId2", Array(2.1, 2.2)))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
        Array[Any]("stringProp1", "extId1", 1, "unrelatedProp1", Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("stringProp2", "extId2", null, "unrelatedProp2", Array[Any](2.1, 2.2), null)
      )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[NodeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "intProp" -> Some(InstancePropertyValue.Int32(1)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any](
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(5)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any](
        "stringProp1",
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null)
      ),
      Array[Any](
        "stringProp2",
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
      Array[Any](
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
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

    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty).asInstanceOf[Vector[EdgeWrite]]
    nodes.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    val extIdPropsMap = nodes.map { e =>
      e.externalId -> e.sources
        .getOrElse(Seq.empty)
        .flatMap(s => s.properties.getOrElse(Map.empty))
        .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(5)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
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
      Array[Any](
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
        "stringProp2",
        2,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2),
        null
      ),
      Array[Any](
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2", "extId3")
    val extIdPropsMap = nodesOrEdges.map {
      case n: NodeWrite =>
        n.externalId -> n.sources
          .getOrElse(Seq.empty)
          .flatMap(s => s.properties.getOrElse(Map.empty))
          .toMap
      case e: EdgeWrite =>
        e.externalId -> e.sources
          .getOrElse(Seq.empty)
          .flatMap(s => s.properties.getOrElse(Map.empty))
          .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    extIdPropsMap.contains("extId3") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(2)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp3")),
        "intProp" -> Some(InstancePropertyValue.Int32(3)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(3.1, 3.2)))
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
      Array[Any](
        "stringProp1",
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        Seq[Any](1.1, 1.2, null)
      ),
      Array[Any](
        "stringProp2",
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        Array(2.1, 2.2)
      ),
      Array[Any](
        "stringProp3",
        "extId3",
        null,
        null,
        null,
        Seq(3.1, 3.2)
      )
    )
    val rows = values.map(r => new GenericRowWithSchema(r, schema))
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.space).distinct.headOption shouldBe Some("instanceSpaceExternalId1")
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2", "extId3")
    val extIdPropsMap = nodesOrEdges.map {
      case n: NodeWrite =>
        n.externalId -> n.sources
          .getOrElse(Seq.empty)
          .flatMap(s => s.properties.getOrElse(Map.empty))
          .toMap
      case e: EdgeWrite =>
        e.externalId -> e.sources
          .getOrElse(Seq.empty)
          .flatMap(s => s.properties.getOrElse(Map.empty))
          .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    extIdPropsMap.contains("extId3") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp3")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(3.1, 3.2)))
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
      Array[Any](
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
        "stringProp2",
        5,
        "extId2",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId2"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId2"), relationRefSchema),
        "unrelatedProp2",
        Array(2.1, 2.2)
      ),
      Array[Any](
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2", "extId3")
    val extIdPropsMap = nodesOrEdges.map {
      case n: NodeWrite =>
        n.externalId -> n.sources
          .getOrElse(Seq.empty)
          .flatMap(s => s.properties.getOrElse(Map.empty))
          .toMap
      case e: EdgeWrite =>
        e.externalId -> e.sources
          .getOrElse(Seq.empty)
          .flatMap(s => s.properties.getOrElse(Map.empty))
          .toMap
    }.toMap
    extIdPropsMap.contains("extId1") shouldBe true
    extIdPropsMap.contains("extId2") shouldBe true
    extIdPropsMap.contains("extId3") shouldBe true
    (extIdPropsMap("extId1") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp1")),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(List(1.1, 1.2))),
        "floatListProp" -> Some(InstancePropertyValue.Float32List(List(2.1F)))
      ))
    (extIdPropsMap("extId2") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp2")),
        "intProp" -> Some(InstancePropertyValue.Int32(5)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(2.1, 2.2)))
      ))
    (extIdPropsMap("extId3") should contain).theSameElementsAs(
      Map(
        "stringProp" -> Some(InstancePropertyValue.String("stringProp3")),
        "intProp" -> Some(InstancePropertyValue.Int32(6)),
        "doubleListProp" -> Some(InstancePropertyValue.Float64List(Seq(3.1, 3.2)))
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
    val result = createNodesOrEdges(rows, schema, propertyMap, Some(destRef), Some("instanceSpaceExternalId1"))
    result.isRight shouldBe true

    val nodesOrEdges = result.toOption.getOrElse(Vector.empty)
    nodesOrEdges.map(_.externalId).distinct.sorted shouldBe Vector("extId1")
    val props = nodesOrEdges
      .collect {
        case n: NodeWrite => n.sources.getOrElse(Seq.empty).flatMap(_.properties.getOrElse(Map.empty))
      }
      .flatten
      .toMap

    val justBeforeNow = LocalDateTime.now(ZoneId.of("UTC")).minusMinutes(10)
    val today = LocalDate.now()
    val nextWeek = LocalDate.now().plusDays(7)

    propertyMap.keys.map { prop =>
      props(prop).get match {
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

  it should "successfully delete nodes from the specified instanceSpace" in {
    val schema =
      StructType(
        Array(
          StructField("space", StringType, nullable = false),
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", StringType, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Seq[Array[Any]](
        Array[Any]("space2", "stringProp1", "extId1", null, Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("space2", "stringProp2", "extId2", 5, Array(2.1, 2.2), null))

    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodeDeleteData(schema, rows, Some("space1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.collect {
      case n: InstanceDeletionRequest.NodeDeletionRequest => n.space
    }.distinct shouldBe Vector("space1")

    val extIds = nodes.collect {
      case n: InstanceDeletionRequest.NodeDeletionRequest => n.externalId
    }
    extIds.contains("extId1") shouldBe true
    extIds.contains("extId2") shouldBe true
  }

  it should "successfully delete nodes from the space in data" in {
    val schema =
      StructType(
        Array(
          StructField("space", StringType, nullable = false),
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", StringType, nullable = true),
          StructField("doubleListProp", ArrayType(DoubleType), nullable = false),
          StructField("floatListProp", ArrayType(FloatType), nullable = true)
        )
      )

    val values =
      Seq[Array[Any]](
        Array[Any]("space2", "stringProp1", "extId1", null, Seq[Any](1.1, 1.2, null), Array[Any](2.1, null)),
        Array[Any]("space2", "stringProp2", "extId2", 5, Array(2.1, 2.2), null))

    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodeDeleteData(schema, rows, None)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.collect {
      case n: InstanceDeletionRequest.NodeDeletionRequest => n.space
    }.distinct shouldBe Vector("space2")

    val extIds = nodes.collect {
      case n: InstanceDeletionRequest.NodeDeletionRequest => n.externalId
    }
    extIds.contains("extId1") shouldBe true
    extIds.contains("extId2") shouldBe true
  }

  it should "successfully delete edges from the specified instanceSpace" in {
    val schema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
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
      Array[Any](
        "space2",
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
        "space2",
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

    val result = createEdgeDeleteData(schema, rows, Some("space1"))
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.collect {
      case n: InstanceDeletionRequest.EdgeDeletionRequest => n.space
    }.distinct shouldBe Vector("space1")

    val extIds = nodes.collect {
      case n: InstanceDeletionRequest.EdgeDeletionRequest => n.externalId
    }
    extIds.contains("extId1") shouldBe true
    extIds.contains("extId2") shouldBe true
  }

  it should "successfully delete edges from the space in data" in {
    val schema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
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
      Array[Any](
        "space2",
        "stringProp1",
        null,
        "extId1",
        new GenericRowWithSchema(Array("typeSpace1", "typeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("startNodeSpace1", "startNodeExtId1"), relationRefSchema),
        new GenericRowWithSchema(Array("endNodeSpace1", "endNodeExtId1"), relationRefSchema),
        "unrelatedProp1",
        Seq[Any](1.1, 1.2, null),
        Array[Any](2.1, null)
      ),
      Array[Any](
        "space2",
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

    val result = createEdgeDeleteData(schema, rows, None)
    result.isRight shouldBe true

    val nodes = result.toOption.getOrElse(Vector.empty)
    nodes.collect {
      case n: InstanceDeletionRequest.EdgeDeletionRequest => n.space
    }.distinct shouldBe Vector("space2")

    val extIds = nodes.collect {
      case n: InstanceDeletionRequest.EdgeDeletionRequest => n.externalId
    }
    extIds.contains("extId1") shouldBe true
    extIds.contains("extId2") shouldBe true
  }

  private def verifyErrorMessage[A](result: Either[Exception, A], errorMsg: String): Assertion =
    result match {
      case Left(err) if err.getMessage.contains(errorMsg) => succeed
      case Left(err) =>
        fail(s"expecting error to contain '${err.getMessage}' fail but found: ${err.getMessage}")
      case Right(value) => fail(s"expecting to fail but found: ${value.toString}")
    }

  it should "return the instance property value for numbers in correct type" in {
    val valuesToTest = List(
      InstancePropertyValue.Int64(100L),
      InstancePropertyValue.Int32(100),
      InstancePropertyValue.Float64(100.0),
      InstancePropertyValue.Float32(100f))
    valuesToTest.map(extractInstancePropertyValue(LongType, _) shouldBe 100L)
    valuesToTest.map(extractInstancePropertyValue(FloatType, _) shouldBe 100f)
    valuesToTest.map(extractInstancePropertyValue(DoubleType, _) shouldBe 100.0)
    valuesToTest.map(extractInstancePropertyValue(IntegerType, _) shouldBe 100)

    val arrayTypesToTest = List(
      InstancePropertyValue.Int64List(Seq(100L, 200L, 300L)),
      InstancePropertyValue.Int32List(Seq(100, 200, 300)),
      InstancePropertyValue.Float64List(Seq(100.0, 200.0, 300.0)),
      InstancePropertyValue.Float32List(Seq(100.0f, 200.0f, 300.0f))
    )

    arrayTypesToTest.map(
      extractInstancePropertyValue(ArrayType(LongType), _) shouldBe List(100L, 200L, 300L))
    arrayTypesToTest.map(
      extractInstancePropertyValue(ArrayType(FloatType), _) shouldBe List(100f, 200f, 300f))
    arrayTypesToTest.map(
      extractInstancePropertyValue(ArrayType(IntegerType), _) shouldBe List(100, 200, 300))
    arrayTypesToTest.map(
      extractInstancePropertyValue(ArrayType(DoubleType), _) shouldBe List(100.0, 200.0, 300.0))

  }

  it should "return the instance property value for strings formatted as date in correct type" in {
    val expected = "2023-01-01"
    val valuesToTest =
      List(InstancePropertyValue.String(expected), InstancePropertyValue.Date(LocalDate.parse(expected)))
    valuesToTest.map(extractInstancePropertyValue(StringType, _) shouldBe expected)

    val arrayTypesToTest = List(
      InstancePropertyValue.StringList(Seq(expected)),
      InstancePropertyValue.DateList(Seq(LocalDate.parse(expected)))
    )
    arrayTypesToTest.map(extractInstancePropertyValue(ArrayType(StringType), _) shouldBe List(expected))
  }

  it should "return the instance property value for strings formatted as timestamp in correct type" in {
    val expected = "2007-12-03T10:15:30+01:00"
    val valuesToTest = List(
      InstancePropertyValue.String(expected),
      InstancePropertyValue.Timestamp(ZonedDateTime.parse(expected))
    )
    valuesToTest.map(extractInstancePropertyValue(StringType, _) shouldBe expected)

    val arrayTypesToTest = List(
      InstancePropertyValue.StringList(Seq(expected)),
      InstancePropertyValue.TimestampList(Seq(ZonedDateTime.parse(expected)))
    )
    arrayTypesToTest.map(extractInstancePropertyValue(ArrayType(StringType), _) shouldBe List(expected))
  }

}
