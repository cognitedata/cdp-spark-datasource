package cognite.spark.v1

import cognite.spark.v1.DataModelInstancesRelationV3.{createEdges, createNodes}
import cognite.spark.v1.FDMTestUtils.createAllPossibleViewPropCombinations
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ContainerPropertyDefinition,
  ViewPropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.TextProperty
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DataModelInstancesRelationV3Test
    extends FlatSpec
    with Matchers
    with SparkTest
    with BeforeAndAfterAll {

  private val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  private val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  private val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  private val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val bluefieldAlphaClient = getBlufieldClient(Some("alpha"))
  private val space = "test-space-scala-sdk"
  private val metricPrefix = "sparkDataSourceTestsFDMV3"

  private val destRef = ViewReference("space", "viewExtId1", "viewV1")

  it should "fail to create nodes when externalId is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
    result.left.get.getMessage.contains("'externalId' shouldn't be null") shouldBe true
  }

  it should "fail to create nodes when required a property is null" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        ),
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
    result.left.get.getMessage.contains("Can't find required properties") shouldBe true
  }

  it should "fail to create nodes when required a property is nullable" in {
    val propertyMap = Map(
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        ),
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
    result.left.get.getMessage.contains("cannot be nullable") shouldBe true
  }

  it should "successfully create nodes with all nullable/non-nullable properties" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        )
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false))
      )

    val values = Array[Array[Any]](Array("stringProp1", "extId1"), Array("stringProp2", "extId2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    nodes.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2")
  }

  it should "successfully create nodes with only required properties" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false))
      )

    val values = Array[Array[Any]](Array("stringProp1", "extId1"), Array("stringProp2", "extId2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    nodes.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2")
  }

  it should "successfully create nodes when there are unrelated properties in Rows" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "externalId" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test externalId"),
          name = Some(s"ext-id"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
    )
    val schema =
      StructType(
        Array(
          StructField("stringProp", StringType, nullable = false),
          StructField("externalId", StringType, nullable = false),
          StructField("intProp", IntegerType, nullable = true),
          StructField("unrelatedProp", StringType, nullable = false)
        )
      )

    val values =
      Array[Array[Any]](
        Array("stringProp1", "extId1", 1, "unrelatedProp1"),
        Array("stringProp2", "extId2", null, "unrelatedProp2"))
    val rows = values.map(r => new GenericRowWithSchema(r, schema))

    val result = createNodes("instanceSpaceExternalId1", rows, schema, propertyMap, destRef)
    result.isRight shouldBe true

    val nodes = result.right.get
    nodes.map(_.space).distinct.head shouldBe "instanceSpaceExternalId1"
    nodes.map(_.externalId).distinct.sorted shouldBe Vector("extId1", "extId2")
  }

  it should "fail to create edges when externalId is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
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
      .contains("Couldn't find required property 'type'") shouldBe true
  }

  it should "fail to create edges when startNode is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
    )
    val relationRefSchema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
        StructField("externalId", StringType, nullable = false)
      )
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
      .contains("Couldn't find required property 'startNode'") shouldBe true
  }

  it should "fail to create edges when endNode is not present" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
    )
    val relationRefSchema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
        StructField("externalId", StringType, nullable = false)
      )
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
      .contains("Couldn't find required property 'endNode'") shouldBe true
  }

  it should "fail to create edges when type.space is null" in {
    val propertyMap = Map(
      "stringProp" ->
        ViewPropertyDefinition(
          nullable = Some(false),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Str Description"),
          name = Some(s"Test-Str-Name"),
          `type` = PropertyType.TextProperty()
        ),
      "intProp" ->
        ViewPropertyDefinition(
          nullable = Some(true),
          autoIncrement = None,
          defaultValue = None,
          description = Some(s"Test Int Description"),
          name = Some(s"Test-Int-Name"),
          `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32)
        )
    )
    val relationRefSchema = StructType(
      Array(
        StructField("space", StringType, nullable = false),
        StructField("externalId", StringType, nullable = false)
      )
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
      .contains("(Edge type) shouldn't contain null values") shouldBe true
  }

  //  private def fetchInstancesByExternalId(
//      space: String,
//      containerExternalId: String,
//      instanceType: InstanceType,
//      instanceExternalId: String): InstanceFilterResponse =
//    bluefieldAlphaClient.instances
//      .retrieveByExternalIds(
//        items = Seq(
//          InstanceRetrieve(
//            sources = Some(Seq(ContainerReference(space, containerExternalId))),
//            instanceType = instanceType,
//            externalId = instanceExternalId,
//            space = space
//          )
//        ),
//        includeTyping = true
//      )
//      .unsafeRunSync()
//
//  private def readRows(containerExternalId: String, metricPrefix: String) =
//    spark.read
//      .format("cognite.spark.v1")
//      .option("baseUrl", "https://bluefield.cognitedata.com")
//      .option("tokenUri", tokenUri)
//      .option("clientId", clientId)
//      .option("clientSecret", clientSecret)
//      .option("project", "extractor-bluefield-testing")
//      .option("scopes", "https://bluefield.cognitedata.com/.default")
//      .option("vehicleContainerExternalId", containerExternalId)
//      .option("space", space)
//      .option("collectMetrics", true)
//      .option("metricsPrefix", metricPrefix)
//      .option("type", DataModelInstancesRelationV3.ResourceType)
//      .load()
//
//  private def insertRows(
//      containerExternalId: String,
//      df: DataFrame,
//      onConflict: String = "upsert"): Unit =
//    df.write
//      .format("cognite.spark.v1")
//      .option("type", DataModelInstancesRelationV3.ResourceType)
//      .option("baseUrl", "https://bluefield.cognitedata.com")
//      .option("tokenUri", tokenUri)
//      .option("clientId", clientId)
//      .option("clientSecret", clientSecret)
//      .option("project", "extractor-bluefield-testing")
//      .option("scopes", "https://bluefield.cognitedata.com/.default")
//      .option("containerExternalId", containerExternalId)
//      .option("space", space)
//      .option("onconflict", onConflict)
//      .option("collectMetrics", true)
//      .option("metricsPrefix", containerExternalId)
//      .save()
//
}
