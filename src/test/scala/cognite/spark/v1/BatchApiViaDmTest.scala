package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import cognite.spark.v1.BatchApiTestSupport.FakeBatchApi
import com.cognite.sdk.scala.v1.fdm.common.{DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ViewCorePropertyDefinition,
  ViewPropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.instances.{
  InstanceDefinition,
  InstancePropertyValue,
  InstanceType,
  TypePropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.views.ViewDefinition
import io.circe.parser.parse
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import java.time.ZonedDateTime
import java.util.UUID
import scala.collection.immutable.ListMap
import scala.concurrent.duration._

object BatchApiViaDmTest {
  final case class TestProperty(
      propertyType: PropertyType,
      nullable: Boolean,
      value: InstancePropertyValue)
}

/**
  * Static-mock tests: DM instances encoded as batch-api JSON, then read back through
  * [[BatchApiRelation]], must match non-sync FDM schema and rows.
  */
class BatchApiViaDmTest extends FlatSpec with Matchers {
  import BatchApiViaDmTest._

  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("BatchApiViaDmTest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private val statusId = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
  private val config = BatchApiTestSupport.relationConfig(applicationId = "BatchApiViaDmTest")

  private val testProperties: Map[String, TestProperty] = Map(
    "name" -> TestProperty(PropertyType.TextProperty(), true, InstancePropertyValue.String("n1")),
    "count" -> TestProperty(
      PropertyType.PrimitiveProperty(PrimitivePropType.Int64),
      false,
      InstancePropertyValue.Int64(5L)),
    "active" -> TestProperty(
      PropertyType.PrimitiveProperty(PrimitivePropType.Boolean),
      true,
      InstancePropertyValue.Boolean(true)),
    "tags" -> TestProperty(
      PropertyType.TextProperty(list = Some(true)),
      true,
      InstancePropertyValue.StringList(Seq("a", "b"))),
    "seen" -> TestProperty(
      PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp),
      true,
      InstancePropertyValue.Timestamp(ZonedDateTime.parse("2024-01-02T03:04:05Z"))),
    "owner" -> TestProperty(
      PropertyType.DirectNodeRelationProperty(None, None),
      true,
      InstancePropertyValue.ViewDirectNodeRelation(
        Some(DirectRelationReference("space", "owner-1"))))
  )

  private val viewSpace = "space"
  private val viewExtId = "view"
  private val viewVersion = "v1"
  private val viewKey = s"$viewExtId/$viewVersion"

  private val testViewProperties: Map[String, ViewPropertyDefinition] = ListMap(
    testProperties.toSeq.sortBy(_._1).map {
      case (name, property) =>
        name -> (ViewCorePropertyDefinition(
          nullable = Some(property.nullable),
          defaultValue = None,
          `type` = property.propertyType): ViewPropertyDefinition)
    }: _*)

  private val testView = ViewDefinition(
    space = viewSpace,
    externalId = viewExtId,
    version = viewVersion,
    createdTime = 1L,
    lastUpdatedTime = 2L,
    writable = true,
    usedFor = Usage.Node,
    properties = testViewProperties
  )

  private val testTyping: BatchApiViaDm.Typing =
    Map(
      viewSpace -> Map(viewKey -> testProperties.map {
        case (name, property) =>
          name -> TypePropertyDefinition(
            nullable = Some(property.nullable),
            defaultValue = None,
            description = None,
            name = None,
            `type` = property.propertyType)
      }))

  private val testPropertyValues =
    Map(viewSpace -> Map(viewKey -> testProperties.map { case (name, p) => name -> p.value }))

  private def testNode(version: Option[Long] = Some(3L), deletedTime: Option[Long] = None) =
    InstanceDefinition.NodeDefinition(
      space = viewSpace,
      externalId = "n1",
      createdTime = 1L,
      lastUpdatedTime = 2L,
      deletedTime = deletedTime,
      version = version,
      properties = Some(testPropertyValues),
      `type` = Some(DirectRelationReference("typeSpace", "nodeType"))
    )

  private def testEdge =
    InstanceDefinition.EdgeDefinition(
      `type` = DirectRelationReference("typeSpace", "edgeType"),
      space = viewSpace,
      externalId = "e1",
      createdTime = 1L,
      lastUpdatedTime = 2L,
      deletedTime = Some(9L),
      version = Some(4L),
      properties = Some(testPropertyValues),
      startNode = DirectRelationReference(viewSpace, "start"),
      endNode = DirectRelationReference(viewSpace, "end")
    )

  private def fdmReference(usage: Usage) =
    new BatchApiTestSupport.FdmReference(config, usage, testViewProperties)(spark.sqlContext)

  private def batchRelation(typing: BatchApiViaDm.Typing, instanceType: InstanceType) =
    new BatchApiRelation(
      config,
      statusId,
      instanceType,
      typing,
      pollInterval = 1.millisecond)(spark.sqlContext)

  private def dataFrame(row: Row, schema: StructType) =
    spark.createDataFrame(java.util.Collections.singletonList(row), schema)

  private def readThroughBatch(
      items: Seq[InstanceDefinition],
      typing: BatchApiViaDm.Typing,
      instanceType: InstanceType,
      pageSize: Int = 100): List[InstanceDefinition] = {
    val api = FakeBatchApi.fromInstances(items, pageSize = pageSize)
    val relation = batchRelation(typing, instanceType)
    relation.getStreams(BatchApiTestSupport.clientFor(api.backend)).head.compile.toList.unsafeRunSync()
  }

  it should "build begin typing from a view definition" in {
    BatchApiViaDm.typingFromView(testView) shouldBe testTyping
  }

  it should "derive the same schema from view typing as the FDM relation for nodes" in {
    val typing = BatchApiViaDm.typingFromView(testView)
    BatchApiRelation.deriveSchema(InstanceType.Node, typing) shouldBe fdmReference(Usage.Node).schema
  }

  it should "derive the same schema from view typing as the FDM relation for edges" in {
    val typing = BatchApiViaDm.typingFromView(testView.copy(usedFor = Usage.Edge))
    BatchApiRelation.deriveSchema(InstanceType.Edge, typing) shouldBe fdmReference(Usage.Edge).schema
  }

  it should "round-trip a node through batch-api JSON including type" in {
    val typing = BatchApiViaDm.typingFromView(testView)
    val json = BatchApiViaDm.encodeReadResponse(Seq(testNode()), nextReadCursor = None)
    val decoded = json.as[ReadResponse](ReadResponse.decoder(typing)).getOrElse(fail(json.noSpaces))

    decoded.items should have size 1
    val node = decoded.items.head.asInstanceOf[InstanceDefinition.NodeDefinition]
    node.externalId shouldBe "n1"
    node.`type` shouldBe Some(DirectRelationReference("typeSpace", "nodeType"))
    node.version shouldBe Some(3L)
  }

  it should "round-trip an edge through batch-api JSON including start and end nodes" in {
    val typing = BatchApiViaDm.typingFromView(testView.copy(usedFor = Usage.Edge))
    val json = BatchApiViaDm.encodeReadResponse(Seq(testEdge), nextReadCursor = Some("next"))
    val decoded = json.as[ReadResponse](ReadResponse.decoder(typing)).getOrElse(fail(json.noSpaces))

    decoded.nextReadCursor shouldBe Some("next")
    decoded.items should have size 1
    val edge = decoded.items.head.asInstanceOf[InstanceDefinition.EdgeDefinition]
    edge.startNode shouldBe DirectRelationReference(viewSpace, "start")
    edge.endNode shouldBe DirectRelationReference(viewSpace, "end")
    edge.`type` shouldBe DirectRelationReference("typeSpace", "edgeType")
  }

  it should "page instances into swamp read responses that BatchApiRelation can consume" in {
    val nodes = Seq(
      testNode().copy(externalId = "a"),
      testNode().copy(externalId = "b"),
      testNode().copy(externalId = "c")
    )
    val (cursors, pages) = BatchApiViaDm.asSwampPages(nodes, pageSize = 2)
    cursors shouldBe Seq("dm-cursor-0")
    pages.keySet shouldBe Set("dm-cursor-0", "dm-cursor-1")

    parse(pages("dm-cursor-0")).flatMap(_.hcursor.get[Option[String]]("nextReadCursor")) shouldBe
      Right(Some("dm-cursor-1"))
    parse(pages("dm-cursor-1")).flatMap(_.hcursor.get[Option[String]]("nextReadCursor")) shouldBe
      Right(None)

    val typing = BatchApiViaDm.typingFromView(testView)
    readThroughBatch(nodes, typing, InstanceType.Node, pageSize = 2).map(_.externalId) shouldBe
      List("a", "b", "c")
  }

  it should "produce the same DataFrame as FDM after a via-DM encode/decode of a node" in {
    val typing = BatchApiViaDm.typingFromView(testView)
    val decoded = readThroughBatch(Seq(testNode()), typing, InstanceType.Node)
    decoded should have size 1

    val fdm = fdmReference(Usage.Node)
    val batch = batchRelation(typing, InstanceType.Node)
    val fdmDf = dataFrame(fdm.row(decoded.head), fdm.schema)
    val batchDf = dataFrame(batch.toRow(decoded.head, Array.empty), batch.schema)

    batchDf.schema shouldBe fdmDf.schema
    batchDf.collect().toSeq shouldBe fdmDf.collect().toSeq

    val row = batchDf.collect().head
    row.getAs[String]("name") shouldBe "n1"
    row.getAs[Long]("node.version") shouldBe 3L
    row.getAs[Row]("owner").getString(1) shouldBe "owner-1"
    row.getAs[Row]("_type").getString(1) shouldBe "nodeType"
  }

  it should "produce the same DataFrame as FDM after a via-DM encode/decode of an edge" in {
    val typing = BatchApiViaDm.typingFromView(testView.copy(usedFor = Usage.Edge))
    val decoded = readThroughBatch(Seq(testEdge), typing, InstanceType.Edge)
    decoded should have size 1

    val fdm = fdmReference(Usage.Edge)
    val batch = batchRelation(typing, InstanceType.Edge)
    val fdmDf = dataFrame(fdm.row(decoded.head), fdm.schema)
    val batchDf = dataFrame(batch.toRow(decoded.head, Array.empty), batch.schema)

    batchDf.schema shouldBe fdmDf.schema
    batchDf.collect().toSeq shouldBe fdmDf.collect().toSeq

    val row = batchDf.collect().head
    row.getAs[Long]("edge.deletedTime") shouldBe 9L
    row.getAs[Row]("startNode").getString(1) shouldBe "start"
    row.getAs[Row]("endNode").getString(1) shouldBe "end"
    edgeType(row) shouldBe "edgeType"
  }

  private def edgeType(row: Row): String = row.getAs[Row]("type").getString(1)

  it should "match FDM on projected columns after via-DM round-trip" in {
    val typing = BatchApiViaDm.typingFromView(testView)
    val decoded = readThroughBatch(Seq(testNode()), typing, InstanceType.Node).head
    val cols = Array("externalId", "name", "count")

    val fdm = fdmReference(Usage.Node)
    val batch = batchRelation(typing, InstanceType.Node)
    // FdmReference.row always projects the full schema; compare through batch projection only,
    // and against FDM's full-row values for the same fields.
    val batchRow = batch.toRow(decoded, cols)
    batchRow.getString(0) shouldBe "n1"
    batchRow.getString(1) shouldBe "n1"
    batchRow.getLong(2) shouldBe 5L

    val fdmFull = fdm.row(decoded)
    batchRow.getString(0) shouldBe fdmFull.getString(fdm.schema.fieldIndex("externalId"))
    batchRow.getString(1) shouldBe fdmFull.getString(fdm.schema.fieldIndex("name"))
    batchRow.getLong(2) shouldBe fdmFull.getLong(fdm.schema.fieldIndex("count"))
  }

  it should "keep node schema free of instanceType and type edge fields typed as structs" in {
    val typing = BatchApiViaDm.typingFromView(testView)
    val nodeSchema = BatchApiRelation.deriveSchema(InstanceType.Node, typing)
    val edgeSchema = BatchApiRelation.deriveSchema(InstanceType.Edge, typing)
    val relationStruct = StructType(
      Seq(StructField("space", StringType, false), StructField("externalId", StringType, false)))

    nodeSchema.fieldNames.toSeq should not contain "instanceType"
    edgeSchema("startNode") shouldBe StructField("startNode", relationStruct, false)
    edgeSchema("endNode") shouldBe StructField("endNode", relationStruct, false)
  }
}
