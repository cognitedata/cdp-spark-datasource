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
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import java.time.ZonedDateTime
import java.util.UUID
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Using

object BatchApiRelationTest {
  final case class TestProperty(
      propertyType: PropertyType,
      nullable: Boolean,
      value: InstancePropertyValue)
}

class BatchApiRelationTest extends FlatSpec with Matchers {
  import BatchApiRelationTest._

  private val project = "test-project"
  private val statusId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val readyId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val basePath = Seq("api", "v1", "projects", project, "models", "swamp")

  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("BatchApiRelationTest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private def config(collectMetrics: Boolean = false, metricsPrefix: String = ""): RelationConfig =
    BatchApiTestSupport.relationConfig(
      project = project,
      collectMetrics = collectMetrics,
      metricsPrefix = metricsPrefix,
      applicationId = "BatchApiRelationTest")

  private def node(externalId: String): String =
    s"""{"instanceType":"node","space":"space","externalId":"$externalId","version":1,"createdTime":1,"lastUpdatedTime":2,"properties":{}}"""

  private def page(externalIds: Seq[String], next: Option[String]): String =
    s"""{"items":[${externalIds.map(node).mkString(",")}]${next
      .map(c => s""","nextReadCursor":"$c"""")
      .getOrElse("")}}"""

  private val pollPending = """{"status":"pending"}"""
  private val pollBackfilling = """{"status":"backfilling"}"""
  private val pollProgressing = """{"status":"progressing"}"""
  private val pollFailed = """{"status":"failed"}"""
  private val pollReady = FakeBatchApi.readyPoll(readyId)

  private def relation(
      config: RelationConfig,
      typing: BatchApiViaDm.Typing = Map.empty,
      instanceType: InstanceType = InstanceType.Node) =
    new BatchApiRelation(
      config,
      statusId,
      instanceType,
      typing,
      pollInterval = 1.millisecond)(spark.sqlContext)

  private def readAll(relation: BatchApiRelation, api: FakeBatchApi): List[String] = {
    val streams = relation.getStreams(BatchApiTestSupport.clientFor(api.backend, project))
    (streams should have).size(1)
    streams.head.compile.toList.unsafeRunSync().map(_.externalId)
  }

  it should "follow nextReadCursor until it is absent" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollReady),
      readCursors = Seq("c1"),
      pages = Map(
        "c1" -> page(Seq("a", "b"), Some("c2")),
        "c2" -> page(Seq("c"), None)
      )
    )

    readAll(relation(config()), api) shouldBe List("a", "b", "c")

    api.recorded.map(r => (r.method, r.path, r.body)) shouldBe List(
      ("GET", basePath :+ "poll" :+ statusId.toString, ""),
      ("GET", basePath :+ "cursors" :+ readyId.toString, ""),
      ("POST", basePath :+ "read", """{"readCursor":"c1"}"""),
      ("POST", basePath :+ "read", """{"readCursor":"c2"}""")
    )
  }

  it should "read several starting cursors one after another in a single stream" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollReady),
      readCursors = Seq("c1", "c2"),
      pages = Map(
        "c1" -> page(Seq("a"), None),
        "c2" -> page(Seq("b"), None)
      )
    )

    readAll(relation(config()), api) shouldBe List("a", "b")
  }

  it should "poll until the batch is ready, counting each status" in {
    val prefix = s"batchapi-test-${UUID.randomUUID()}"
    val api = new FakeBatchApi(
      pollResponses = Seq(pollPending, pollPending, pollBackfilling, pollProgressing, pollReady),
      readCursors = Seq("c1"),
      pages = Map("c1" -> page(Seq("a"), None))
    )

    readAll(relation(config(collectMetrics = true, metricsPrefix = prefix)), api) shouldBe List("a")

    api.recorded.count(_.path.contains("poll")) shouldBe 5
    def pollCount(status: String): Long =
      MetricsSource.metricsMap.get(s"$prefix.batchapi.poll.$status").value.getCount
    pollCount("pending") shouldBe 2
    pollCount("backfilling") shouldBe 1
    pollCount("progressing") shouldBe 1
    pollCount("ready") shouldBe 1
  }

  it should "fail without fetching cursors when the batch fails" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollPending, pollFailed),
      readCursors = Seq("c1"),
      pages = Map.empty
    )

    val e = intercept[CdfSparkException] {
      readAll(relation(config()), api)
    }

    e.getMessage should include(statusId.toString)
    api.recorded.map(_.path.dropWhile(_ != "swamp").drop(1).head) shouldBe List("poll", "poll")
  }

  it should "tag its requests with the BATCHAPI resource type" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollReady),
      readCursors = Seq("c1"),
      pages = Map("c1" -> page(Seq("a"), None))
    )

    readAll(relation(config()), api) shouldBe List("a")

    (api.recorded should have).size(3)
    api.recorded.map(_.resourceType) shouldBe List.fill(3)(Some("BATCHAPI"))
  }

  // Fixtures copied from the batch API's own DTO tests (services/batch-api/api/src/test/resources),
  // so that a change in its wire format shows up here.
  private def resourceJson(name: String): Json =
    Using.resource(Source.fromResource(s"batchapi/$name"))(source => parse(source.mkString)) match {
      case Right(json) => json
      case Left(e) => fail(s"Could not parse $name: ${e.getMessage}")
    }

  private def fixtureTyping: BatchApiViaDm.Typing =
    resourceJson("begin_response.json").hcursor.downField("typing").as[BatchApiViaDm.Typing] match {
      case Right(typing) => typing
      case Left(e) => fail(s"Could not decode typing: ${e.getMessage}")
    }

  private def fixtureReadResponse(typing: BatchApiViaDm.Typing): ReadResponse =
    resourceJson("read_response.json").as[ReadResponse](ReadResponse.decoder(typing)) match {
      case Right(response) => response
      case Left(e) => fail(s"Could not decode the read response: ${e.getMessage}")
    }

  it should "derive its schema from the begin typing, followed by the node columns" in {
    val schema = BatchApiRelation.deriveSchema(InstanceType.Node, fixtureTyping)

    schema.fieldNames.toSeq shouldBe Seq(
      "prop1",
      "space",
      "externalId",
      "node.version",
      "node.createdTime",
      "node.lastUpdatedTime",
      "node.deletedTime",
      "_type")
    schema("prop1") shouldBe StructField("prop1", StringType, nullable = true)
  }

  it should "use the edge columns for edges" in {
    val schema = BatchApiRelation.deriveSchema(InstanceType.Edge, fixtureTyping)

    schema.fieldNames.toSeq should contain allOf ("prop1", "edge.version", "type", "startNode", "endNode")
    schema.fieldNames.toSeq should not contain "node.version"
  }

  it should "decode a batch API read response and turn its items into rows" in {
    val typing = fixtureTyping
    val response = fixtureReadResponse(typing)
    response.nextReadCursor shouldBe Some("nextCursor")
    response.items should have size 1

    val row = relation(config(), typing).toRow(response.items.head, Array.empty)

    val expected: Seq[Any] =
      Seq("value", "mySpace", "node-1", 1L, 1700000000000L, 1700000000000L, 0L, null)
    row.toSeq shouldBe expected
  }

  it should "only return the requested columns" in {
    val typing = fixtureTyping
    val response = fixtureReadResponse(typing)

    val row = relation(config(), typing).toRow(response.items.head, Array("externalId", "prop1"))

    row.toSeq shouldBe Seq("node-1", "value")
  }

  // Properties of several kinds, described both as a begin typing and as an FDM view.
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

  private val testTyping: BatchApiViaDm.Typing =
    Map(
      "space" -> Map("view/v1" -> testProperties.map {
        case (name, property) =>
          name -> TypePropertyDefinition(
            nullable = Some(property.nullable),
            defaultValue = None,
            description = None,
            name = None,
            `type` = property.propertyType)
      }))

  // Sorted by name, like batch, so that the property columns come out in the same order.
  private val testViewProperties: Map[String, ViewPropertyDefinition] = ListMap(
    testProperties.toSeq.sortBy(_._1).map {
      case (name, property) =>
        name -> (ViewCorePropertyDefinition(
          nullable = Some(property.nullable),
          defaultValue = None,
          `type` = property.propertyType): ViewPropertyDefinition)
    }: _*)

  private val testPropertyValues =
    Map("space" -> Map("view/v1" -> testProperties.map { case (name, p) => name -> p.value }))

  private def testNode(version: Option[Long] = None, deletedTime: Option[Long] = None) =
    InstanceDefinition.NodeDefinition(
      space = "space",
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
      space = "space",
      externalId = "e1",
      createdTime = 1L,
      lastUpdatedTime = 2L,
      deletedTime = Some(9L),
      version = Some(4L),
      properties = Some(testPropertyValues),
      startNode = DirectRelationReference("space", "start"),
      endNode = DirectRelationReference("space", "end")
    )

  private def fdmReference(usage: Usage) =
    new BatchApiTestSupport.FdmReference(config(), usage, testViewProperties)(spark.sqlContext)

  private val relationStruct = StructType(
    Seq(StructField("space", StringType, false), StructField("externalId", StringType, false)))

  it should "have the same schema as the FDM relation for nodes" in {
    BatchApiRelation.deriveSchema(InstanceType.Node, testTyping) shouldBe
      fdmReference(Usage.Node).schema
  }

  it should "have the same schema as the FDM relation for edges" in {
    BatchApiRelation.deriveSchema(InstanceType.Edge, testTyping) shouldBe
      fdmReference(Usage.Edge).schema
  }

  it should "not expose an instanceType column, and expose typed start and end nodes for edges" in {
    val nodeSchema = BatchApiRelation.deriveSchema(InstanceType.Node, testTyping)
    val edgeSchema = BatchApiRelation.deriveSchema(InstanceType.Edge, testTyping)

    nodeSchema.fieldNames.toSeq should not contain "instanceType"
    edgeSchema.fieldNames.toSeq shouldBe Seq(
      "active",
      "count",
      "name",
      "owner",
      "seen",
      "tags",
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
    edgeSchema("startNode") shouldBe StructField("startNode", relationStruct, false)
    edgeSchema("endNode") shouldBe StructField("endNode", relationStruct, false)
    edgeSchema("type") shouldBe StructField("type", relationStruct, true)
    edgeSchema("_type") shouldBe StructField("_type", relationStruct, false)
  }

  it should "default version to -1 and deletedTime to 0 when the instance has none" in {
    val row = relation(config(), testTyping)
      .toRow(testNode(), Array("node.version", "node.deletedTime"))

    val expected: Seq[Any] = Seq(-1L, 0L)
    row.toSeq shouldBe expected
  }

  it should "keep the version and deletedTime of an edge that has them" in {
    val row = relation(config(), testTyping, InstanceType.Edge)
      .toRow(testEdge, Array("edge.version", "edge.deletedTime"))

    val expected: Seq[Any] = Seq(4L, 9L)
    row.toSeq shouldBe expected
  }

  private def dataFrame(row: Row, schema: StructType) =
    spark.createDataFrame(java.util.Collections.singletonList(row), schema)

  it should "produce the same DataFrame as the FDM relation for a node" in {
    val node = testNode(version = Some(3L))
    val fdm = fdmReference(Usage.Node)
    val batch = relation(config(), testTyping)

    val fdmDf = dataFrame(fdm.row(node), fdm.schema)
    val batchDf = dataFrame(batch.toRow(node, Array.empty), batch.schema)

    batchDf.schema shouldBe fdmDf.schema
    batchDf.collect().toSeq shouldBe fdmDf.collect().toSeq

    // guards against comparing two frames that are empty or all null
    val row = batchDf.collect().head
    row.getAs[String]("name") shouldBe "n1"
    row.getAs[Long]("node.version") shouldBe 3L
    row.getAs[Row]("owner").getString(1) shouldBe "owner-1"
    row.getAs[Row]("_type").getString(1) shouldBe "nodeType"
  }

  it should "produce the same DataFrame as the FDM relation for an edge" in {
    val edge = testEdge
    val fdm = fdmReference(Usage.Edge)
    val batch = relation(config(), testTyping, InstanceType.Edge)

    val fdmDf = dataFrame(fdm.row(edge), fdm.schema)
    val batchDf = dataFrame(batch.toRow(edge, Array.empty), batch.schema)

    batchDf.schema shouldBe fdmDf.schema
    batchDf.collect().toSeq shouldBe fdmDf.collect().toSeq

    val row = batchDf.collect().head
    row.getAs[Long]("edge.deletedTime") shouldBe 9L
    row.getAs[Row]("startNode").getString(1) shouldBe "start"
    row.getAs[Row]("endNode").getString(1) shouldBe "end"
    row.getAs[Row]("type").getString(1) shouldBe "edgeType"
  }
}
