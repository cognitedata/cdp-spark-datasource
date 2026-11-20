package cognite.spark.v1.fdm

import cats.effect.unsafe.implicits.global
import cognite.spark.v1.BatchApiTestSupport.FakeBatchApi
import cognite.spark.v1.fdm.utils.FDMContainerPropertyDefinitions._
import cognite.spark.v1.fdm.utils.FDMSparkDataframeTestOperations._
import cognite.spark.v1.fdm.utils.FDMTestConstants._
import cognite.spark.v1.fdm.utils.FDMTestInitializer
import cognite.spark.v1.fdm.utils.FDMTestMetricOperations._
import cognite.spark.v1._
import com.cognite.sdk.scala.common.OAuth2
import com.cognite.sdk.scala.v1.SpaceCreateDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.{DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModelCreate
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{ViewDefinition, ViewReference}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}

import java.util.UUID
import scala.concurrent.duration._
import scala.util.Try

/**
  * Live CDF comparison of [[BatchApiRelation]] (fed by [[BatchApiViaDm]]) against non-sync FDM
  * instance relations for nodes, edges, and data-model views.
  *
  * Requires the same TEST_* credentials as other FDM integration tests. This environment may not
  * have them; compile here and run separately:
  * {{{
  * sbt "testOnly cognite.spark.v1.fdm.BatchApiRelationFdmComparisonTest"
  * }}}
  */
class BatchApiRelationFdmComparisonTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with FDMTestInitializer {

  private val suffix = apiCompatibleRandomString()
  private val containerNodesExtId = s"sparkBatchCmpNodes_$suffix"
  private val containerEdgesExtId = s"sparkBatchCmpEdges_$suffix"
  private val viewNodesExtId = s"sparkBatchCmpViewNodes_$suffix"
  private val viewEdgesExtId = s"sparkBatchCmpViewEdges_$suffix"
  private val modelExtId = s"sparkBatchCmpModel_$suffix"

  private val containerProps: Map[String, ContainerPropertyDefinition] = Map(
    "name" -> TextPropertyNonListWithoutDefaultValueNullable,
    "count" -> Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable
  )

  // Space creation is idempotent enough for parallel suites that share the test space.
  locally {
    client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()
  }

  private lazy val nodeView: ViewDefinition = (for {
    container <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesExtId)
    view <- createViewWithCorePropsIfNotExists(container, viewNodesExtId, viewVersion)
  } yield view).unsafeRunSync()

  private lazy val edgeView: ViewDefinition = (for {
    container <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesExtId)
    view <- createViewWithCorePropsIfNotExists(container, viewEdgesExtId, viewVersion)
  } yield view).unsafeRunSync()

  private def oauthCredentials: OAuth2.ClientCredentials =
    OAuth2.ClientCredentials(
      tokenUri = sttp.model.Uri.unsafeParse(tokenUri),
      clientId = clientId,
      clientSecret = clientSecret,
      scopes = List(s"https://$cluster.cognitedata.com/.default"),
      audience = Some(audience),
      cdfProjectName = project
    )

  private def batchRelationConfig: RelationConfig =
    getDefaultConfig(
      auth = CdfSparkAuth.OAuth2ClientCredentials(oauthCredentials),
      projectName = project,
      cluster = cluster,
      applicationName = Some("BatchApiRelationFdmComparisonTest")
    )

  private def ensureNodes(viewDef: ViewDefinition, externalIds: Seq[String]): Unit = {
    val instanceSource = viewDef.toInstanceSource
    val sourceRef = viewDef.toSourceReference
    val existing = client.instances
      .retrieveByExternalIds(
        externalIds.map(id => InstanceRetrieve(InstanceType.Node, id, spaceExternalId)),
        sources = Some(Seq(instanceSource))
      )
      .unsafeRunSync()
      .items
      .map(_.externalId)
      .toSet

    val missing = externalIds.filterNot(existing.contains)
    if (missing.nonEmpty) {
      val writes = missing.map { id =>
        NodeWrite(
          space = spaceExternalId,
          externalId = id,
          sources = Some(
            Seq(
              EdgeOrNodeData(
                source = sourceRef,
                properties = Some(
                  Map(
                    "name" -> Some(InstancePropertyValue.String(s"name-$id")),
                    "count" -> Some(InstancePropertyValue.Int64(id.hashCode.toLong))
                  ))
              ))),
          `type` = None
        )
      }
      val _ = client.instances
        .createItems(InstanceCreate(items = writes))
        .unsafeRunSync()
    }
  }

  private def ensureEdges(
      viewDef: ViewDefinition,
      edgeIds: Seq[String],
      startNode: String,
      endNode: String): Unit = {
    ensureNodes(nodeView, Seq(startNode, endNode))
    val instanceSource = viewDef.toInstanceSource
    val sourceRef = viewDef.toSourceReference
    val existing = client.instances
      .retrieveByExternalIds(
        edgeIds.map(id => InstanceRetrieve(InstanceType.Edge, id, spaceExternalId)),
        sources = Some(Seq(instanceSource))
      )
      .unsafeRunSync()
      .items
      .map(_.externalId)
      .toSet

    val missing = edgeIds.filterNot(existing.contains)
    if (missing.nonEmpty) {
      val writes = missing.map { id =>
        EdgeWrite(
          `type` = DirectRelationReference(spaceExternalId, s"edgeType-$suffix"),
          space = spaceExternalId,
          externalId = id,
          startNode = DirectRelationReference(spaceExternalId, startNode),
          endNode = DirectRelationReference(spaceExternalId, endNode),
          sources = Some(
            Seq(
              EdgeOrNodeData(
                source = sourceRef,
                properties = Some(
                  Map(
                    "name" -> Some(InstancePropertyValue.String(s"edge-$id")),
                    "count" -> Some(InstancePropertyValue.Int64(7L))
                  ))
              )))
        )
      }
      val _ = client.instances
        .createItems(InstanceCreate(items = writes))
        .unsafeRunSync()
    }
  }

  private def readBatchViaDm(
      viewDef: ViewDefinition,
      instanceType: InstanceType): (StructType, Array[Row]) = {
    val instances = BatchApiViaDm
      .queryInstances(client, viewDef, instanceType)
      .unsafeRunSync()
    val typing = BatchApiViaDm.typingFromView(viewDef)
    val api = FakeBatchApi.fromInstances(instances, pageSize = 2)
    val relation = new BatchApiRelation(
      batchRelationConfig,
      statusId = UUID.randomUUID(),
      instanceType = instanceType,
      typing = typing,
      pollInterval = 1.millisecond
    )(spark.sqlContext)

    val decoded =
      relation
        .getStreams(BatchApiTestSupport.clientFor(api.backend, project))
        .head
        .compile
        .toList
        .unsafeRunSync()

    val rows = decoded.map(relation.toRow(_, Array.empty))
    val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), relation.schema)
    (relation.schema, orderedRows(df))
  }

  private def schemaByName(schema: StructType): Map[String, (DataType, Boolean)] =
    schema.fields.map(f => f.name -> ((f.dataType, f.nullable))).toMap

  private def orderedRows(df: DataFrame): Array[Row] = {
    // Backticks required: FDM metadata columns are flat names with dots (e.g. node.createdTime),
    // and Spark would otherwise parse them as nested struct field access.
    val cols =
      df.columns.sorted.map(c => org.apache.spark.sql.functions.col(s"`$c`")).toIndexedSeq
    df.select(cols: _*).orderBy("externalId").collect()
  }

  private def assertSameSchema(batch: StructType, fdm: StructType): Unit = {
    schemaByName(batch) shouldBe schemaByName(fdm)
  }

  it should "match non-sync FDM node schema and rows via batch-api-through-DM" in {
    val viewDef = nodeView
    val nodeIds = Seq(s"${viewDef.externalId}_n1", s"${viewDef.externalId}_n2")
    ensureNodes(viewDef, nodeIds)

    // Also exercise the Spark FDM write path so metrics/options match other FDM tests.
    val sparkInsertId = s"${viewDef.externalId}_n3"
    Try {
      insertNodeRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = viewDef.space,
        viewExternalId = viewDef.externalId,
        viewVersion = viewDef.version,
        instanceSpaceExternalId = spaceExternalId,
        df = spark.sql(
          s"""select '$sparkInsertId' as externalId, 'from-spark' as name, cast(3 as bigint) as count""")
      )
    }.get
    getUpsertedMetricsCount(viewDef) should be >= 1L

    val fdmDf = readRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = viewDef.space,
      viewExternalId = viewDef.externalId,
      viewVersion = viewDef.version,
      instanceSpaceExternalId = spaceExternalId
    ).filter(s"externalId like '${viewDef.externalId}_%'")

    val (batchSchema, batchRows) = readBatchViaDm(viewDef, InstanceType.Node)
    val fdmRows = orderedRows(fdmDf)

    assertSameSchema(batchSchema, fdmDf.schema)
    batchRows.map(_.getAs[String]("externalId")).toSeq.sorted shouldBe
      (nodeIds :+ sparkInsertId).sorted
    batchRows shouldBe fdmRows
  }

  it should "match non-sync FDM edge schema and rows via batch-api-through-DM" in {
    val viewDef = edgeView
    val start = s"${viewDef.externalId}_start"
    val end = s"${viewDef.externalId}_end"
    val edgeIds = Seq(s"${viewDef.externalId}_e1", s"${viewDef.externalId}_e2")
    ensureEdges(viewDef, edgeIds, start, end)

    val fdmDf = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = viewDef.space,
      viewExternalId = viewDef.externalId,
      viewVersion = viewDef.version,
      instanceSpaceExternalId = spaceExternalId
    ).filter(s"externalId like '${viewDef.externalId}_%'")

    val (batchSchema, batchRows) = readBatchViaDm(viewDef, InstanceType.Edge)
    val fdmRows = orderedRows(fdmDf)

    assertSameSchema(batchSchema, fdmDf.schema)
    batchRows.map(_.getAs[String]("externalId")).toSeq.sorted shouldBe edgeIds.sorted
    batchRows shouldBe fdmRows

    val row = batchRows.head
    row.getAs[Row]("startNode").getString(1) shouldBe start
    row.getAs[Row]("endNode").getString(1) shouldBe end
  }

  it should "match non-sync FDM data-model view reads via batch-api-through-DM" in {
    val viewDef = nodeView
    val nodeIds = Seq(s"${viewDef.externalId}_model_n1")
    ensureNodes(viewDef, nodeIds)

    val _ = client.dataModelsV3
      .createItems(
        Seq(
          DataModelCreate(
            space = spaceExternalId,
            externalId = modelExtId,
            name = Some("BatchApiCmpModel"),
            description = Some("Batch API vs FDM comparison model"),
            version = viewVersion,
            views = Some(Seq(ViewReference(spaceExternalId, viewDef.externalId, viewVersion)))
          )))
      .unsafeRunSync()

    val fdmDf = readRowsFromModel(
      modelSpace = spaceExternalId,
      modelExternalId = modelExtId,
      modelVersion = viewVersion,
      viewExternalId = viewDef.externalId,
      instanceSpace = Some(spaceExternalId)
    ).filter(s"externalId = '${nodeIds.head}'")

    val (batchSchema, batchRows) = {
      val (schema, rows) = readBatchViaDm(viewDef, InstanceType.Node)
      val filtered = rows.filter(_.getAs[String]("externalId") == nodeIds.head)
      (schema, filtered)
    }
    val fdmRows = orderedRows(fdmDf)

    assertSameSchema(batchSchema, fdmDf.schema)
    batchRows shouldBe fdmRows
    batchRows.head.getAs[String]("name") shouldBe s"name-${nodeIds.head}"
  }
}
