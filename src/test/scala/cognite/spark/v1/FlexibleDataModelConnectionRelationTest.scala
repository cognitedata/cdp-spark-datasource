package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.{DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.EdgeWrite
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceCreate, SlimNodeOrEdge}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.DurationInt
import scala.util.{Success, Try}

class FlexibleDataModelConnectionRelationTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with FlexibleDataModelsTestBase {

  private val startEndNodeContainerExternalId = "sparkDsConnectionsTestContainerStartEndNodes1"
  private val startEndNodeViewExternalId = "sparkDsConnectionsTestViewStartEndNodes1"
  private val propsMap = Map(
    "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithoutDefaultValueNullable
  )
  private val connectionsViewExtId = "sparkDsTestConnectionsView"

  //  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  it should "fetch all connection instances" in {
    val edgeTypeExtId = s"edgeTypeExternalId${apiCompatibleRandomString()}"
    val startNodeExtId = s"${startEndNodeViewExternalId}FetchStartNode"
    val endNodeExtId = s"${startEndNodeViewExternalId}FetchEndNode"

    val results = (for {
      startEndNodeContainer <- createContainerIfNotExists(
        usage = Usage.Node,
        propsMap,
        startEndNodeContainerExternalId
      )
      startEndNodeView <- createViewWithCorePropsIfNotExists(
        startEndNodeContainer,
        startEndNodeViewExternalId,
        viewVersion)
      _ <- createNodesForEdgesIfNotExists(
        startNodeExtId,
        endNodeExtId,
        startEndNodeView.toSourceReference,
      )

      connectionsContainer <- createContainerIfNotExists(
        usage = Usage.Edge,
        propsMap,
        "connectionSourceContainer")
      connectionSourceView <- createViewWithCorePropsIfNotExists(
        connectionsContainer,
        "connectionSourceView",
        viewVersion)
      _ <- createViewWithConnectionsIfNotExists(
        connectionSourceView.toSourceReference,
        `type` = DirectRelationReference(spaceExternalId, externalId = edgeTypeExtId),
        connectionsViewExtId,
        viewVersion
      )
      connectionsWritten <- createConnectionWriteInstances(
        typeNode = DirectRelationReference(space = spaceExternalId, externalId = edgeTypeExtId),
        startNode = DirectRelationReference(space = spaceExternalId, externalId = startNodeExtId),
        endNode = DirectRelationReference(space = spaceExternalId, externalId = endNodeExtId)
      )
      _ <- IO.sleep(2.seconds)
    } yield connectionsWritten).unsafeRunSync()

    val readConnectionsDf = readRows(edgeSpace = spaceExternalId, edgeExternalId = edgeTypeExtId)

    readConnectionsDf.createTempView("connection_instances_table")

    val selectedConnectionInstances = spark
      .sql("select * from connection_instances_table")
      .collect()

    def toExternalIds(rows: Array[Row]): Array[String] =
      rows.map(row => row.getString(row.schema.fieldIndex("externalId")))

    val instExtIds = toExternalIds(selectedConnectionInstances)
    (instExtIds should contain).allElementsOf(results.map(_.externalId))
    instExtIds.length shouldBe 2
  }

  it should "insert connection instances" in {
    val edgeExternalId = s"edgeInstExtId${apiCompatibleRandomString()}"
    val startNodeExtId = s"${startEndNodeViewExternalId}InsertStartNode"
    val endNodeExtId = s"${startEndNodeViewExternalId}InsertEndNode"

    createNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      ViewReference(
        space = spaceExternalId,
        externalId = startEndNodeViewExternalId,
        version = viewVersion
      ),
    ).unsafeRunSync()

    def insertionDf(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
             |select
             |'$spaceExternalId' as space,
             |'$instanceExtId' as externalId,
             |named_struct(
             |    'space', '$spaceExternalId',
             |    'externalId', '$startNodeExtId'
             |) as startNode,
             |named_struct(
             |    'space', '$spaceExternalId',
             |    'externalId', '$endNodeExtId'
             |) as endNode
             |""".stripMargin)

    val insertionResult = Try {
      insertRows(
        edgeSpace = spaceExternalId,
        edgeExternalId = edgeExternalId,
        insertionDf(edgeExternalId)
      )
    }

    insertionResult shouldBe Success(())
    getUpsertedMetricsCount(spaceExternalId, edgeExternalId) shouldBe 1
  }

  private def createConnectionWriteInstances(
      typeNode: DirectRelationReference,
      startNode: DirectRelationReference,
      endNode: DirectRelationReference): IO[Seq[SlimNodeOrEdge]] = {
    val connectionInstExternalIdPrefix = typeNode.externalId
    val connectionInstances = Seq(
      EdgeWrite(
        `type` = typeNode,
        space = spaceExternalId,
        externalId = s"${connectionInstExternalIdPrefix}1",
        startNode = startNode,
        endNode = endNode,
        sources = None
      ),
      EdgeWrite(
        `type` = typeNode,
        space = spaceExternalId,
        externalId = s"${connectionInstExternalIdPrefix}2",
        startNode = startNode,
        endNode = endNode,
        sources = None
      )
    )
    client.instances.createItems(InstanceCreate(connectionInstances, replace = Some(true)))
  }

  private def getUpsertedMetricsCount(edgeSpace: String, edgeExternalId: String): Long =
    getNumberOfRowsUpserted(s"$edgeSpace-$edgeExternalId", FlexibleDataModelRelation.ResourceType)

  private def insertRows(
      edgeSpace: String,
      edgeExternalId: String,
      df: DataFrame,
      onConflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelation.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("edgeSpace", edgeSpace)
      .option("edgeExternalId", edgeExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$edgeSpace-$edgeExternalId")
      .save()

  private def readRows(edgeSpace: String, edgeExternalId: String): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelation.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("edgeSpace", edgeSpace)
      .option("edgeExternalId", edgeExternalId)
      .option("metricsPrefix", s"$edgeExternalId-$viewVersion")
      .option("collectMetrics", true)
      .load()
}
