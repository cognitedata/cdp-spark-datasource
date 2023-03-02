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

  it should "fetch connection instances with filters" in {
    val edgeTypeExtId = s"edgeTypeExternalId${apiCompatibleRandomString()}"
    val startNodeExtIdPrefix = s"${startEndNodeViewExternalId}FetchStartNode"
    val endNodeExtIdPrefix = s"${startEndNodeViewExternalId}FetchEndNode"

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
        s"${startNodeExtIdPrefix}1",
        s"${endNodeExtIdPrefix}1",
        startEndNodeView.toSourceReference,
      )
      _ <- createNodesForEdgesIfNotExists(
        s"${startNodeExtIdPrefix}2",
        s"${endNodeExtIdPrefix}2",
        startEndNodeView.toSourceReference,
      )

      connectionsSourceContainer <- createContainerIfNotExists(
        usage = Usage.Edge,
        propsMap,
        "connectionSourceContainer")
      connectionSourceView <- createViewWithCorePropsIfNotExists(
        connectionsSourceContainer,
        "connectionSourceView",
        viewVersion)
      _ <- createViewWithConnectionsIfNotExists(
        connectionSourceView.toSourceReference,
        `type` = DirectRelationReference(spaceExternalId, externalId = edgeTypeExtId),
        connectionsViewExtId,
        viewVersion
      )
      c1 <- createConnectionWriteInstances(
        externalId = "edge1",
        typeNode = DirectRelationReference(space = spaceExternalId, externalId = edgeTypeExtId),
        startNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"${startNodeExtIdPrefix}1"),
        endNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"${endNodeExtIdPrefix}1")
      )
      c2 <- createConnectionWriteInstances(
        externalId = "edge2",
        typeNode = DirectRelationReference(space = spaceExternalId, externalId = edgeTypeExtId),
        startNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"${startNodeExtIdPrefix}2"),
        endNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"${endNodeExtIdPrefix}2")
      )
      _ <- IO.sleep(5.seconds)
    } yield c1 ++ c2).unsafeRunSync()

    val readConnectionsDf = readRows(edgeSpace = spaceExternalId, edgeExternalId = edgeTypeExtId)

    readConnectionsDf.createTempView("connection_instances_table")

    val selectedConnectionInstances = spark
      .sql(s"""select * from connection_instances_table
           |where startNode = named_struct('space', '$spaceExternalId', 'externalId', '${startNodeExtIdPrefix}1')
           |""".stripMargin)
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
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = edgeExternalId,
        insertionDf(edgeExternalId)
      )
    }

    insertionResult shouldBe Success(())
    getUpsertedMetricsCount(spaceExternalId, edgeExternalId) shouldBe 1
  }

  private def createConnectionWriteInstances(
      externalId: String,
      typeNode: DirectRelationReference,
      startNode: DirectRelationReference,
      endNode: DirectRelationReference): IO[Seq[SlimNodeOrEdge]] = {
    val connectionInstances = Seq(
      EdgeWrite(
        `type` = typeNode,
        space = spaceExternalId,
        externalId = externalId,
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
      edgeTypeSpace: String,
      edgeTypeExternalId: String,
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
      .option("edgeSpace", edgeTypeSpace)
      .option("edgeExternalId", edgeTypeExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$edgeTypeSpace-$edgeTypeExternalId")
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
      .option("edgeTypeSpace", edgeSpace)
      .option("edgeTypeExternalId", edgeExternalId)
      .option("metricsPrefix", s"$edgeExternalId-$viewVersion")
      .option("collectMetrics", true)
      .load()
}
