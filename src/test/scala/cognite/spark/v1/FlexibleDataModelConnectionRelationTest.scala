package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModelCreate
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.EdgeWrite
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceCreate, SlimNodeOrEdge}
import com.cognite.sdk.scala.v1.fdm.views.{
  ConnectionDirection,
  ViewCreateDefinition,
  ViewDefinition,
  ViewPropertyCreateDefinition,
  ViewReference
}
import org.apache.spark.sql.DataFrame
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
  private val connectionsViewExtId = "sparkDsTestConnectionsView1"

  //  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  private val testDataModelExternalId = "testDataModelConnectionsExternalId1"
  private val edgeTypeExtId = s"sparkDsConnectionsEdgeTypeExternalId"

  client.dataModelsV3
    .createItems(
      Seq(DataModelCreate(
        spaceExternalId,
        testDataModelExternalId,
        Some("SparkDatasourceConnectionsTestModel"),
        Some("Spark Datasource connections test model"),
        viewVersion,
        views = Some(
          Seq(ViewReference(
            spaceExternalId,
            connectionsViewExtId,
            viewVersion
          )))
      )))
    .unsafeRunSync()

  it should "fetch connection instances with filters" in {
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
        "connectionSourceContainer1")
      connectionSourceView <- createViewWithCorePropsIfNotExists(
        connectionsSourceContainer,
        "connectionSourceView1",
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
           | where startNode = named_struct('space', '$spaceExternalId', 'externalId', '${startNodeExtIdPrefix}1')
           | and space = '$spaceExternalId'
           |""".stripMargin)
      .collect()

    val instExtIds = toExternalIds(selectedConnectionInstances)
    results.size shouldBe 2
    (instExtIds should contain).allElementsOf(Array("edge1"))
  }

  it should "fetch connection instances from a data model" in {
    val df = readRowsFromModel(
      spaceExternalId,
      testDataModelExternalId,
      viewVersion,
      spaceExternalId,
      edgeTypeExtId)

    df.createTempView("data_model_read_connections_table")

    val rows = spark
      .sql(s"""select * from data_model_read_connections_table
           | where startNode = named_struct(
           |    'space', '$spaceExternalId',
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode1'
           |)
           | """.stripMargin)
      .collect()

    rows.isEmpty shouldBe false
    (toExternalIds(rows) should contain).allElementsOf(Array("edge1"))
  }

  it should "insert connection instance to a data model" in {
    val df = spark
      .sql(s"""
           |select
           |'edgeThroughModel1' as externalId,
           |named_struct(
           |    'space', '$spaceExternalId',
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode1'
           |) as startNode,
           |named_struct(
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode2'
           |) as endNode
           |""".stripMargin)

    val result = Try {
      insertRowsToModel(
        spaceExternalId,
        testDataModelExternalId,
        viewVersion,
        connectionsViewExtId,
        "connectionProp",
        Some(spaceExternalId),
        df
      )
    }

    result shouldBe Success(())
    getUpsertedMetricsCountForModel(testDataModelExternalId, viewVersion) shouldBe 1
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
             |    'spaceExternalId', '$spaceExternalId',
             |    'externalId', '$startNodeExtId'
             |) as startNode,
             |named_struct(
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

  private def createViewWithConnectionsIfNotExists(
      connectionSource: ViewReference,
      `type`: DirectRelationReference,
      viewExternalId: String,
      viewVersion: String): IO[ViewDefinition] =
    client.views
      .retrieveItems(items = Seq(DataModelReference(spaceExternalId, viewExternalId, Some(viewVersion))))
      .flatMap { views =>
        if (views.isEmpty) {
          val viewToCreate = ViewCreateDefinition(
            space = spaceExternalId,
            externalId = viewExternalId,
            version = viewVersion,
            name = Some(s"Test-View-Connections-Spark-DS"),
            description = Some("Test View For Connections Spark Datasource"),
            filter = None,
            properties = Map(
              "connectionProp" -> ViewPropertyCreateDefinition.ConnectionDefinition(
                name = Some("connectionProp"),
                description = Some("connectionProp"),
                `type` = `type`,
                source = connectionSource,
                direction = Some(ConnectionDirection.Outwards)
              )
            ),
            implements = None,
          )

          client.views
            .createItems(items = Seq(viewToCreate))
            .flatTap(_ => IO.sleep(3.seconds))
        } else {
          IO.delay(views)
        }
      }
      .map(_.head)

  private def insertRows(
      edgeTypeSpace: String,
      edgeTypeExternalId: String,
      df: DataFrame,
      onConflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("edgeTypeSpace", edgeTypeSpace)
      .option("edgeTypeExternalId", edgeTypeExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$edgeTypeSpace-$edgeTypeExternalId")
      .save()

  private def readRows(edgeSpace: String, edgeExternalId: String): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
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

  private def readRowsFromModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      edgeTypeSpace: String,
      edgeTypeExternalId: String): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("modelSpace", modelSpace)
      .option("modelExternalId", modelExternalId)
      .option("modelVersion", modelVersion)
      .option("edgeTypeSpace", edgeTypeSpace)
      .option("edgeTypeExternalId", edgeTypeExternalId)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .option("collectMetrics", true)
      .load()

  private def insertRowsToModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
      connectionPropertyName: String,
      instanceSpace: Option[String],
      df: DataFrame,
      onConflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("modelSpace", modelSpace)
      .option("modelExternalId", modelExternalId)
      .option("modelVersion", modelVersion)
      .option("viewExternalId", viewExternalId)
      .option("connectionPropertyName", connectionPropertyName)
      .option("instanceSpace", instanceSpace.orNull)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .save()

  private def getUpsertedMetricsCount(edgeTypeSpace: String, edgeTypeExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$edgeTypeSpace-$edgeTypeExternalId",
      FlexibleDataModelRelationFactory.ResourceType)

  private def getUpsertedMetricsCountForModel(modelExternalId: String, modelVersion: String): Long =
    getNumberOfRowsUpserted(
      s"$modelExternalId-$modelVersion",
      FlexibleDataModelRelationFactory.ResourceType)
}
