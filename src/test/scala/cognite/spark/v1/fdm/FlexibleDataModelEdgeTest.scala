package cognite.spark.v1.fdm

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cognite.spark.v1.SparkTest
import cognite.spark.v1.fdm.utils.FDMSparkDataframeTestOperations._
import cognite.spark.v1.fdm.utils.FDMTestConstants._
import cognite.spark.v1.fdm.utils.FDMTestMetricOperations.{
  getUpsertedMetricsCount,
  getUpsertedMetricsCountForModel
}
import cognite.spark.v1.fdm.utils.{FDMContainerPropertyDefinitions, FDMTestInitializer}
import com.cognite.sdk.scala.v1.SpaceCreateDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.EdgeConnection
import com.cognite.sdk.scala.v1.fdm.common.{DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModelCreate
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.EdgeWrite
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Success, Try}

class FlexibleDataModelEdgeTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with FDMTestInitializer {

  private val startEndNodeContainerExternalId = "sparkDsConnectionsTestContainerStartEndNodes1"
  private val startEndNodeViewExternalId = "sparkDsConnectionsTestViewStartEndNodes1"
  private val propsMap = Map(
    "stringProp1" -> FDMContainerPropertyDefinitions.TextPropertyNonListWithoutDefaultValueNullable
  )
  private val connectionsViewExtId = "sparkDsTestConnectionsView2"


  private val edgeTestDataModelExternalId = "testDataModelConnectionsExternalId1"
  private val edgeTypeExtId = s"sparkDsConnectionsEdgeTypeExternalId"

  private val duplicateViewExtId1 = s"sparkDsConnectionsDuplicatePropertyViewExternalId3"
  private val duplicateViewExtId2 = s"sparkDsConnectionsDuplicatePropertyViewExternalId4"

  private val duplicateEdgeTypeExtId1 = s"sparkDsConnectionsDuplicateEdgeTypeExternalId1"
  private val duplicateEdgeTypeExtId2 = s"sparkDsConnectionsDuplicateEdgeTypeExternalId2"
  private val duplicatePropertyName = "testDataModelDuplicatePropertyName"

  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  client.dataModelsV3
    .createItems(
      Seq(DataModelCreate(
        spaceExternalId,
        edgeTestDataModelExternalId,
        Some("SparkDatasourceConnectionsTestModel"),
        Some("Spark Datasource connections test model"),
        viewVersion,
        views = Some(
          Seq(
            ViewCreateDefinition(
              space=spaceExternalId,
              externalId=connectionsViewExtId,
              version=viewVersion,
              properties=Map(
                duplicatePropertyName -> ViewPropertyCreateDefinition.CreateConnectionDefinition(
                  EdgeConnection(
                    `type` = DirectRelationReference(spaceExternalId, externalId = edgeTypeExtId),
                    source = ViewReference(spaceExternalId, connectionsViewExtId, viewVersion),
                    name = None,
                    description = None,
                    direction = None,
                    connectionType = None,
                  ),
                ),
              ),
            ),
            ViewCreateDefinition(
              space=spaceExternalId,
              externalId=duplicateViewExtId1,
              version=viewVersion,
              properties=Map(
                duplicatePropertyName -> ViewPropertyCreateDefinition.CreateConnectionDefinition(
                  EdgeConnection(
                    `type` = DirectRelationReference(space = spaceExternalId, externalId = duplicateEdgeTypeExtId1),
                    source = ViewReference(spaceExternalId, connectionsViewExtId, viewVersion),
                    name = None,
                    description = None,
                    direction = None,
                    connectionType = None,
                  ),
                ),
              ),
            ),
            ViewCreateDefinition(
              space = spaceExternalId,
              externalId = duplicateViewExtId2,
              version = viewVersion,
              properties = Map(
                duplicatePropertyName -> ViewPropertyCreateDefinition.CreateConnectionDefinition(
                  EdgeConnection(
                    `type` = DirectRelationReference(space = spaceExternalId, externalId = duplicateEdgeTypeExtId2),
                    source = ViewReference(spaceExternalId, connectionsViewExtId, viewVersion),
                    name = None,
                    description = None,
                    direction = None,
                    connectionType = None,
                  ),
                ),
              ),
            ),
          )
        )
      )))
    .unsafeRunSync()



  it should "fetch edges with filters" in {
    val startNodeExtIdPrefix = s"${startEndNodeViewExternalId}FetchStartNode"
    val endNodeExtIdPrefix = s"${startEndNodeViewExternalId}FetchEndNode"

    val created = (for {
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
    created.size shouldBe 2
    (instExtIds should contain).allElementsOf(Array("edge1"))
  }

  it should "filter on edgeType when specifying edgeType in df" in {
    val created = (for {
      c1 <- createConnectionWriteInstances(
        externalId = "edge1",
        typeNode = DirectRelationReference(space = spaceExternalId, externalId = "edgeType"),
        startNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"start1"),
        endNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"end1"),
        autoCreateNodes = true
      )
      c2 <- createConnectionWriteInstances(
        externalId = "edge2",
        typeNode = DirectRelationReference(space = spaceExternalId, externalId = "wongEdgeType"),
        startNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"start2"),
        endNode =
          DirectRelationReference(space = spaceExternalId, externalId = s"end2"),
        autoCreateNodes = true
      )
    } yield c1 ++ c2).unsafeRunSync()

    val readConnectionsDf = readRows(edgeSpace = spaceExternalId, edgeExternalId = "edgeType")

    readConnectionsDf.createTempView("connection_instances_table")

    val selectedConnectionInstances = spark
      .sql(s"""select * from connection_instances_table""".stripMargin)
      .collect()

    val instExtIds = toExternalIds(selectedConnectionInstances)
    created.size shouldBe 2
    instExtIds.size shouldBe 1
    instExtIds should be(Seq("edge1"))
  }

  it should "fetch edges from a data model" in {
    val df = readRowsFromModel(
      spaceExternalId,
      edgeTestDataModelExternalId,
      viewVersion,
      spaceExternalId,
      edgeTypeExtId)

    df.createTempView("data_model_read_connections_table")

    val rows = spark
      .sql(s"""select * from data_model_read_connections_table
           | where startNode = named_struct(
           |    'space', '$spaceExternalId',
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode1'
           |) or startNode = named_struct(
           |    'space', '$spaceExternalId',
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode2'
           |)
           | """.stripMargin)
      .collect()

    rows.isEmpty shouldBe false
    (toExternalIds(rows) should contain).allElementsOf(Array("edge1"))
  }

  it should "insert edge to a data model" in {
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
        edgeTestDataModelExternalId,
        viewVersion,
        connectionsViewExtId,
        Some(spaceExternalId),
        df,
        connectionPropertyName = Some("connectionProp")
      )
    }

    result shouldBe Success(())
    getUpsertedMetricsCountForModel(edgeTestDataModelExternalId, viewVersion) shouldBe 1
  }

  it should "pick the right property as default for _type even though two views in the data models have same property names" in {

    val df = spark
      .sql(
        s"""
           |select
           |'edgeWithDuplicate1' as externalId,
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
        edgeTestDataModelExternalId,
        viewVersion,
        duplicateViewExtId1,
        Some(spaceExternalId),
        df,
        connectionPropertyName = Some(duplicatePropertyName)
      )
    }
    result shouldBe Success(())

    val df2 = spark
      .sql(
        s"""
           |select
           |'edgeWithDuplicate2' as externalId,
           |named_struct(
           |    'space', '$spaceExternalId',
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode1'
           |) as startNode,
           |named_struct(
           |    'externalId', '${startEndNodeViewExternalId}FetchStartNode2'
           |) as endNode
           |""".stripMargin)

    val result2 = Try {
      insertRowsToModel(
        spaceExternalId,
        edgeTestDataModelExternalId,
        viewVersion,
        duplicateViewExtId2,
        Some(spaceExternalId),
        df2,
        connectionPropertyName = Some(duplicatePropertyName)
      )
    }
    result2 shouldBe Success(())

    client.instances.retrieveByExternalIds(
      items = Seq(
        InstanceRetrieve(instanceType = InstanceType.Edge, externalId = "edgeWithDuplicate1", space = spaceExternalId),
        InstanceRetrieve(instanceType = InstanceType.Edge, externalId = "edgeWithDuplicate2", space = spaceExternalId),
      ),
      sources = None,
      includeTyping = true,
    ).unsafeRunSync().items.foreach {
      case e: InstanceDefinition.EdgeDefinition => e.externalId match {
        case "edgeWithDuplicate1" => e.`type`.externalId shouldBe duplicateEdgeTypeExtId1
        case "edgeWithDuplicate2" => e.`type`.externalId shouldBe duplicateEdgeTypeExtId2
      }
      case _ => ()
    }
  }

  it should "insert edges" in {
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
      insertEdgeRows(
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
      endNode: DirectRelationReference,
      autoCreateNodes: Boolean = false): IO[Seq[SlimNodeOrEdge]] = {
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
    client.instances.createItems(InstanceCreate(connectionInstances, replace = Some(true), autoCreateEndNodes = Some(autoCreateNodes), autoCreateStartNodes = Some(autoCreateNodes)))
  }

}
