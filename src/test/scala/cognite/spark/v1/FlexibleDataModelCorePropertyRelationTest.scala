package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.containers.{ContainerDefinition, ContainerId, ContainerReference}
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModelCreate
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import java.time.{LocalDate, ZonedDateTime}
import scala.concurrent.duration.DurationInt
import scala.util.{Success, Try}

class FlexibleDataModelCorePropertyRelationTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with FlexibleDataModelsTestBase {

  private val containerAllListAndNonListExternalId = "sparkDsTestContainerAllListAndNonList"
  private val containerNodesListAndNonListExternalId = "sparkDsTestContainerNodesListAndNonList"
  private val containerEdgesListAndNonListExternalId = "sparkDsTestContainerEdgesListAndNonList"

  private val containerAllNonListExternalId = "sparkDsTestContainerAllNonList"
  private val containerNodesNonListExternalId = "sparkDsTestContainerNodesNonList"
  private val containerEdgesNonListExternalId = "sparkDsTestContainerEdgesNonList"

  private val containerAllListExternalId = "sparkDsTestContainerAllList"
  private val containerNodesListExternalId = "sparkDsTestContainerNodesList"
  private val containerEdgesListExternalId = "sparkDsTestContainerEdgesList"

  private val viewAllListAndNonListExternalId = "sparkDsTestViewAllListAndNonList"
  private val viewNodesListAndNonListExternalId = "sparkDsTestViewNodesListAndNonList"
  private val viewEdgesListAndNonListExternalId = "sparkDsTestViewEdgesListAndNonList"

  private val viewAllNonListExternalId = "sparkDsTestViewAllNonList"
  private val viewNodesNonListExternalId = "sparkDsTestViewNodesNonList"
  private val viewEdgesNonListExternalId = "sparkDsTestViewEdgesNonList"

  private val viewAllListExternalId = "sparkDsTestViewAllList"
  private val viewNodesListExternalId = "sparkDsTestViewNodesList"
  private val viewEdgesListExternalId = "sparkDsTestViewEdgesList"

  private val containerAllNumericProps = "sparkDsTestContainerNumericProps"
  private val viewAllNumericProps = "sparkDsTestViewNumericProps"

  private val containerFilterByProps = "sparkDsTestContainerFilterByProps"
  private val viewFilterByProps = "sparkDsTestViewFilterByProps"

  private val containerStartNodeAndEndNodesExternalId = "sparkDsTestContainerStartAndEndNodes"
  private val viewStartNodeAndEndNodesExternalId = "sparkDsTestViewStartAndEndNodes"

  private val testDataModelExternalId = "sparkDsTestModel"

//  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  val nodeContainerProps: Map[String, ContainerPropertyDefinition] = Map(
    "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
    "stringProp2" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNullable,
  )

  private val containerStartAndEndNodes: ContainerDefinition =
    createContainerIfNotExists(Usage.Node, nodeContainerProps, containerStartNodeAndEndNodesExternalId)
      .unsafeRunSync()

  private val viewStartAndEndNodes: ViewDefinition =
    createViewWithCorePropsIfNotExists(
      containerStartAndEndNodes,
      viewStartNodeAndEndNodesExternalId,
      viewVersion)
      .unsafeRunSync()

  it should "succeed when inserting all nullable & non nullable non list values" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertNonListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertNonListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      viewStartAndEndNodes.toInstanceSource,
      viewStartAndEndNodes.toSourceReference,
    ).unsafeRunSync()

    val (viewAll, viewNodes, viewEdges) = setupAllNonListPropertyTest.unsafeRunSync()
    val randomId = generateNodeExternalId
    val instanceExtIdAll = s"${randomId}All"
    val instanceExtIdNode = s"${randomId}Node"
    val instanceExtIdEdge = s"${randomId}Edge"

    def insertionDf(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
                |select
                |'$instanceExtId' as externalId,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$instanceExtId'
                |) as type,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as startNode,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$endNodeExtId'
                |) as endNode,
                |'stringProp1' as stringProp1,
                |null as stringProp2,
                |1 as intProp1,
                |null as intProp2,
                |2 as longProp1,
                |null as longProp2,
                |3.1 as floatProp1,
                |null as floatProp2,
                |4.2 as doubleProp1,
                |null as doubleProp2,
                |true as boolProp1,
                |null as boolProp2,
                |'${LocalDate.now().format(InstancePropertyValue.Date.formatter)}' as dateProp1,
                |null as dateProp2,
                |'${ZonedDateTime
                  .now()
                  .format(InstancePropertyValue.Timestamp.formatter)}' as timestampProp1,
                |null as timestampProp2,
                |'{"a": "a", "b": 1}' as jsonProp1,
                |null as jsonProp2,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as directRelation1,
                |null as directRelation2
                |""".stripMargin)

    val insertionResults = Try {
      Vector(
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdAll)
        ),
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdNode)
        ),
        insertRows(
          instanceType = InstanceType.Edge,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewEdges.externalId,
          viewVersion = viewEdges.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdEdge)
        )
      )
    }

    insertionResults shouldBe Success(Vector((), (), ()))
    insertionResults.get.size shouldBe 3
    getUpsertedMetricsCount(viewAll) shouldBe 1
    getUpsertedMetricsCount(viewNodes) shouldBe 1
    getUpsertedMetricsCount(viewEdges) shouldBe 1

    def deletionDf(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
             |select
             |'$spaceExternalId' as space,
             |'$instanceExtId' as externalId
             |""".stripMargin)

    val deletionResults = Try {
      Vector(
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdAll),
          onConflict = "delete"
        ),
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdNode),
          onConflict = "delete"
        ),
        insertRows(
          instanceType = InstanceType.Edge,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewEdges.externalId,
          viewVersion = viewEdges.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdEdge),
          onConflict = "delete"
        )
      )
    }

    deletionResults shouldBe Success(Vector((), (), ()))
    deletionResults.get.size shouldBe 3
    getDeletedMetricsCount(viewAll) shouldBe 1
    getDeletedMetricsCount(viewNodes) shouldBe 1
    getDeletedMetricsCount(viewEdges) shouldBe 1
  }

  it should "succeed when inserting all nullable & non nullable list values" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      viewStartAndEndNodes.toInstanceSource,
      viewStartAndEndNodes.toSourceReference).unsafeRunSync()

    val (viewAll, viewNodes, viewEdges) = setupAllListPropertyTest.unsafeRunSync()
    val randomId = generateNodeExternalId
    val instanceExtIdAll = s"${randomId}All"
    val instanceExtIdNode = s"${randomId}Node"
    val instanceExtIdEdge = s"${randomId}Edge"

    def insertionDf(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
                |select
                |'$instanceExtId' as externalId,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$instanceExtId'
                |) as type,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as startNode,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$endNodeExtId'
                |) as endNode,
                |array('stringListProp1Val', null, 'stringListProp2Val', 24) as stringListProp1,
                |null as stringListProp2,
                |array(1, 2, 3) as intListProp1,
                |null as intListProp2,
                |array(101, null, 102, 103) as longListProp1,
                |null as longListProp2,
                |array(3.1, 3.2, 3.3, nvl(null, '3.4')) as floatListProp1,
                |null as floatListProp2,
                |array(null, 104.2, 104.3, 104.4) as doubleListProp1,
                |null as doubleListProp2,
                |array(true, true, false, null, false) as boolListProp1,
                |null as boolListProp2,
                |array('${LocalDate
                  .now()
                  .minusDays(5)
                  .format(InstancePropertyValue.Date.formatter)}', '${LocalDate
                  .now()
                  .minusDays(10)
                  .format(InstancePropertyValue.Date.formatter)}') as dateListProp1,
                |null as dateListProp2,
                |array('{"a": "a", "b": 1}', '{"a": "b", "b": 2}', '{"a": "c", "b": 3}') as jsonListProp1,
                |null as jsonListProp2,
                |array('${ZonedDateTime
                  .now()
                  .minusDays(5)
                  .format(InstancePropertyValue.Timestamp.formatter)}', '${ZonedDateTime
                  .now()
                  .minusDays(10)
                  .format(InstancePropertyValue.Timestamp.formatter)}') as timestampListProp1,
                |null as timestampListProp2
                |""".stripMargin)

    val insertionResult = Try {
      Vector(
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdAll)
        ),
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdNode)
        ),
        insertRows(
          instanceType = InstanceType.Edge,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewEdges.externalId,
          viewVersion = viewEdges.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdEdge)
        )
      )
    }

    insertionResult shouldBe Success(Vector((), (), ()))
    insertionResult.get.size shouldBe 3
    getUpsertedMetricsCount(viewAll) shouldBe 1
    getUpsertedMetricsCount(viewNodes) shouldBe 1
    getUpsertedMetricsCount(viewEdges) shouldBe 1

    def deletionDf(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
             |select
             |'$spaceExternalId' as space,
             |'$instanceExtId' as externalId
             |""".stripMargin)

    val deletionResults = Try {
      Vector(
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdAll),
          onConflict = "delete"
        ),
        insertRows(
          instanceType = InstanceType.Node,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdNode),
          onConflict = "delete"
        ),
        insertRows(
          instanceType = InstanceType.Edge,
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewEdges.externalId,
          viewVersion = viewEdges.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdEdge),
          onConflict = "delete"
        )
      )
    }

    deletionResults shouldBe Success(Vector((), (), ()))
    deletionResults.get.size shouldBe 3
    getDeletedMetricsCount(viewAll) shouldBe 1
    getDeletedMetricsCount(viewNodes) shouldBe 1
    getDeletedMetricsCount(viewEdges) shouldBe 1
  }

  it should "succeed when fetching instances with select *" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertAllStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertAllEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      viewStartAndEndNodes.toInstanceSource,
      viewStartAndEndNodes.toSourceReference).unsafeRunSync()

    val typeNodeRef = DirectRelationReference(
      space = spaceExternalId,
      externalId = "insertAllTypeNodeExtId"
    )
    val startNodeRef = DirectRelationReference(
      space = spaceExternalId,
      externalId = startNodeExtId
    )
    val endNodeRef = DirectRelationReference(
      space = spaceExternalId,
      externalId = endNodeExtId
    )
    val directNodeReference = DirectRelationReference(
      space = spaceExternalId,
      externalId = startNodeExtId
    )
    // DirectRelationReference(space = spaceExternalId, externalId = s"${edgeExternalIdPrefix}Type1"),

    val (viewAll, viewNodes, viewEdges, allInstanceExternalIds) = (for {
      (viewAll, viewNodes, viewEdges) <- setupAllListAndNonListPropertyTest
      nodeIds <- createTestInstancesForView(
        viewNodes,
        directNodeReference = directNodeReference,
        None,
        None,
        None)
      edgeIds <- createTestInstancesForView(
        viewEdges,
        directNodeReference,
        Some(typeNodeRef),
        Some(startNodeRef),
        Some(endNodeRef))
      allIds <- createTestInstancesForView(
        viewAll,
        directNodeReference,
        Some(typeNodeRef),
        Some(startNodeRef),
        Some(endNodeRef))
    } yield (viewAll, viewNodes, viewEdges, nodeIds ++ edgeIds ++ allIds)).unsafeRunSync()

    val readNodesDf = readRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewNodes.externalId,
      viewVersion = viewNodes.version,
      instanceSpaceExternalId = spaceExternalId
    )

    val readEdgesDf = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      instanceSpaceExternalId = spaceExternalId
    )

    val readAllDf = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewAll.externalId,
      viewVersion = viewAll.version,
      instanceSpaceExternalId = spaceExternalId
    ).unionAll(
      readRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = viewAll.externalId,
        viewVersion = viewAll.version,
        instanceSpaceExternalId = spaceExternalId
      ))

    readNodesDf.createTempView(s"node_instances_table")
    readEdgesDf.createTempView(s"edge_instances_table")
    readAllDf.createTempView(s"all_instances_table")

    val selectedNodes = spark
      .sql("select * from node_instances_table")
      .collect()

    val selectedEdges = spark
      .sql("select * from edge_instances_table")
      .collect()

    val selectedNodesAndEdges = spark
      .sql("select * from all_instances_table")
      .collect()

    val actualAllInstanceExternalIds = toExternalIds(selectedNodesAndEdges) ++ toExternalIds(
      selectedNodes) ++ toExternalIds(selectedEdges)

    allInstanceExternalIds.length shouldBe 8
    (actualAllInstanceExternalIds should contain).allElementsOf(allInstanceExternalIds)
  }

  it should "succeed when filtering edges with type, startNode & endNode" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}FilterByEdgePropsStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}FilterByEdgePropsEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      viewStartAndEndNodes.toInstanceSource,
      viewStartAndEndNodes.toSourceReference).unsafeRunSync()

    val typeNodeRef = DirectRelationReference(
      space = spaceExternalId,
      externalId = s"filterByEdgePropsTypeNodeExtId${apiCompatibleRandomString()}"
    )
    val startNodeRef = DirectRelationReference(
      space = spaceExternalId,
      externalId = startNodeExtId
    )
    val endNodeRef = DirectRelationReference(
      space = spaceExternalId,
      externalId = endNodeExtId
    )
    val directNodeReference = DirectRelationReference(
      space = spaceExternalId,
      externalId = startNodeExtId
    )

    val (viewEdges, allEdgeExternalIds) = (for {
      (_, _, viewEdges) <- setupAllListAndNonListPropertyTest
      edgeIds <- createTestInstancesForView(
        viewEdges,
        directNodeReference,
        Some(typeNodeRef),
        Some(startNodeRef),
        Some(endNodeRef))
    } yield (viewEdges, edgeIds)).unsafeRunSync()

    val readEdgesDf = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      instanceSpaceExternalId = spaceExternalId
    )

    readEdgesDf.createTempView(s"edge_filter_instances_table")

    val selectedEdges = spark
      .sql(s"""select * from edge_filter_instances_table
           | where startNode = named_struct('space', '${startNodeRef.space}', 'externalId', '${startNodeRef.externalId}')
           | and endNode = named_struct('space', '${endNodeRef.space}', 'externalId', '${endNodeRef.externalId}')
           | and type = named_struct('space', '${typeNodeRef.space}', 'externalId', '${typeNodeRef.externalId}')
           | and directRelation1 = named_struct('space', '${directNodeReference.space}', 'externalId', '${directNodeReference.externalId}')
           | and space = '$spaceExternalId'
           | """.stripMargin)
      .collect()

    val actualAllEdgeExternalIds = toExternalIds(selectedEdges)

    allEdgeExternalIds.length shouldBe 2
    (actualAllEdgeExternalIds should contain).allElementsOf(allEdgeExternalIds)
  }

  it should "succeed when filtering instances by properties" in {
    val (view, instanceExtIds) = setupFilteringByPropertiesTest.unsafeRunSync()

    val readDf = readRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      instanceSpaceExternalId = spaceExternalId
    )

    readDf.createTempView(s"instance_filter_table")

    val sql = s"""
                 |select * from instance_filter_table
                 |where
                 |forEqualsFilter = 'str1' and
                 |forInFilter in ('str1', 'str2', 'str3') and
                 |forGteFilter >= 1 and
                 |forGtFilter > 1 and
                 |forLteFilter <= 2 and
                 |forLtFilter < 4 and
                 |(forOrFilter1 == 5.1 or forOrFilter2 == 6.1) and
                 |forIsNotNullFilter is not null and
                 |forIsNullFilter is null
                 |""".stripMargin

    val filtered = spark
      .sql(sql)
      .collect()

    filtered.length shouldBe 1
    val filteredInstanceExtIds =
      filtered.map(row => row.getString(row.schema.fieldIndex("externalId"))).toVector
    instanceExtIds.containsSlice(filteredInstanceExtIds) shouldBe true
    filteredInstanceExtIds shouldBe Vector(s"${view.externalId}Node1")
  }

  it should "successfully cast numeric properties" in {
    val viewDef = setupNumericConversionTest.unsafeRunSync()
    val nodeExtId1 = s"${viewDef.externalId}Numeric1"
    val nodeExtId2 = s"${viewDef.externalId}Numeric2"

    val df = spark
      .sql(s"""
              |select
              |'$nodeExtId1' as externalId,
              |1 as intProp,
              |2 as longProp,
              |3.2 as floatProp,
              |4.3 as doubleProp,
              |'$nodeExtId1' as stringProp
              |
              |union all
              |
              |select
              |'$nodeExtId2' as externalId,
              |null as intProp,
              |null as longProp,
              |null as floatProp,
              |null as doubleProp,
              |'$nodeExtId2' as stringProp
              |""".stripMargin)

    val result = Try {
      insertRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = viewDef.space,
        viewExternalId = viewDef.externalId,
        viewVersion = viewDef.version,
        instanceSpaceExternalId = viewDef.space,
        df
      )
    }

    result shouldBe Success(())

    val propertyMapForInstances = (IO.sleep(2.seconds) *> Vector(
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = nodeExtId1,
        space = spaceExternalId,
        sources = Some(Seq(InstanceSource(viewDef.toSourceReference)))
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = nodeExtId2,
        space = spaceExternalId,
        sources = Some(Seq(InstanceSource(viewDef.toSourceReference)))
      )
    ).traverse(i => client.instances.retrieveByExternalIds(Vector(i), includeTyping = true))
      .map { instances =>
        instances.flatMap(_.items).collect {
          case n: InstanceDefinition.NodeDefinition =>
            n.externalId -> n.properties
              .getOrElse(Map.empty)
              .values
              .flatMap(_.values)
              .fold(Map.empty)(_ ++ _)
        }
      }
      .map(_.toMap))
      .unsafeRunSync()

    propertyMapForInstances(nodeExtId1)("intProp") shouldBe InstancePropertyValue.Int32(1)
    propertyMapForInstances(nodeExtId1)("longProp") shouldBe InstancePropertyValue.Int64(2)
    propertyMapForInstances(nodeExtId1)("floatProp") shouldBe InstancePropertyValue.Float32(3.2F)
    propertyMapForInstances(nodeExtId1)("doubleProp") shouldBe InstancePropertyValue.Float64(4.3)

    propertyMapForInstances(nodeExtId2).get("intProp") shouldBe None
    propertyMapForInstances(nodeExtId2).get("longProp") shouldBe None
    propertyMapForInstances(nodeExtId2).get("floatProp") shouldBe None
    propertyMapForInstances(nodeExtId2).get("doubleProp") shouldBe None
  }

  it should "successfully filter instances from a data model" in {
    setUpDataModel()
    val df = readRowsFromModel(
      modelSpace = spaceExternalId,
      modelExternalId = testDataModelExternalId,
      modelVersion = viewVersion,
      viewExternalId = viewStartNodeAndEndNodesExternalId,
      instanceSpace = None
    )

    df.createTempView("data_model_read_table")

    val rows = spark
      .sql(s"""select * from data_model_read_table
           | where externalId = '${viewStartNodeAndEndNodesExternalId}InsertNonListStartNode'
           | """.stripMargin)
      .collect()

    rows.isEmpty shouldBe false
    toExternalIds(rows).toVector shouldBe Vector("sparkDsTestViewStartAndEndNodesInsertNonListStartNode")
    toPropVal(rows, "stringProp1").toVector shouldBe Vector("stringProp1Val")
    toPropVal(rows, "stringProp2").toVector shouldBe Vector("stringProp2Val")
  }

  it should "successfully insert instances to a data model" in {
    setUpDataModel()
    val df = spark
      .sql(s"""
           |select
           |'insertedThroughModel' as externalId,
           |'throughModelProp1' as stringProp1,
           |'throughModelProp2' as stringProp2
           |""".stripMargin)

    val result = Try {
      insertRowsToModel(
        modelSpace = spaceExternalId,
        modelExternalId = testDataModelExternalId,
        modelVersion = viewVersion,
        viewExternalId = viewStartNodeAndEndNodesExternalId,
        instanceSpace = Some(spaceExternalId),
        df
      )
    }

    result shouldBe Success(())
    getUpsertedMetricsCountForModel(testDataModelExternalId, viewVersion) shouldBe 1
  }

  // This should be kept as ignored
  ignore should "delete containers and views used for testing" in {
    client.containers
      .delete(Seq(
        ContainerId(spaceExternalId, containerAllListAndNonListExternalId),
        ContainerId(spaceExternalId, containerNodesListAndNonListExternalId),
        ContainerId(spaceExternalId, containerEdgesListAndNonListExternalId),
        //
        ContainerId(spaceExternalId, containerAllNonListExternalId),
        ContainerId(spaceExternalId, containerNodesNonListExternalId),
        ContainerId(spaceExternalId, containerEdgesNonListExternalId),
        //
        ContainerId(spaceExternalId, containerAllListExternalId),
        ContainerId(spaceExternalId, containerNodesListExternalId),
        ContainerId(spaceExternalId, containerEdgesListExternalId),
        //
        ContainerId(spaceExternalId, containerAllNumericProps),
        ContainerId(spaceExternalId, containerFilterByProps),
        ContainerId(spaceExternalId, containerStartNodeAndEndNodesExternalId),
      ))
      .unsafeRunSync()

    client.views
      .deleteItems(Seq(
        DataModelReference(spaceExternalId, viewAllListAndNonListExternalId, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewNodesListAndNonListExternalId, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewEdgesListAndNonListExternalId, Some(viewVersion)),
        //
        DataModelReference(spaceExternalId, viewAllNonListExternalId, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewNodesNonListExternalId, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewEdgesNonListExternalId, Some(viewVersion)),
        //
        DataModelReference(spaceExternalId, viewAllListExternalId, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewNodesListExternalId, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewEdgesListExternalId, Some(viewVersion)),
        //
        DataModelReference(spaceExternalId, viewAllNumericProps, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewFilterByProps, Some(viewVersion)),
        DataModelReference(spaceExternalId, viewStartNodeAndEndNodesExternalId, Some(viewVersion)),
      ))
      .unsafeRunSync()

    succeed
  }

  // scalastyle:off method.length
  private def setupAllListAndNonListPropertyTest
    : IO[(ViewDefinition, ViewDefinition, ViewDefinition)] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
      "stringProp2" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNullable,
      "intProp1" -> FDMContainerPropertyTypes.Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable,
      "intProp2" -> FDMContainerPropertyTypes.Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "longProp1" -> FDMContainerPropertyTypes.Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable,
      "longProp2" -> FDMContainerPropertyTypes.Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "floatProp1" -> FDMContainerPropertyTypes.Float32NonListWithoutDefaultValueNonNullable,
      "floatProp2" -> FDMContainerPropertyTypes.Float32NonListWithoutDefaultValueNullable,
      "doubleProp1" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNonNullable,
      "doubleProp2" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNullable,
      "boolProp1" -> FDMContainerPropertyTypes.BooleanNonListWithDefaultValueNonNullable,
      "boolProp2" -> FDMContainerPropertyTypes.BooleanNonListWithDefaultValueNullable,
      "dateProp1" -> FDMContainerPropertyTypes.DateNonListWithDefaultValueNonNullable,
      "dateProp2" -> FDMContainerPropertyTypes.DateNonListWithDefaultValueNullable,
      "timestampProp1" -> FDMContainerPropertyTypes.TimestampNonListWithDefaultValueNonNullable,
      "timestampProp2" -> FDMContainerPropertyTypes.TimestampNonListWithDefaultValueNullable,
      "jsonProp1" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNonNullable,
      "jsonProp2" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNullable,
      "stringListProp1" -> FDMContainerPropertyTypes.TextPropertyListWithoutDefaultValueNonNullable,
      "stringListProp2" -> FDMContainerPropertyTypes.TextPropertyListWithoutDefaultValueNullable,
      "intListProp1" -> FDMContainerPropertyTypes.Int32ListWithoutDefaultValueNonNullable,
      "intListProp2" -> FDMContainerPropertyTypes.Int32ListWithoutDefaultValueNullable,
      "longListProp1" -> FDMContainerPropertyTypes.Int64ListWithoutDefaultValueNonNullable,
      "longListProp2" -> FDMContainerPropertyTypes.Int64ListWithoutDefaultValueNullable,
      "floatListProp1" -> FDMContainerPropertyTypes.Float32ListWithoutDefaultValueNonNullable,
      "floatListProp2" -> FDMContainerPropertyTypes.Float32ListWithoutDefaultValueNullable,
      "doubleListProp1" -> FDMContainerPropertyTypes.Float64ListWithoutDefaultValueNonNullable,
      "doubleListProp2" -> FDMContainerPropertyTypes.Float64ListWithoutDefaultValueNullable,
      "boolListProp1" -> FDMContainerPropertyTypes.BooleanListWithoutDefaultValueNonNullable,
      "boolListProp2" -> FDMContainerPropertyTypes.BooleanListWithoutDefaultValueNullable,
      "dateListProp1" -> FDMContainerPropertyTypes.DateListWithoutDefaultValueNonNullable,
      "dateListProp2" -> FDMContainerPropertyTypes.DateListWithoutDefaultValueNullable,
      "timestampListProp1" -> FDMContainerPropertyTypes.TimestampListWithoutDefaultValueNonNullable,
      "timestampListProp2" -> FDMContainerPropertyTypes.TimestampListWithoutDefaultValueNullable,
      "jsonListProp1" -> FDMContainerPropertyTypes.JsonListWithoutDefaultValueNonNullable,
      "jsonListProp2" -> FDMContainerPropertyTypes.JsonListWithoutDefaultValueNullable,
      "directRelation1" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable
        .copy(
          `type` = DirectNodeRelationProperty(
            container = Some(
              ContainerReference(
                space = spaceExternalId,
                externalId = containerStartNodeAndEndNodesExternalId)),
            source = None)),
      "directRelation2" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllListAndNonListExternalId)
      cNodes <- createContainerIfNotExists(
        Usage.Node,
        containerProps,
        containerNodesListAndNonListExternalId)
      cEdges <- createContainerIfNotExists(
        Usage.Edge,
        containerProps,
        containerEdgesListAndNonListExternalId)
      viewAll <- createViewWithCorePropsIfNotExists(cAll, viewAllListAndNonListExternalId, viewVersion)
      viewNodes <- createViewWithCorePropsIfNotExists(
        cNodes,
        viewNodesListAndNonListExternalId,
        viewVersion)
      viewEdges <- createViewWithCorePropsIfNotExists(
        cEdges,
        viewEdgesListAndNonListExternalId,
        viewVersion)
      _ <- IO.sleep(5.seconds)
    } yield (viewAll, viewNodes, viewEdges)
  }
  // scalastyle:on method.length

  private def setupAllNonListPropertyTest: IO[(ViewDefinition, ViewDefinition, ViewDefinition)] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
      "stringProp2" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNullable,
      "intProp1" -> FDMContainerPropertyTypes.Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable,
      "intProp2" -> FDMContainerPropertyTypes.Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "longProp1" -> FDMContainerPropertyTypes.Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable,
      "longProp2" -> FDMContainerPropertyTypes.Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "floatProp1" -> FDMContainerPropertyTypes.Float32NonListWithoutDefaultValueNonNullable,
      "floatProp2" -> FDMContainerPropertyTypes.Float32NonListWithoutDefaultValueNullable,
      "doubleProp1" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNonNullable,
      "doubleProp2" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNullable,
      "boolProp1" -> FDMContainerPropertyTypes.BooleanNonListWithDefaultValueNonNullable,
      "boolProp2" -> FDMContainerPropertyTypes.BooleanNonListWithDefaultValueNullable,
      "dateProp1" -> FDMContainerPropertyTypes.DateNonListWithDefaultValueNonNullable,
      "dateProp2" -> FDMContainerPropertyTypes.DateNonListWithDefaultValueNullable,
      "timestampProp1" -> FDMContainerPropertyTypes.TimestampNonListWithDefaultValueNonNullable,
      "timestampProp2" -> FDMContainerPropertyTypes.TimestampNonListWithDefaultValueNullable,
      "jsonProp1" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNonNullable,
      "jsonProp2" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNullable,
      "directRelation1" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable
        .copy(
          `type` = DirectNodeRelationProperty(
            container = Some(
              ContainerReference(
                space = spaceExternalId,
                externalId = containerStartNodeAndEndNodesExternalId)),
            source = None)),
      "directRelation2" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable,
//      "file" -> FDMContainerPropertyTypes.FileReference,
      "timeseries" -> FDMContainerPropertyTypes.TimeSeriesReference
//      "sequence" -> FDMContainerPropertyTypes.SequenceReference,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllNonListExternalId)
      cNodes <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesNonListExternalId)
      cEdges <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesNonListExternalId)
      viewAll <- createViewWithCorePropsIfNotExists(cAll, viewAllNonListExternalId, viewVersion)
      viewNodes <- createViewWithCorePropsIfNotExists(cNodes, viewNodesNonListExternalId, viewVersion)
      viewEdges <- createViewWithCorePropsIfNotExists(cEdges, viewEdgesNonListExternalId, viewVersion)
      _ <- IO.sleep(5.seconds)
    } yield (viewAll, viewNodes, viewEdges)
  }

  private def setupAllListPropertyTest: IO[(ViewDefinition, ViewDefinition, ViewDefinition)] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringListProp1" -> FDMContainerPropertyTypes.TextPropertyListWithoutDefaultValueNonNullable,
      "stringListProp2" -> FDMContainerPropertyTypes.TextPropertyListWithoutDefaultValueNullable,
      "intListProp1" -> FDMContainerPropertyTypes.Int32ListWithoutDefaultValueNonNullable,
      "intListProp2" -> FDMContainerPropertyTypes.Int32ListWithoutDefaultValueNullable,
      "longListProp1" -> FDMContainerPropertyTypes.Int64ListWithoutDefaultValueNonNullable,
      "longListProp2" -> FDMContainerPropertyTypes.Int64ListWithoutDefaultValueNullable,
      "floatListProp1" -> FDMContainerPropertyTypes.Float32ListWithoutDefaultValueNonNullable,
      "floatListProp2" -> FDMContainerPropertyTypes.Float32ListWithoutDefaultValueNullable,
      "doubleListProp1" -> FDMContainerPropertyTypes.Float64ListWithoutDefaultValueNonNullable,
      "doubleListProp2" -> FDMContainerPropertyTypes.Float64ListWithoutDefaultValueNullable,
      "boolListProp1" -> FDMContainerPropertyTypes.BooleanListWithoutDefaultValueNonNullable,
      "boolListProp2" -> FDMContainerPropertyTypes.BooleanListWithoutDefaultValueNullable,
      "dateListProp1" -> FDMContainerPropertyTypes.DateListWithoutDefaultValueNonNullable,
      "dateListProp2" -> FDMContainerPropertyTypes.DateListWithoutDefaultValueNullable,
      "timestampListProp1" -> FDMContainerPropertyTypes.TimestampListWithoutDefaultValueNonNullable,
      "timestampListProp2" -> FDMContainerPropertyTypes.TimestampListWithoutDefaultValueNullable,
      "jsonListProp1" -> FDMContainerPropertyTypes.JsonListWithoutDefaultValueNonNullable,
      "jsonListProp2" -> FDMContainerPropertyTypes.JsonListWithoutDefaultValueNullable,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllListExternalId)
      cNodes <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesListExternalId)
      cEdges <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesListExternalId)
      viewAll <- createViewWithCorePropsIfNotExists(cAll, viewAllListExternalId, viewVersion)
      viewNodes <- createViewWithCorePropsIfNotExists(cNodes, viewNodesListExternalId, viewVersion)
      viewEdges <- createViewWithCorePropsIfNotExists(cEdges, viewEdgesListExternalId, viewVersion)
    } yield (viewAll, viewNodes, viewEdges)
  }

  private def setupFilteringByPropertiesTest: IO[(ViewDefinition, Seq[String])] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "forEqualsFilter" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
      "forInFilter" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNullable,
      "forGteFilter" -> FDMContainerPropertyTypes.Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable,
      "forGtFilter" -> FDMContainerPropertyTypes.Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "forLteFilter" -> FDMContainerPropertyTypes.Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable,
      "forLtFilter" -> FDMContainerPropertyTypes.Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "forOrFilter1" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNonNullable,
      "forOrFilter2" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNullable,
      "boolProp1" -> FDMContainerPropertyTypes.BooleanNonListWithDefaultValueNonNullable,
      "boolProp2" -> FDMContainerPropertyTypes.BooleanNonListWithDefaultValueNullable,
      "dateProp1" -> FDMContainerPropertyTypes.DateNonListWithDefaultValueNonNullable,
      "forIsNotNullFilter" -> FDMContainerPropertyTypes.DateNonListWithDefaultValueNullable,
      "forIsNullFilter" -> FDMContainerPropertyTypes.JsonNonListWithoutDefaultValueNullable,
      "forTimeseriesRef" -> FDMContainerPropertyTypes.TimeSeriesReference
//      "forFileRef" -> FDMContainerPropertyTypes.FileReference,
//      "forSequenceRef" -> FDMContainerPropertyTypes.SequenceReference,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerFilterByProps)
      viewAll <- createViewWithCorePropsIfNotExists(cAll, viewFilterByProps, viewVersion)
      extIds <- setupInstancesForFiltering(viewAll)
    } yield (viewAll, extIds)
  }

  private def setUpDataModel() =
    client.dataModelsV3
      .createItems(
        Seq(DataModelCreate(
          spaceExternalId,
          testDataModelExternalId,
          Some("SparkDatasourceTestModel"),
          Some("Spark Datasource test model"),
          viewVersion,
          views = Some(
            Seq(ViewReference(
              spaceExternalId,
              viewStartNodeAndEndNodesExternalId,
              viewVersion
            )))
        )))
      .unsafeRunSync()
  // scalastyle:off method.length
  private def setupInstancesForFiltering(viewDef: ViewDefinition): IO[Seq[String]] = {
    val viewExtId = viewDef.externalId
    val source = viewDef.toInstanceSource
    val viewRef = viewDef.toSourceReference
    val instanceRetrieves = Vector(
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = s"${viewExtId}Node1",
        space = spaceExternalId,
        sources = Some(Seq(source))
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = s"${viewExtId}Node2",
        space = spaceExternalId,
        sources = Some(Seq(source))
      )
    )
    client.instances.retrieveByExternalIds(instanceRetrieves).map(_.items).flatMap { instances =>
      val nodes = instances.collect { case n: InstanceDefinition.NodeDefinition => n.externalId }
      if (nodes.length === 2) {
        IO.pure(nodes)
      } else {
        client.instances
          .createItems(
            instance = InstanceCreate(
              items = Seq(
                NodeWrite(
                  spaceExternalId,
                  s"${viewDef.externalId}Node1",
                  Some(Seq(EdgeOrNodeData(
                    viewRef,
                    Some(Map(
                      "forEqualsFilter" -> InstancePropertyValue.String("str1"),
                      "forInFilter" -> InstancePropertyValue.String("str1"),
                      "forGteFilter" -> InstancePropertyValue.Int32(1),
                      "forGtFilter" -> InstancePropertyValue.Int32(2),
                      "forLteFilter" -> InstancePropertyValue.Int64(2),
                      "forLtFilter" -> InstancePropertyValue.Int64(3),
                      "forOrFilter1" -> InstancePropertyValue.Float64(5.1),
                      "forOrFilter2" -> InstancePropertyValue.Float64(6.1),
                      "forIsNotNullFilter" -> InstancePropertyValue.Date(LocalDate.now())
                    ))
                  )))
                ),
                NodeWrite(
                  spaceExternalId,
                  s"${viewDef.externalId}Node2",
                  Some(Seq(EdgeOrNodeData(
                    viewRef,
                    Some(Map(
                      "forEqualsFilter" -> InstancePropertyValue.String("str2"),
                      "forInFilter" -> InstancePropertyValue.String("str2"),
                      "forGteFilter" -> InstancePropertyValue.Int32(5),
                      "forGtFilter" -> InstancePropertyValue.Int32(2),
                      "forLteFilter" -> InstancePropertyValue.Int64(1),
                      "forLtFilter" -> InstancePropertyValue.Int64(-1),
                      "forOrFilter1" -> InstancePropertyValue.Float64(5.1),
                      "forOrFilter2" -> InstancePropertyValue.Float64(6.1),
                      "forIsNotNullFilter" -> InstancePropertyValue.Date(LocalDate.now()),
                      "forIsNullFilter" -> InstancePropertyValue.Object(Json.fromJsonObject(
                        JsonObject("a" -> Json.fromString("a"), "b" -> Json.fromInt(1))))
                    ))
                  )))
                )
              ),
              replace = Some(true)
            )
          )
          .map(_.collect { case n: SlimNodeOrEdge.SlimNodeDefinition => n.externalId })
          .map(_.distinct)
      }
    }
  }
  // scalastyle:on method.length

  private def setupNumericConversionTest: IO[ViewDefinition] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringProp" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
      "intProp" -> FDMContainerPropertyTypes.Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "longProp" -> FDMContainerPropertyTypes.Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "floatProp" -> FDMContainerPropertyTypes.Float32NonListWithoutDefaultValueNullable,
      "doubleProp" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNullable,
    )

    for {
      container <- createContainerIfNotExists(Usage.All, containerProps, containerAllNumericProps)
      view <- createViewWithCorePropsIfNotExists(container, viewAllNumericProps, viewVersion)
    } yield view
  }

  private def insertRows(
      instanceType: InstanceType,
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: String,
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
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpace", instanceSpaceExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .save()

  private def readRows(
      instanceType: InstanceType,
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: String): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpace", instanceSpaceExternalId)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .option("collectMetrics", true)
      .load()

  private def readRowsFromModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
      instanceSpace: Option[String]): DataFrame =
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
      .option("instanceSpace", instanceSpace.orNull)
      .option("viewExternalId", viewExternalId)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .option("collectMetrics", true)
      .load()

  private def insertRowsToModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
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
      .option("instanceSpace", instanceSpace.orNull)
      .option("viewExternalId", viewExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .save()

  private def getUpsertedMetricsCountForModel(modelSpace: String, modelExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$modelSpace-$modelExternalId",
      FlexibleDataModelRelationFactory.ResourceType)

}
