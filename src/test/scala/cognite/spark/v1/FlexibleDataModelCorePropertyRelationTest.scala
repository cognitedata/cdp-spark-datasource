package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.containers.{ContainerDefinition, ContainerId, ContainerReference}
import com.cognite.sdk.scala.v1.fdm.datamodels.DataModelCreate
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.NodeDeletionRequest
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

  private val containerAllListAndNonListExternalId = "sparkDsTestContainerAllListAndNonList2"
  private val containerNodesListAndNonListExternalId = "sparkDsTestContainerNodesListAndNonList2"
  private val containerEdgesListAndNonListExternalId = "sparkDsTestContainerEdgesListAndNonList2"

  private val containerAllNonListExternalId = "sparkDsTestContainerAllNonList2"
  private val containerNodesNonListExternalId = "sparkDsTestContainerNodesNonList2"
  private val containerEdgesNonListExternalId = "sparkDsTestContainerEdgesNonList2"

  private val containerAllAmbiguousTypeExternalId = "sparkDsTestContainerAllAmbiguousType3"
  private val containerNodesAmbiguousTypeExternalId = "sparkDsTestContainerNodesAmbiguousType3"
  private val containerEdgesAmbiguousTypeExternalId = "sparkDsTestContainerEdgesAmbiguousType3"

  private val containerAllTypeExternalId = "sparkDsTestContainerAllType1"
  private val containerNodesTypeExternalId = "sparkDsTestContainerNodesType1"
  private val containerEdgesTypeExternalId = "sparkDsTestContainerEdgesType1"

  private val containerAllListExternalId = "sparkDsTestContainerAllList2"
  private val containerNodesListExternalId = "sparkDsTestContainerNodesList2"
  private val containerEdgesListExternalId = "sparkDsTestContainerEdgesList2"

  private val viewAllListAndNonListExternalId = "sparkDsTestViewAllListAndNonList2"
  private val viewNodesListAndNonListExternalId = "sparkDsTestViewNodesListAndNonList2"
  private val viewEdgesListAndNonListExternalId = "sparkDsTestViewEdgesListAndNonList2"

  private val viewAllAmbiguousTypeExternalId = "sparkDsTestViewAllAmbiguousType3"
  private val viewNodesAmbiguousTypeExternalId = "sparkDsTestViewNodesAmbiguousType3"
  private val viewEdgesAmbiguousTypeExternalId = "sparkDsTestViewEdgesAmbiguousType3"

  private val viewAllTypeExternalId = "sparkDsTestViewAllType4"
  private val viewNodesTypeExternalId = "sparkDsTestViewNodesType4"
  private val viewEdgesTypeExternalId = "sparkDsTestViewEdgesType4"

  private val viewAllNonListExternalId = "sparkDsTestViewAllNonList2"
  private val viewNodesNonListExternalId = "sparkDsTestViewNodesNonList2"
  private val viewEdgesNonListExternalId = "sparkDsTestViewEdgesNonList2"

  private val viewAllListExternalId = "sparkDsTestViewAllList2"
  private val viewNodesListExternalId = "sparkDsTestViewNodesList2"
  private val viewEdgesListExternalId = "sparkDsTestViewEdgesList2"

  private val containerAllNumericProps = "sparkDsTestContainerNumericProps2"
  private val viewAllNumericProps = "sparkDsTestViewNumericProps2"

  private val containerAllRelationProps = "sparkDsTestContainerRelationProps2"
  private val viewAllRelationProps = "sparkDsTestViewRelationProps2"

  private val containerFilterByProps = "sparkDsTestContainerFilterByProps2"
  private val viewFilterByProps = "sparkDsTestViewFilterByProps2"

  private val containerSyncTest = "sparkDsTestContainerForSync"
  private val viewSyncTest = "sparkDsTestViewForSync"

  private val containerStartNodeAndEndNodesExternalId = "sparkDsTestContainerStartAndEndNodes2"
  private val viewStartNodeAndEndNodesExternalId = "sparkDsTestViewStartAndEndNodes2"
  private val typedContainerNodeExternalId = "sparkDsTestContainerTypedNodes6"
  private val typeContainerNodeExternalId = "sparkDsTestContainerTypeNode6"

  private val typedViewNodeExternalId = "sparkDsTestViewTypedNodes6"
  private val typeViewNodeExternalId = "sparkDsTestViewTypeNodes6"

  private val testDataModelExternalId = "sparkDsTestModel"

//  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  val nodeContainerProps: Map[String, ContainerPropertyDefinition] = Map(
    "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
    "stringProp2" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNullable,
  )

  // Nodes can also have properties named types. These are not the node's type and they should work together
  val nodeWithTypePropertyContainerProps: Map[String, ContainerPropertyDefinition] = Map(
    "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
    "type" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable
  )

  private lazy val containerTypeNode: ContainerDefinition =
    createContainerIfNotExists(Usage.Node, nodeContainerProps, typeContainerNodeExternalId)
      .unsafeRunSync()

  //To verify there is no conflict, we use a container with a propertyt also named "type"
  private lazy val containerTypedNode: ContainerDefinition =
    createContainerIfNotExists(Usage.Node, nodeWithTypePropertyContainerProps, typedContainerNodeExternalId)
      .unsafeRunSync()

  private lazy val containerStartAndEndNodes: ContainerDefinition =
    createContainerIfNotExists(Usage.Node, nodeContainerProps, containerStartNodeAndEndNodesExternalId)
      .unsafeRunSync()

  private lazy val viewStartAndEndNodes: ViewDefinition =
    createViewWithCorePropsIfNotExists(
      containerStartAndEndNodes,
      viewStartNodeAndEndNodesExternalId,
      viewVersion)
      .unsafeRunSync()

  //To use as type for other node
  private lazy val viewTypeNode: ViewDefinition = createViewWithCorePropsIfNotExists(
    containerTypeNode,
    typeViewNodeExternalId,
    viewVersion
  ).unsafeRunSync()

  //Types are optional on nodes, this is used to test using a node which has a type
  private lazy val viewTypedNode: ViewDefinition =
    createViewWithCorePropsIfNotExists(
      containerTypedNode,
      typedViewNodeExternalId,
      viewVersion
    ).unsafeRunSync()

  it should "succeed when inserting all nullable & non nullable non list values" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertNonListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertNonListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
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
                |    'externalId', '$startNodeExtId'
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

  it should "handle ambiguous types when there is a type property in the view of the edge" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      viewStartAndEndNodes.toSourceReference).unsafeRunSync()

    val (viewAll, viewNodes, viewEdges) = setupAmbiguousTypeTest.unsafeRunSync()
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
                |    'externalId', '$startNodeExtId'
                |) as _type,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$endNodeExtId'
                |) as type,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as startNode,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$endNodeExtId'
                |) as endNode
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

    val readEdgesDf = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      instanceSpaceExternalId = spaceExternalId
    )
    readEdgesDf.createTempView(s"edge_ambiguous_type_test_instances_table")

    val selectedEdgesBothTypes = spark
      .sql(
        f"""select * from edge_ambiguous_type_test_instances_table
          | where _type = struct('${spaceExternalId}' as space, '${startNodeExtId}' as externalId)
          | and type = struct('${spaceExternalId}' as space, '${endNodeExtId}' as externalId)
          |""".stripMargin)
      .collect()

    //In this case since both are present, we assume type refers to the view property
    val selectedEdgesTypeViewProperty = spark
      .sql(
        f"""select * from edge_ambiguous_type_test_instances_table
           | where type = struct('${spaceExternalId}' as space, '${endNodeExtId}' as externalId)
           |""".stripMargin)
      .collect()

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

    toExternalIds(selectedEdgesBothTypes).length shouldBe(1)
    toExternalIds(selectedEdgesTypeViewProperty).length shouldBe(1)
  }

  it should "handle using type for edges instance property when there is no property named type in the associated view" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
      viewStartAndEndNodes.toSourceReference).unsafeRunSync()

    val (viewAll, viewNodes, viewEdges) = setupTypeTest.unsafeRunSync()
    val randomId = generateNodeExternalId
    val instanceExtIdAll = s"${randomId}All"
    val instanceExtIdNode = s"${randomId}Node"
    val instanceExtIdEdge = s"${randomId}Edge"

    def insertionDf(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
                |select
                |'$instanceExtId' as externalId,
                |"propValue" as stringProp,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as _type,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as startNode,
                |named_struct(
                |    'spaceExternalId', '$spaceExternalId',
                |    'externalId', '$endNodeExtId'
                |) as endNode
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

    val readEdgesDf: DataFrame = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      instanceSpaceExternalId = spaceExternalId
    )
    readEdgesDf.createTempView(s"edge_type_test_instances_table")

    //since there is no property named type in the view, this refers to the instance property and is equal to _type
    val selectedEdgesBothTypes = spark
      .sql(
        f"""select * from edge_type_test_instances_table
           | where type = struct('${spaceExternalId}' as space, '${startNodeExtId}' as externalId)
           |""".stripMargin)
      .collect()

    val selectedEdgesTypeViewProperty = spark
      .sql(
        f"""select * from edge_type_test_instances_table
           | where _type = struct('${spaceExternalId}' as space, '${startNodeExtId}' as externalId)
           |""".stripMargin)
      .collect()

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

    toExternalIds(selectedEdgesBothTypes).length shouldBe(1)
    toExternalIds(selectedEdgesTypeViewProperty).length shouldBe(1)
  }

  it should "succeed when inserting all nullable & non nullable list values" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
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
                |    'externalId', '$startNodeExtId'
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
                |null as timestampListProp2,
                |array(
                |    named_struct(
                |       'spaceExternalId', '$spaceExternalId',
                |       'externalId', '$startNodeExtId'
                |    ),
                |    named_struct(
                |       'spaceExternalId', '$spaceExternalId',
                |       'externalId', '$endNodeExtId'
                |    )
                |) as directRelation3
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

    val syncNodesDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewNodes.externalId,
      viewVersion = viewNodes.version,
      cursor = ""
    )

    val readEdgesDf = readRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      instanceSpaceExternalId = spaceExternalId
    )

    val syncEdgesDf = syncRows(
      instanceType = InstanceType.Edge,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      cursor = ""
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
    syncNodesDf.createTempView(s"sync_nodes_table")
    syncEdgesDf.createTempView(s"sync_edges_table")

    val selectedNodes = spark
      .sql("select * from node_instances_table")
      .collect()

    val selectedEdges = spark
      .sql("select * from edge_instances_table")
      .collect()

    val selectedNodesAndEdges = spark
      .sql("select * from all_instances_table")
      .collect()

    val syncedNodes = spark
      .sql("select * from sync_nodes_table")
      .collect()

    val syncedEdges = spark
      .sql("select * from sync_edges_table")
      .collect()

    val syncedNodesExternalIds = toExternalIds(syncedNodes)
    val syncedEdgesExternalIds = toExternalIds(syncedEdges)
    val filterNodes = toExternalIds(selectedNodes)
    val filterEdges = toExternalIds(selectedEdges)
    val actualAllInstanceExternalIds = toExternalIds(selectedNodesAndEdges) ++ filterNodes ++ filterEdges

    allInstanceExternalIds.length shouldBe 8
    (actualAllInstanceExternalIds should contain).allElementsOf(allInstanceExternalIds)
    (syncedNodesExternalIds should contain).allElementsOf(filterNodes)
    (syncedEdgesExternalIds should contain).allElementsOf(filterEdges)
  }

  it should "succeed when filtering edges with type, startNode & endNode" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}FilterByEdgePropsStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}FilterByEdgePropsEndNode"
    createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId,
      endNodeExtId,
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
           | where startNode = struct('${startNodeRef.space}' as space, '${startNodeRef.externalId}' as externalId)
           | and endNode = struct('${endNodeRef.space}' as space, '${endNodeRef.externalId}' as externalId)
           | and _type = struct('${typeNodeRef.space}' as space, '${typeNodeRef.externalId}' as externalId)
           | and directRelation1 = struct('${directNodeReference.space}' as space, '${directNodeReference.externalId}' as externalId)
           | and space = '$spaceExternalId'
           | """.stripMargin)
      .collect()

    val actualAllEdgeExternalIds = toExternalIds(selectedEdges)

    allEdgeExternalIds.length shouldBe 2
    (actualAllEdgeExternalIds should contain).allElementsOf(allEdgeExternalIds)
  }

  it should "succeed when filtering nodes with type" in {
    val nullTypedNode = s"${viewStartNodeAndEndNodesExternalId}FilterByTypeNullType"
    val nonNullTypedNode = s"${viewStartNodeAndEndNodesExternalId}FilterByType"
    val typeNode = s"${viewStartNodeAndEndNodesExternalId}FilterByTypeType"

    createTypedNodesIfNotExists(
      nullTypedNode,
      nonNullTypedNode,
      typeNode,
      viewTypeNode.toSourceReference,
      viewTypedNode.toSourceReference).unsafeRunSync()

    val readNodesDf = readRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewTypedNode.externalId,
      viewVersion = viewTypedNode.version,
      instanceSpaceExternalId = spaceExternalId
    )

    readNodesDf.createTempView(s"node_filter_instances_table")

    val selectedNodes = spark
      .sql(s"""select * from node_filter_instances_table
              | where type = struct('${spaceExternalId}' as space, '${typeNode}' as externalId)
              | and _type = struct('${spaceExternalId}' as space, '${typeNode}' as externalId)
              | and space = '$spaceExternalId'
              | """.stripMargin)
      .collect()

    selectedNodes.length shouldBe 1
  }

  it should "be able to fetch more data with cursor when syncing" in {
    val (viewDefinition, modifiedExternalIds) = setupSyncTest.unsafeRunSync()
    val view = viewDefinition.toSourceReference
    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = ""
    )

    syncDf.createTempView(s"sync_empty_cursor")
    val syncedNodes = spark.sql("select * from sync_empty_cursor").collect()
    val syncedExternalIds = toExternalIds(syncedNodes)
    val lastRow = syncedNodes.last
    val cursor = lastRow.getString(lastRow.schema.fieldIndex("metadata.cursor"))

    val createdTime = lastRow.getLong(lastRow.schema.fieldIndex("node.createdTime"))
    createdTime should be > 1L
    val lastUpdated = lastRow.getLong(lastRow.schema.fieldIndex("node.lastUpdatedTime"))
    lastUpdated should be >= createdTime
    val deletedTime = lastRow.getLong(lastRow.schema.fieldIndex("node.deletedTime"))
    deletedTime shouldBe 0L
    val version = lastRow.getLong(lastRow.schema.fieldIndex("node.version"))
    version should be > 0L

    (syncedExternalIds should contain).allElementsOf(modifiedExternalIds)

    val syncNext = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = cursor)

    syncNext.createTempView(s"sync_next_cursor")
    var syncedNextNodes = spark.sql("select * from sync_next_cursor").collect()
    syncedNextNodes.length shouldBe 0

    // Add 10 nodes and verify we can sync them out with same cursor
    setupInstancesForSync(viewDefinition.toSourceReference, 10).unsafeRunSync()
    syncedNextNodes = spark.sql("select * from sync_next_cursor").collect()
    syncedNextNodes.length shouldBe 10
  }

  it should "respect filters in sync queries" in {
    val (viewDefinition, _) = setupSyncTest.unsafeRunSync()
    val view = viewDefinition.toSourceReference
    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = ""
    )

    syncDf.createTempView(s"sync_empty_cursor_with_filter")
    val syncedNodes = spark
      .sql("select * from sync_empty_cursor_with_filter where longProp > 10L and longProp <= 50")
      .collect()
    syncedNodes.length shouldBe 40

    val syncedNodes2 = spark
      .sql(
        "select * from sync_empty_cursor_with_filter where " +
          "`node.lastUpdatedTime` > 10L and " +
          "longProp > 0 and " +
          "space = '" + spaceExternalId + "' and " +
          "`node.deletedTime` = 0")
      .collect()
    syncedNodes2.length shouldBe 50

    val syncedNodes3 = spark
      .sql(
        "select * from sync_empty_cursor_with_filter where " +
          "`node.lastUpdatedTime` > 10L and " +
          "longProp > 0 and " +
          "space = 'nonexistingspace'")
      .collect()
    syncedNodes3.length shouldBe 0
  }

  it should "sync with old cursor " in {
    // Let us see what happens when this cursor gets more than 3 days..
    val oldCursorValue = "\"Z0FBQUFBQmw0RjJXenpCbHl3Ny02MzRRN21lTEVkdm5hU3hpQTM4d05ERkwxV2xNSG5" +
      "wa2ltOHhXMXloWC05akN6TDEzWGExMkkwZlpocGVhdVZXSDBGMVpEdmIwSTlzVDhQQUVhOHBNV2pCNUFNMFBsREg" +
      "4WjlEU3RacFFMbS1MM3hZaGVkV3ZFcVhoUy13R0NlODEzaEhlZUhUbVoxeTZ3bWVCX0tnSWdXRkphcjJIS2hfemh" +
      "pM216WktROG9RdjdqVmRWeFRMdDZ5TVdVNG1iNGo2X3J1VVVVRy10ZnowYUozSnV0R2ozZjd6YU1mMjBNVnc1eWN" +
      "tX1hmbF80RmY0RkdVRk5DVTI0UE9scW5rbkF1MEZIUHBKUklERDVnRDJ5ZWhXVGVwNE4wcVBpeVV1amdGSDZKeTd" +
      "nanhkRTNiZ2QtUTdWdGdhbUpHakdEWXpQRmNueEFGdGd3UTBHdHZpY3hZdnpHdk16QVBlNE8wSlZzUGk0d2hOV01" +
      "jRUFQdjNPNE9mQ0g0MG11S1NYMzJyWnczRkhBWG00WWJCRU1jUT09\""

    val (viewDefinition, modifiedExternalIds) = setupSyncTest.unsafeRunSync()
    val view = viewDefinition.toSourceReference
    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = oldCursorValue
    )

    syncDf.createTempView(s"sync_old_cursor")
    val syncedNodes = spark.sql("select * from sync_old_cursor").collect()
    val syncedExternalIds = toExternalIds(syncedNodes)

    (syncedExternalIds should contain).allElementsOf(modifiedExternalIds)
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

    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = ""
    )

    readDf.createTempView(s"instance_filter_table")
    syncDf.createTempView(s"sync_instance_filter_table")
    val filter =
      s"""
         |where
         |forEqualsFilter = 'str1' and
         |forInFilter in ('str1', 'str2', 'str3') and
         |forGteFilter >= 1 and
         |forGtFilter > 1 and
         |forLteFilter <= 2 and
         |forLtFilter < 4 and
         |(forOrFilter1 == 5.1 or forOrFilter2 == 6.1) and
         |forIsNotNullFilter is not null and
         |forIsNullFilter is null""".stripMargin
    val filterSql = s"""select * from instance_filter_table
                    |$filter
                    |""".stripMargin

    val syncSql = s"""select * from sync_instance_filter_table
                    |$filter
                    |""".stripMargin
    val filtered = spark
      .sql(filterSql)
      .collect()

    val synced = spark
      .sql(syncSql)
      .collect()

    filtered.length shouldBe (1)
    synced.length shouldBe (1)

    for (row <- filtered ++ synced) {
      row.getLong(row.schema.fieldIndex("node.createdTime")) should be >= 1L
      row.getLong(row.schema.fieldIndex("node.lastUpdatedTime")) should be >= 1L
      row.getLong(row.schema.fieldIndex("node.version")) should be >= 1L
      row.getLong(row.schema.fieldIndex("node.deletedTime")) shouldBe 0L
    }

    filtered.length shouldBe 1
    synced.length shouldBe 1
    val filteredInstanceExtIds =
      filtered.map(row => row.getString(row.schema.fieldIndex("externalId"))).toVector
    instanceExtIds.containsSlice(filteredInstanceExtIds) shouldBe true
    filteredInstanceExtIds shouldBe Vector(s"${view.externalId}Node1")
    val syncedInstanceExtIds =
      synced.map(row => row.getString(row.schema.fieldIndex("externalId"))).toVector
    syncedInstanceExtIds shouldBe Vector(s"${view.externalId}Node1")
  }

  it should "successfully read from relation properties" in {
    val viewDef = setupRelationReadPropsTest.unsafeRunSync()
    val nodeExtId1 = s"${viewDef.externalId}Relation1"

    //we use named struct here because we don't have access to node_reference
    val df = spark
      .sql(s"""
              |select
              |'$nodeExtId1' as externalId,
              |named_struct('space', '$spaceExternalId', 'externalId', '$nodeExtId1') as relProp,
              |array(named_struct('space', '$spaceExternalId', 'externalId', '$nodeExtId1')) as relListProp
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
    val dfFromModel = readRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = viewDef.space,
      viewVersion = viewDef.version,
      viewExternalId = viewDef.externalId,
      instanceSpaceExternalId = viewDef.space
    )
    dfFromModel.createTempView("temp_view_with_relations")
    val dfRead = spark
      .sql(s"""
              |select
              |'$nodeExtId1' as externalId,
              |`relProp` as relProp,
              |`relListProp` as relListProp
              |from temp_view_with_relations
              |""".stripMargin)
    val result2 = Try {
      insertRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = viewDef.space,
        viewExternalId = viewDef.externalId,
        viewVersion = viewDef.version,
        instanceSpaceExternalId = viewDef.space,
        dfRead
      )
    }
    result2 shouldBe Success(())
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

    val propertyMapForInstances = (IO.sleep(2.seconds) *> client.instances
      .retrieveByExternalIds(
        Vector(
          InstanceRetrieve(
            instanceType = InstanceType.Node,
            externalId = nodeExtId1,
            space = spaceExternalId
          ),
          InstanceRetrieve(
            instanceType = InstanceType.Node,
            externalId = nodeExtId2,
            space = spaceExternalId
          )
        ),
        includeTyping = true,
        sources = Some(Seq(InstanceSource(viewDef.toSourceReference)))
      )
      .map { instances =>
        instances.items.collect {
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
    toExternalIds(rows).toVector shouldBe Vector(
      s"${viewStartNodeAndEndNodesExternalId}InsertNonListStartNode")
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

  it should "leave unmentioned properties alone" in {
    val externalId = s"sparkDsTestNodePropsTest-${shortRandomString()}"
    try {
      // First, create a random node
      val insertDf = spark
        .sql(s"""
             |select
             |'${externalId}' as externalId,
             |'stringProp1Val' as stringProp1,
             |'stringProp2Val' as stringProp2
             |""".stripMargin)
      val insertResult = Try {
        insertRowsToModel(
          modelSpace = spaceExternalId,
          modelExternalId = testDataModelExternalId,
          modelVersion = viewVersion,
          viewExternalId = viewStartNodeAndEndNodesExternalId,
          instanceSpace = Some(spaceExternalId),
          insertDf
        )
      }
      insertResult shouldBe Success(())
      // Now, update one of the values
      val updateDf = spark
        .sql(s"""
             |select
             |'${externalId}' as externalId,
             |'updatedProp1Val' as stringProp1
             |""".stripMargin)
      val updateResult = Try {
        insertRowsToModel(
          modelSpace = spaceExternalId,
          modelExternalId = testDataModelExternalId,
          modelVersion = viewVersion,
          viewExternalId = viewStartNodeAndEndNodesExternalId,
          instanceSpace = Some(spaceExternalId),
          updateDf
        )
      }
      updateResult shouldBe Success(())
      // Fetch the updated instance
      val instance = client.instances
        .retrieveByExternalIds(
          items = Seq(InstanceRetrieve(InstanceType.Node, externalId, spaceExternalId)),
          sources = Some(
            Seq(
              InstanceSource(
                ViewReference(
                  space = spaceExternalId,
                  externalId = viewStartNodeAndEndNodesExternalId,
                  version = viewVersion
                ))))
        )
        .unsafeRunSync()
        .items
        .head
      val props = instance.properties.get
        .get(spaceExternalId)
        .get(s"${viewStartNodeAndEndNodesExternalId}/${viewVersion}")
      // Check the properties
      props.get("stringProp1") shouldBe Some(InstancePropertyValue.String("updatedProp1Val"))
      props.get("stringProp2") shouldBe Some(InstancePropertyValue.String("stringProp2Val"))
    } finally {
      val _ = client.instances
        .delete(Seq(NodeDeletionRequest(space = spaceExternalId, externalId = externalId)))
        .unsafeRunSync()
    }
  }

  it should "handle nulls according to ignoreNullFields" in {
    val externalId = s"sparkDsTestNullProps-${shortRandomString()}"
    try {
      // First, create a random node
      val insertDf = spark
        .sql(s"""
             |select
             |'${externalId}' as externalId,
             |'stringProp1Val' as stringProp1,
             |'stringProp2Val' as stringProp2
             |""".stripMargin)
      val insertResult = Try {
        insertRowsToModel(
          modelSpace = spaceExternalId,
          modelExternalId = testDataModelExternalId,
          modelVersion = viewVersion,
          viewExternalId = viewStartNodeAndEndNodesExternalId,
          instanceSpace = Some(spaceExternalId),
          insertDf
        )
      }
      insertResult shouldBe Success(())
      // Now, update one of the values with a null, but ignoreNullFields
      val updateDf1 = spark
        .sql(s"""
             |select
             |'${externalId}' as externalId,
             |'updatedProp1Val' as stringProp1,
             |null as stringProp2
             |""".stripMargin)
      val updateResult1 = Try {
        insertRowsToModel(
          modelSpace = spaceExternalId,
          modelExternalId = testDataModelExternalId,
          modelVersion = viewVersion,
          viewExternalId = viewStartNodeAndEndNodesExternalId,
          instanceSpace = Some(spaceExternalId),
          updateDf1,
          ignoreNullFields = true
        )
      }
      updateResult1 shouldBe Success(())

      // Fetch the updated instance
      val instance1 = client.instances
        .retrieveByExternalIds(
          items = Seq(InstanceRetrieve(InstanceType.Node, externalId, spaceExternalId)),
          sources = Some(
            Seq(
              InstanceSource(
                ViewReference(
                  space = spaceExternalId,
                  externalId = viewStartNodeAndEndNodesExternalId,
                  version = viewVersion
                ))))
        )
        .unsafeRunSync()
        .items
        .head
      val props1 = instance1.properties.get
        .get(spaceExternalId)
        .get(s"${viewStartNodeAndEndNodesExternalId}/${viewVersion}")
      // Check the properties
      props1.get("stringProp1") shouldBe Some(InstancePropertyValue.String("updatedProp1Val"))
      props1.get("stringProp2") shouldBe Some(InstancePropertyValue.String("stringProp2Val"))

      // Update the value with null, and don't ignoreNullFields
      val updateDf2 = spark
        .sql(s"""
             |select
             |'${externalId}' as externalId,
             |'updatedProp1ValAgain' as stringProp1,
             |null as stringProp2
             |""".stripMargin)
      val updateResult2 = Try {
        insertRowsToModel(
          modelSpace = spaceExternalId,
          modelExternalId = testDataModelExternalId,
          modelVersion = viewVersion,
          viewExternalId = viewStartNodeAndEndNodesExternalId,
          instanceSpace = Some(spaceExternalId),
          updateDf2,
          ignoreNullFields = false
        )
      }
      updateResult2 shouldBe Success(())
      // Fetch the updated instance
      val instance = client.instances
        .retrieveByExternalIds(
          items = Seq(InstanceRetrieve(InstanceType.Node, externalId, spaceExternalId)),
          sources = Some(
            Seq(
              InstanceSource(
                ViewReference(
                  space = spaceExternalId,
                  externalId = viewStartNodeAndEndNodesExternalId,
                  version = viewVersion
                ))))
        )
        .unsafeRunSync()
        .items
        .head
      val props2 = instance.properties.get
        .get(spaceExternalId)
        .get(s"${viewStartNodeAndEndNodesExternalId}/${viewVersion}")
      // Check the properties
      props2.get("stringProp1") shouldBe Some(InstancePropertyValue.String("updatedProp1ValAgain"))
      props2.get("stringProp2") shouldBe None
    } finally {
      val _ = client.instances
        .delete(Seq(NodeDeletionRequest(space = spaceExternalId, externalId = externalId)))
        .unsafeRunSync()
    }
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
      "directRelation3" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyListWithoutDefaultValueNullable,
      "listOfDirectRelations" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyListWithoutDefaultValueNullable,
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

  private def setupAmbiguousTypeTest: IO[(ViewDefinition, ViewDefinition, ViewDefinition)] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "type" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllAmbiguousTypeExternalId)
      cNodes <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesAmbiguousTypeExternalId)
      cEdges <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesAmbiguousTypeExternalId)
      viewAll <- createViewWithCorePropsIfNotExists(cAll, viewAllAmbiguousTypeExternalId, viewVersion)
      viewNodes <- createViewWithCorePropsIfNotExists(cNodes, viewNodesAmbiguousTypeExternalId, viewVersion)
      viewEdges <- createViewWithCorePropsIfNotExists(cEdges, viewEdgesAmbiguousTypeExternalId, viewVersion)
      _ <- IO.sleep(5.seconds)
    } yield (viewAll, viewNodes, viewEdges)
  }

  private def setupTypeTest: IO[(ViewDefinition, ViewDefinition, ViewDefinition)] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringProp" -> FDMContainerPropertyTypes.TextPropertyNonListWithoutDefaultValueNullable,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllTypeExternalId)
      cNodes <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesTypeExternalId)
      cEdges <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesTypeExternalId)
      viewAll <- createViewWithCorePropsIfNotExists(cAll, viewAllTypeExternalId, viewVersion)
      viewNodes <- createViewWithCorePropsIfNotExists(cNodes, viewNodesTypeExternalId, viewVersion)
      viewEdges <- createViewWithCorePropsIfNotExists(cEdges, viewEdgesTypeExternalId, viewVersion)
      _ <- IO.sleep(5.seconds)
    } yield (viewAll, viewNodes, viewEdges)
  }

  private def setupSyncTest: IO[(ViewDefinition, Seq[String])] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringProp" -> FDMContainerPropertyTypes.TextPropertyNonListWithoutDefaultValueNullable,
      "longProp" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNullable,
    )

    for {
      containerDefinition <- createContainerIfNotExists(Usage.All, containerProps, containerSyncTest)
      viewDefinition <- createViewWithCorePropsIfNotExists(
        containerDefinition,
        viewSyncTest,
        viewVersion)
      extIds <- setupInstancesForSync(viewDefinition.toSourceReference, 50)
    } yield (viewDefinition, extIds)
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

  private def setupInstancesForSync(viewRef: ViewReference, instances: Int): IO[Seq[String]] = {
    val items =
      for (i <- 1 to instances)
        yield
          NodeWrite(
            spaceExternalId,
            "external" + i,
            sources = Some(
              Seq(EdgeOrNodeData(
                viewRef,
                Some(Map(
                  "stringProp" -> Some(InstancePropertyValue.String(System.nanoTime().toString)),
                  "longProp" -> Some(InstancePropertyValue.Int64(i.toLong))
                ))
              ))),
            `type` = None
          )
    client.instances
      .createItems(InstanceCreate(items = items))
      .map(_.collect { case n: SlimNodeOrEdge.SlimNodeDefinition => n.externalId })
      .map(_.distinct)
  }

  private def setupInstancesForFiltering(viewDef: ViewDefinition): IO[Seq[String]] = {
    val viewExtId = viewDef.externalId
    val source = viewDef.toInstanceSource
    val viewRef = viewDef.toSourceReference
    val instanceRetrieves = Vector(
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = s"${viewExtId}Node1",
        space = spaceExternalId
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = s"${viewExtId}Node2",
        space = spaceExternalId
      )
    )
    client.instances
      .retrieveByExternalIds(instanceRetrieves, sources = Some(Seq(source)))
      .map(_.items)
      .flatMap { instances =>
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
                        "forEqualsFilter" -> Some(InstancePropertyValue.String("str1")),
                        "forInFilter" -> Some(InstancePropertyValue.String("str1")),
                        "forGteFilter" -> Some(InstancePropertyValue.Int32(1)),
                        "forGtFilter" -> Some(InstancePropertyValue.Int32(2)),
                        "forLteFilter" -> Some(InstancePropertyValue.Int64(2)),
                        "forLtFilter" -> Some(InstancePropertyValue.Int64(3)),
                        "forOrFilter1" -> Some(InstancePropertyValue.Float64(5.1)),
                        "forOrFilter2" -> Some(InstancePropertyValue.Float64(6.1)),
                        "forIsNotNullFilter" -> Some(InstancePropertyValue.Date(LocalDate.now()))
                      ))
                    ))),
                    `type` = None
                  ),
                  NodeWrite(
                    spaceExternalId,
                    s"${viewDef.externalId}Node2",
                    Some(Seq(EdgeOrNodeData(
                      viewRef,
                      Some(Map(
                        "forEqualsFilter" -> Some(InstancePropertyValue.String("str2")),
                        "forInFilter" -> Some(InstancePropertyValue.String("str2")),
                        "forGteFilter" -> Some(InstancePropertyValue.Int32(5)),
                        "forGtFilter" -> Some(InstancePropertyValue.Int32(2)),
                        "forLteFilter" -> Some(InstancePropertyValue.Int64(1)),
                        "forLtFilter" -> Some(InstancePropertyValue.Int64(-1)),
                        "forOrFilter1" -> Some(InstancePropertyValue.Float64(5.1)),
                        "forOrFilter2" -> Some(InstancePropertyValue.Float64(6.1)),
                        "forIsNotNullFilter" -> Some(InstancePropertyValue.Date(LocalDate.now())),
                        "forIsNullFilter" -> Some(InstancePropertyValue.Object(Json.fromJsonObject(
                          JsonObject("a" -> Json.fromString("a"), "b" -> Json.fromInt(1)))))
                      ))
                    ))),
                    `type` = None
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

  private def setupRelationReadPropsTest: IO[ViewDefinition] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "relProp" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable,
      "relListProp" -> FDMContainerPropertyTypes.DirectNodeRelationPropertyListWithoutDefaultValueNullable,
    )

    for {
      _ <- createSpaceIfNotExists(spaceExternalId)
      container <- createContainerIfNotExists(Usage.All, containerProps, containerAllRelationProps)
      view <- createViewWithCorePropsIfNotExists(container, viewAllRelationProps, viewVersion)
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
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
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
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpace", instanceSpaceExternalId)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .option("collectMetrics", true)
      .load()

  private def syncRows(
      instanceType: InstanceType,
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      cursor: String): DataFrame =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
      .option("cursor", cursor)
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
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
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
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
      onConflict: String = "upsert",
      ignoreNullFields: Boolean = true): Unit =
    df.write
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
      .option("modelSpace", modelSpace)
      .option("modelExternalId", modelExternalId)
      .option("modelVersion", modelVersion)
      .option("instanceSpace", instanceSpace.orNull)
      .option("viewExternalId", viewExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .option("ignoreNullFields", ignoreNullFields)
      .save()

  private def getUpsertedMetricsCountForModel(modelSpace: String, modelExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$modelSpace-$modelExternalId",
      FlexibleDataModelRelationFactory.ResourceType)

}
