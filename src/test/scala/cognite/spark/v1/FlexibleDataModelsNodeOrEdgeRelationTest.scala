package cognite.spark.v1

import cats.Apply
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ContainerPropertyDefinition,
  ViewCorePropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.common.{DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.containers.{
  ContainerCreateDefinition,
  ContainerDefinition,
  ContainerId,
  ContainerReference
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}

import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.util.{Random, Success, Try}

class FlexibleDataModelsNodeOrEdgeRelationTest extends FlatSpec with Matchers with SparkTest {

  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val client = getBlufieldClient()

  private val spaceExternalId = "testSpaceSparkDs2"

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

  private val viewVersion = "v1"

//  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  val nodeContainerProps: Map[String, ContainerPropertyDefinition] = Map(
    "stringProp1" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNonNullable,
    "stringProp2" -> FDMContainerPropertyTypes.TextPropertyNonListWithDefaultValueNullable,
  )

  private val containerStartAndEndNodes: ContainerDefinition =
    createContainerIfNotExists(Usage.Node, nodeContainerProps, containerStartNodeAndEndNodesExternalId)
      .unsafeRunSync()

  private val viewStartAndEndNodes: ViewDefinition =
    createViewIfNotExists(containerStartAndEndNodes, viewStartNodeAndEndNodesExternalId, viewVersion)
      .unsafeRunSync()

  ignore should "succeed when inserting all nullable & non nullable non list values" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertNonListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertNonListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(startNodeExtId, endNodeExtId).unsafeRunSync()

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
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdAll)
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdNode)
        ),
        insertRows(
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

    def deletionDf(instanceExtId: String): DataFrame = {
      val space = if (instanceExtId.endsWith("Node")) "null" else s"'$spaceExternalId'"
      spark
        .sql(s"""
             |select
             | $space as space,
             |'$instanceExtId' as externalId
             |""".stripMargin)
    }

    val deletionResults = Try {
      Vector(
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdAll),
          onConflict = "delete"
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdNode),
          onConflict = "delete"
        ),
        insertRows(
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

  ignore should "succeed when inserting all nullable & non nullable list values" in {
    val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListStartNode"
    val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}InsertListEndNode"
    createStartAndEndNodesForEdgesIfNotExists(startNodeExtId, endNodeExtId).unsafeRunSync()

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
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdAll)
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          insertionDf(instanceExtIdNode)
        ),
        insertRows(
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

    def deletionDf(instanceExtId: String): DataFrame = {
      val space = if (instanceExtId.endsWith("Node")) "null" else s"'$spaceExternalId'"
      spark
        .sql(s"""
             |select
             | $space as space,
             |'$instanceExtId' as externalId
             |""".stripMargin)
    }

    val deletionResults = Try {
      Vector(
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdAll),
          onConflict = "delete"
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          deletionDf(instanceExtIdNode),
          onConflict = "delete"
        ),
        insertRows(
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
    createStartAndEndNodesForEdgesIfNotExists(startNodeExtId, endNodeExtId).unsafeRunSync()

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

    val (_, _, viewEdges, allInstanceExternalIds) = (for {
      (viewAll, viewNodes, viewEdges) <- setupAllListAndNonListPropertyTest
      nodeIds <- createTestInstancesForView(
        viewNodes,
        directNodeReference = directNodeReference,
        None,
        None)
      edgeIds <- createTestInstancesForView(
        viewEdges,
        directNodeReference,
        Some(startNodeRef),
        Some(endNodeRef))
      allIds <- createTestInstancesForView(
        viewAll,
        directNodeReference,
        Some(startNodeRef),
        Some(endNodeRef))
    } yield (viewAll, viewNodes, viewEdges, nodeIds ++ edgeIds ++ allIds)).unsafeRunSync()

//    val readNodesDf = readRows(
//      viewSpaceExternalId = spaceExternalId,
//      viewExternalId = viewNodes.externalId,
//      viewVersion = viewNodes.version,
//      instanceSpaceExternalId = spaceExternalId
//    )

    val readEdgesDf = readRows(
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewEdges.externalId,
      viewVersion = viewEdges.version,
      instanceSpaceExternalId = spaceExternalId
    )

//    val readAllDf = readRows(
//      viewSpaceExternalId = spaceExternalId,
//      viewExternalId = viewAll.externalId,
//      viewVersion = viewAll.version,
//      instanceSpaceExternalId = spaceExternalId
//    )

//    readNodesDf.createTempView(s"node_instances_table")
    readEdgesDf.createTempView(s"edge_instances_table")
//    readAllDf.createTempView(s"all_instances_table")

//    val selectedNodes = spark
//      .sql("select * from node_instances_table")
//      .collect()

    val selectedEdges = spark
      .sql("select * from edge_instances_table")
      .collect()

//    val selectedNodesAndEdges = spark
//      .sql("select * from all_instances_table")
//      .collect()

    def toExternalIds(rows: Array[Row]): Array[String] =
      rows.map(row => row.getString(row.schema.fieldIndex("externalId")))

//    val actualAllInstanceExternalIds = toExternalIds(selectedNodesAndEdges) ++ toExternalIds(
//      selectedNodes) ++ toExternalIds(selectedEdges)
    val actualAllInstanceExternalIds = toExternalIds(selectedEdges)

    allInstanceExternalIds.length shouldBe 8
    (actualAllInstanceExternalIds should contain).allElementsOf(allInstanceExternalIds)
  }

  ignore should "succeed when filtering instances by properties" in {
    val (view, instanceExtIds) = setupFilteringByPropertiesTest.unsafeRunSync()

    val readDf = readRows(
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

  ignore should "successfully cast numeric properties" in {
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

  ignore should "delete containers and views used for testing" in {
    client.containers
      .delete(Seq(
        ContainerId(spaceExternalId, containerAllNonListExternalId),
        ContainerId(spaceExternalId, containerNodesNonListExternalId),
        ContainerId(spaceExternalId, containerEdgesNonListExternalId),
        ContainerId(spaceExternalId, containerAllNumericProps),
        ContainerId(spaceExternalId, containerFilterByProps),
        ContainerId(spaceExternalId, containerStartNodeAndEndNodesExternalId),
      ))
      .unsafeRunSync()

    client.views
      .deleteItems(Seq(
        DataModelReference(spaceExternalId, viewAllListAndNonListExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewNodesListAndNonListExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewEdgesListAndNonListExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewAllNumericProps, viewVersion),
        DataModelReference(spaceExternalId, viewFilterByProps, viewVersion),
        DataModelReference(spaceExternalId, viewStartNodeAndEndNodesExternalId, viewVersion),
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
      viewAll <- createViewIfNotExists(cAll, viewAllListAndNonListExternalId, viewVersion)
      viewNodes <- createViewIfNotExists(cNodes, viewNodesListAndNonListExternalId, viewVersion)
      viewEdges <- createViewIfNotExists(cEdges, viewEdgesListAndNonListExternalId, viewVersion)
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
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllNonListExternalId)
      cNodes <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesNonListExternalId)
      cEdges <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesNonListExternalId)
      viewAll <- createViewIfNotExists(cAll, viewAllNonListExternalId, viewVersion)
      viewNodes <- createViewIfNotExists(cNodes, viewNodesNonListExternalId, viewVersion)
      viewEdges <- createViewIfNotExists(cEdges, viewEdgesNonListExternalId, viewVersion)
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
      viewAll <- createViewIfNotExists(cAll, viewAllListExternalId, viewVersion)
      viewNodes <- createViewIfNotExists(cNodes, viewNodesListExternalId, viewVersion)
      viewEdges <- createViewIfNotExists(cEdges, viewEdgesListExternalId, viewVersion)
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
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerFilterByProps)
      viewAll <- createViewIfNotExists(cAll, viewFilterByProps, viewVersion)
      extIds <- setupInstancesForFiltering(viewAll)
    } yield (viewAll, extIds)
  }

  // scalastyle:off method.length
  private def createTestInstancesForView(
      viewDef: ViewDefinition,
      directNodeReference: DirectRelationReference,
      startNode: Option[DirectRelationReference],
      endNode: Option[DirectRelationReference]): IO[Seq[String]] = {
    val randomPrefix = apiCompatibleRandomString()
    val writeData = viewDef.usedFor match {
      case Usage.Node =>
        createNodeWriteInstances(viewDef, directNodeReference, randomPrefix)
      case Usage.Edge =>
        Apply[Option].map2(startNode, endNode)(Tuple2.apply).toSeq.flatMap {
          case (s, e) =>
            createEdgeWriteInstances(
              viewDef,
              startNode = s,
              endNode = e,
              directNodeReference,
              randomPrefix)
        }
      case Usage.All =>
        createNodeWriteInstances(viewDef, directNodeReference, randomPrefix) ++ Apply[Option]
          .map2(startNode, endNode)(Tuple2.apply)
          .toSeq
          .flatMap {
            case (s, e) =>
              createEdgeWriteInstances(
                viewDef,
                startNode = s,
                endNode = e,
                directNodeReference,
                randomPrefix)
          }
    }

    client.instances
      .createItems(
        instance = InstanceCreate(
          items = writeData,
          replace = Some(true)
        )
      )
      .map(_.map(_.externalId))
      .flatTap(_ => IO.sleep(5.seconds))
  }
  // scalastyle:on method.length

  private def createEdgeWriteInstances(
      viewDef: ViewDefinition,
      startNode: DirectRelationReference,
      endNode: DirectRelationReference,
      directNodeReference: DirectRelationReference,
      randomPrefix: String) = {
    val viewRef = viewDef.toSourceReference
    val edgeExternalIdPrefix = s"${viewDef.externalId}${randomPrefix}Edge"
    Seq(
      EdgeWrite(
//        `type` = DirectRelationReference(space = spaceExternalId, externalId = s"${edgeExternalIdPrefix}Type1"),
        `type` = startNode,
        space = spaceExternalId,
        externalId = s"${edgeExternalIdPrefix}1",
        startNode = startNode,
        endNode = endNode,
        sources = Some(
          Seq(
            EdgeOrNodeData(
              viewRef,
              Some(viewDef.properties.collect { case (n, p: ViewCorePropertyDefinition) => n -> p }.map {
                case (n, p) => n -> createInstancePropertyValue(n, p.`type`, directNodeReference)
              })
            )
          )
        )
      ),
      EdgeWrite(
        `type` =
          DirectRelationReference(space = spaceExternalId, externalId = s"${edgeExternalIdPrefix}Type2"),
        space = spaceExternalId,
        externalId = s"${edgeExternalIdPrefix}2",
        startNode = startNode,
        endNode = endNode,
        sources = Some(
          Seq(
            EdgeOrNodeData(
              viewRef,
              Some(viewDef.properties.collect { case (n, p: ViewCorePropertyDefinition) => n -> p }.map {
                case (n, p) => n -> createInstancePropertyValue(n, p.`type`, directNodeReference)
              })
            )
          )
        )
      )
    )
  }

  private def createNodeWriteInstances(
      viewDef: ViewDefinition,
      directNodeReference: DirectRelationReference,
      randomPrefix: String) = {
    val viewRef = viewDef.toSourceReference
    Seq(
      NodeWrite(
        spaceExternalId,
        s"${viewDef.externalId}${randomPrefix}Node1",
        sources = Some(
          Seq(
            EdgeOrNodeData(
              viewRef,
              Some(viewDef.properties.collect { case (n, p: ViewCorePropertyDefinition) => n -> p }.map {
                case (n, p) => n -> createInstancePropertyValue(n, p.`type`, directNodeReference)
              })
            ))
        )
      ),
      NodeWrite(
        spaceExternalId,
        s"${viewDef.externalId}${randomPrefix}Node2",
        sources = Some(
          Seq(
            EdgeOrNodeData(
              viewRef,
              Some(viewDef.properties.collect { case (n, p: ViewCorePropertyDefinition) => n -> p }.map {
                case (n, p) => n -> createInstancePropertyValue(n, p.`type`, directNodeReference)
              })
            ))
        )
      )
    )
  }

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
      view <- createViewIfNotExists(container, viewAllNumericProps, viewVersion)
    } yield view
  }

  private def insertRows(
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: String,
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
      .option("viewSpaceExternalId", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpaceExternalId", instanceSpaceExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .save()

  private def readRows(
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: String): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", FlexibleDataModelRelation.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("viewSpaceExternalId", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpaceExternalId", instanceSpaceExternalId)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .option("collectMetrics", true)
      .load()

  private def createContainerIfNotExists(
      usage: Usage,
      properties: Map[String, ContainerPropertyDefinition],
      containerExternalId: String): IO[ContainerDefinition] =
    client.containers
      .retrieveByExternalIds(
        Seq(ContainerId(spaceExternalId, containerExternalId))
      )
      .flatMap { containers =>
        if (containers.isEmpty) {
          val containerToCreate = ContainerCreateDefinition(
            space = spaceExternalId,
            externalId = containerExternalId,
            name = Some(s"Test-Container-Spark-DS-$usage"),
            description = Some(s"Test Container For Spark Datasource $usage"),
            usedFor = Some(usage),
            properties = properties,
            constraints = None,
            indexes = None
          )
          client.containers
            .createItems(containers = Seq(containerToCreate))
            .flatTap(_ => IO.sleep(5.seconds))
        } else {
          IO.delay(containers)
        }
      }
      .map(_.head)

  private def createViewIfNotExists(
      container: ContainerDefinition,
      viewExternalId: String,
      viewVersion: String): IO[ViewDefinition] =
    client.views
      .retrieveItems(items = Seq(DataModelReference(spaceExternalId, viewExternalId, viewVersion)))
      .flatMap { views =>
        if (views.isEmpty) {
          val containerRef = container.toSourceReference
          val viewToCreate = ViewCreateDefinition(
            space = spaceExternalId,
            externalId = viewExternalId,
            version = viewVersion,
            name = Some(s"Test-View-Spark-DS"),
            description = Some("Test View For Spark Datasource"),
            filter = None,
            properties = container.properties.map {
              case (pName, _) =>
                pName -> ViewPropertyCreateDefinition.CreateViewProperty(
                  name = Some(pName),
                  container = containerRef,
                  containerPropertyIdentifier = pName)
            },
            implements = None,
          )

          client.views
            .createItems(items = Seq(viewToCreate))
            .flatTap(_ => IO.sleep(5.seconds))
        } else {
          IO.delay(views)
        }
      }
      .map(_.head)

  // scalastyle:off method.length
  private def createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId: String,
      endNodeExtId: String): IO[Unit] = {
    val instanceRetrieves = Vector(
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = startNodeExtId,
        space = spaceExternalId,
        sources = Some(Seq(viewStartAndEndNodes.toInstanceSource))
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = endNodeExtId,
        space = spaceExternalId,
        sources = Some(Seq(viewStartAndEndNodes.toInstanceSource))
      )
    )
    client.instances
      .retrieveByExternalIds(instanceRetrieves, false)
      .flatMap { response =>
        val nodes = response.items.collect {
          case n: InstanceDefinition.NodeDefinition => n
        }
        if (nodes.size === 2) {
          IO.unit
        } else {
          client.instances
            .createItems(instance = InstanceCreate(
              items = Seq(
                NodeWrite(
                  spaceExternalId,
                  startNodeExtId,
                  Some(
                    Seq(EdgeOrNodeData(
                      viewStartAndEndNodes.toSourceReference,
                      Some(Map(
                        "stringProp1" -> InstancePropertyValue.String("stringProp1Val"),
                        "stringProp2" -> InstancePropertyValue.String("stringProp2Val")))
                    ))
                  )
                ),
                NodeWrite(
                  spaceExternalId,
                  endNodeExtId,
                  Some(
                    Seq(EdgeOrNodeData(
                      viewStartAndEndNodes.toSourceReference,
                      Some(Map(
                        "stringProp1" -> InstancePropertyValue.String("stringProp1Val"),
                        "stringProp2" -> InstancePropertyValue.String("stringProp2Val")))
                    ))
                  )
                )
              ),
              replace = Some(true)
            ))
            .flatTap(_ => IO.sleep(5.seconds)) *> IO.unit
        }
      }
  }
  // scalastyle:off method.length

  private def apiCompatibleRandomString(): String =
    UUID.randomUUID().toString.replaceAll("[_\\-x0]", "").substring(0, 5)

  private def generateNodeExternalId: String = s"randomId${apiCompatibleRandomString()}"

  private def getUpsertedMetricsCount(viewDef: ViewDefinition) =
    getNumberOfRowsUpserted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelation.ResourceType)

  private def getDeletedMetricsCount(viewDef: ViewDefinition) =
    getNumberOfRowsDeleted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelation.ResourceType)

  def createInstancePropertyValue(
      propName: String,
      propType: PropertyType,
      directNodeReference: DirectRelationReference
  ): InstancePropertyValue =
    propType match {
      case d: DirectNodeRelationProperty =>
        val ref = d.container.map(_ => directNodeReference)
        InstancePropertyValue.ViewDirectNodeRelation(value = ref)
      case p =>
        if (p.isList) {
          listContainerPropToInstanceProperty(propName, p)
        } else {
          nonListContainerPropToInstanceProperty(propName, p)
        }
    }

  // scalastyle:off cyclomatic.complexity
  private def listContainerPropToInstanceProperty(
      propName: String,
      propertyType: PropertyType
  ): InstancePropertyValue =
    propertyType match {
      case PropertyType.TextProperty(Some(true), _) =>
        InstancePropertyValue.StringList(List(s"${propName}Value1", s"${propName}Value2"))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
        InstancePropertyValue.BooleanList(List(true, false, true, false))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
        InstancePropertyValue.Int32List((1 to 10).map(_ => Random.nextInt(10000)).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
        InstancePropertyValue.Int64List((1 to 10).map(_ => Random.nextLong()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
        InstancePropertyValue.Float32List((1 to 10).map(_ => Random.nextFloat()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
        InstancePropertyValue.Float64List((1 to 10).map(_ => Random.nextDouble()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
        InstancePropertyValue.DateList(
          (1 to 10).toList.map(i => LocalDate.now().minusDays(i.toLong))
        )
      case PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
        InstancePropertyValue.TimestampList(
          (1 to 10).toList.map(i => LocalDateTime.now().minusDays(i.toLong).atZone(ZoneId.of("UTC")))
        )
      case PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)) =>
        InstancePropertyValue.ObjectList(
          List(
            Json.fromJsonObject(
              JsonObject.fromMap(
                Map(
                  "a" -> Json.fromString("a"),
                  "b" -> Json.fromInt(1),
                  "c" -> Json.fromBoolean(true)
                )
              )
            ),
            Json.fromJsonObject(
              JsonObject.fromMap(
                Map(
                  "a" -> Json.fromString("b"),
                  "b" -> Json.fromInt(1),
                  "c" -> Json.fromBoolean(false),
                  "d" -> Json.fromDoubleOrString(1.56)
                )
              )
            )
          )
        )
      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  private def nonListContainerPropToInstanceProperty(
      propName: String,
      propertyType: PropertyType
  ): InstancePropertyValue =
    propertyType match {
      case PropertyType.TextProperty(None | Some(false), _) =>
        InstancePropertyValue.String(s"${propName}Value")
      case PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, _) =>
        InstancePropertyValue.Boolean(false)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
        InstancePropertyValue.Int32(Random.nextInt(10000))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
        InstancePropertyValue.Int64(Random.nextLong())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float32, None | Some(false)) =>
        InstancePropertyValue.Float32(Random.nextFloat())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
        InstancePropertyValue.Float64(Random.nextDouble())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
        InstancePropertyValue.Date(LocalDate.now().minusDays(Random.nextInt(30).toLong))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
        InstancePropertyValue.Timestamp(
          LocalDateTime.now().minusDays(Random.nextInt(30).toLong).atZone(ZoneId.of("UTC")))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
        InstancePropertyValue.Object(
          Json.fromJsonObject(
            JsonObject.fromMap(
              Map(
                "a" -> Json.fromString("a"),
                "b" -> Json.fromInt(1),
                "c" -> Json.fromBoolean(true)
              )
            )
          )
        )
      case _: PropertyType.DirectNodeRelationProperty =>
        InstancePropertyValue.ViewDirectNodeRelation(None)

      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }
}
