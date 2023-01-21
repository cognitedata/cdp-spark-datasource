package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.containers.{
  ContainerCreateDefinition,
  ContainerDefinition,
  ContainerId
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.util.{Success, Try}

class FlexibleDataModelsRelationTest extends FlatSpec with Matchers with SparkTest {

  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val bluefieldAlphaClient = getBlufieldClient(Some("alpha"))

  private val spaceExternalId = "test-space-scala-sdk"

  private val containerAllExternalId = "sparkDatasourceTestContainerAll5"
  private val containerNodesExternalId = "sparkDatasourceTestContainerNodes5"
  private val containerEdgesExternalId = "sparkDatasourceTestContainerEdges5"

  private val containerAllListExternalId = "sparkDatasourceTestContainerAllList5"
  private val containerNodesListExternalId = "sparkDatasourceTestContainerNodesList5"
  private val containerEdgesListExternalId = "sparkDatasourceTestContainerEdgesList5"

  private val viewAllExternalId = "sparkDatasourceTestViewAll5"
  private val viewNodesExternalId = "sparkDatasourceTestViewNodes5"
  private val viewEdgesExternalId = "sparkDatasourceTestViewEdges5"

  private val viewAllListExternalId = "sparkDatasourceTestViewAllList5"
  private val viewNodesListExternalId = "sparkDatasourceTestViewNodesList5"
  private val viewEdgesListExternalId = "sparkDatasourceTestViewEdgesList5"

  private val containerAllNumericProps = "sparkDatasourceTestContainerNumericProps1"
  private val viewAllNumericProps = "sparkDatasourceTestViewNumericProps1"

  private val containerFilterByProps = "sparkDatasourceTestContainerFilterByProps1"
  private val viewFilterByProps = "sparkDatasourceTestViewFilterByProps1"

  private val containerStartNodeAndEndNodesExternalId = "sparkDatasourceTestContainerStartAndEndNodes1"
  private val viewStartNodeAndEndNodesExternalId = "sparkDatasourceTestViewStartAndEndNodes1"

  private val viewVersion = "v1"

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

  private val startNodeExtId = s"${viewStartNodeAndEndNodesExternalId}StartNode"
  private val endNodeExtId = s"${viewStartNodeAndEndNodesExternalId}EndNode"

  createStartAndEndNodesForEdgesIfNotExists.unsafeRunSync()

  ignore should "succeed when inserting all nullable & non nullable non list values" in {
    val (viewAll, viewNodes, viewEdges) = setupAllNonListPropertyTest.unsafeRunSync()
    val randomId = generateNodeExternalId
    val instanceExtIdAll = s"${randomId}All"
    val instanceExtIdNode = s"${randomId}Node"
    val instanceExtIdEdge = s"${randomId}Edge"

    def df(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
                |select 
                |'$instanceExtId' as externalId,
                |named_struct(
                |    'space', '$spaceExternalId',
                |    'externalId', '$instanceExtId'
                |) as type,
                |named_struct(
                |    'space', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as startNode,
                |named_struct(
                |    'space', '$spaceExternalId',
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
                |'${LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)}' as dateProp1,
                |null as dateProp2,
                |'${ZonedDateTime
                  .now()
                  .format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}' as timestampProp1,
                |null as timestampProp2,
                |'{"a": "a", "b": 1}' as jsonProp1,
                |null as jsonProp2
                |""".stripMargin)

    val result = Try {
      Vector(
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          df(instanceExtIdAll)
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          df(instanceExtIdNode)
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewEdges.externalId,
          viewVersion = viewEdges.version,
          instanceSpaceExternalId = spaceExternalId,
          df(instanceExtIdEdge)
        )
      )
    }

    result shouldBe Success(Vector((), (), ()))
    result.get.size shouldBe 3
  }

  it should "succeed when inserting all nullable & non nullable list values" in {
    val (viewAll, viewNodes, viewEdges) = setupAllListPropertyTest.unsafeRunSync()
    val randomId = generateNodeExternalId
    val instanceExtIdAll = s"${randomId}All"
    val instanceExtIdNode = s"${randomId}Node"
    val instanceExtIdEdge = s"${randomId}Edge"

    def df(instanceExtId: String): DataFrame =
      spark
        .sql(s"""
                |select 
                |'$instanceExtId' as externalId,
                |named_struct(
                |    'space', '$spaceExternalId',
                |    'externalId', '$instanceExtId'
                |) as type,
                |named_struct(
                |    'space', '$spaceExternalId',
                |    'externalId', '$startNodeExtId'
                |) as startNode,
                |named_struct(
                |    'space', '$spaceExternalId',
                |    'externalId', '$endNodeExtId'
                |) as endNode,
                |array('stringListProp1Val', null, 'stringListProp2Val') as stringListProp1,
                |null as stringListProp2,
                |array(1, 2, 3) as intListProp1,
                |null as intListProp2,
                |array(101, null, 102, 103) as longListProp1,
                |null as longListProp2,
                |array(3.1, 3.2, 3.3) as floatListProp1,
                |null as floatListProp2,
                |array(null, 104.2, 104.3, 104.4) as doubleListProp1,
                |null as doubleListProp2,
                |array(true, true, false, null, false) as boolListProp1,
                |null as boolListProp2,
                |array('${LocalDate
                  .now()
                  .minusDays(5)
                  .format(DateTimeFormatter.ISO_LOCAL_DATE)}', '${LocalDate
                  .now()
                  .minusDays(10)
                  .format(DateTimeFormatter.ISO_LOCAL_DATE)}') as dateListProp1,
                |null as dateListProp2,
                |array('{"a": "a", "b": 1}', '{"a": "b", "b": 2}', '{"a": "c", "b": 3}') as jsonListProp1,
                |null as jsonListProp2,
                |array('${ZonedDateTime
                  .now()
                  .minusDays(5)
                  .format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}', '${ZonedDateTime
                  .now()
                  .minusDays(10)
                  .format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}') as timestampListProp1,
                |null as timestampListProp2
                |""".stripMargin)

    val result = Try {
      Vector(
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewAll.externalId,
          viewVersion = viewAll.version,
          instanceSpaceExternalId = spaceExternalId,
          df(instanceExtIdAll)
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewNodes.externalId,
          viewVersion = viewNodes.version,
          instanceSpaceExternalId = spaceExternalId,
          df(instanceExtIdNode)
        ),
        insertRows(
          viewSpaceExternalId = spaceExternalId,
          viewExternalId = viewEdges.externalId,
          viewVersion = viewEdges.version,
          instanceSpaceExternalId = spaceExternalId,
          df(instanceExtIdEdge)
        )
      )
    }

    result shouldBe Success(Vector((), (), ()))
    result.get.size shouldBe 3
    getUpsertedMetricsCount(viewAll) shouldBe 1
    getUpsertedMetricsCount(viewNodes) shouldBe 1
    getUpsertedMetricsCount(viewEdges) shouldBe 1
  }

  // Blocked by filter 'values' issue
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
    val row = filtered.head
    val filteredInstanceExtId = row.getString(row.schema.fieldIndex("externalId"))
    instanceExtIds.contains(filteredInstanceExtId) shouldBe true
    filteredInstanceExtId shouldBe s"${view}Node1"
  }

  // Blocked by types not returning issue
  ignore should "successfully cast numeric properties" in {
    val viewDef = setupNumericConversionTest.unsafeRunSync()
    val nodeExtId1 = s"${generateNodeExternalId}Numeric1"
    val nodeExtId2 = s"${generateNodeExternalId}Numeric2"

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
        sources = Some(Seq(InstanceSource(viewDef.toViewReference)))
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = nodeExtId2,
        space = spaceExternalId,
        sources = Some(Seq(InstanceSource(viewDef.toViewReference)))
      )
    ).traverse(i => bluefieldAlphaClient.instances.retrieveByExternalIds(Vector(i), true))
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

  ignore should "delete" in {
    bluefieldAlphaClient.containers
      .delete(Seq(
        ContainerId(spaceExternalId, containerAllExternalId),
        ContainerId(spaceExternalId, "containerAllListExternalId1"),
        ContainerId(spaceExternalId, "containerAllListExternalId11"),
        ContainerId(spaceExternalId, "containerAllListExternalId111"),
        ContainerId(spaceExternalId, "containerAllListExternalId1111"),
        ContainerId(spaceExternalId, "containerAllListExternalId11111"),
        ContainerId(spaceExternalId, "containerAllListExternalId111111"),
        ContainerId(spaceExternalId, containerNodesExternalId),
        ContainerId(spaceExternalId, containerEdgesExternalId),
        ContainerId(spaceExternalId, containerAllNumericProps),
      ))
      .unsafeRunSync()

    bluefieldAlphaClient.views
      .deleteItems(Seq(
        DataModelReference(spaceExternalId, "sparkDatasourceTestViewAllList1", viewVersion),
        DataModelReference(spaceExternalId, "sparkDatasourceTestViewAllList11", viewVersion),
        DataModelReference(spaceExternalId, "sparkDatasourceTestViewAllList111", viewVersion),
        DataModelReference(spaceExternalId, "sparkDatasourceTestViewAllList1111", viewVersion),
        DataModelReference(spaceExternalId, "sparkDatasourceTestViewAllList11111", viewVersion),
        DataModelReference(spaceExternalId, "sparkDatasourceTestViewAllList111111", viewVersion),
        DataModelReference(spaceExternalId, viewAllExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewNodesExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewEdgesExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewAllNumericProps, viewVersion),
      ))
      .unsafeRunSync()

    1 shouldBe 1
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
//      "timestampProp1" -> FDMContainerPropertyTypes.TimestampNonListWithDefaultValueNonNullable,
//      "timestampProp2" -> FDMContainerPropertyTypes.TimestampNonListWithDefaultValueNullable,
      "jsonProp1" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNonNullable,
      "jsonProp2" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNullable,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerAllExternalId)
      cNodes <- createContainerIfNotExists(Usage.Node, containerProps, containerNodesExternalId)
      cEdges <- createContainerIfNotExists(Usage.Edge, containerProps, containerEdgesExternalId)
      viewAll <- createViewIfNotExists(cAll, viewAllExternalId, viewVersion)
      viewNodes <- createViewIfNotExists(cNodes, viewNodesExternalId, viewVersion)
      viewEdges <- createViewIfNotExists(cEdges, viewEdgesExternalId, viewVersion)
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
//      "timestampListProp1" -> FDMContainerPropertyTypes.TimestampListWithoutDefaultValueNonNullable,
//      "timestampListProp2" -> FDMContainerPropertyTypes.TimestampListWithoutDefaultValueNullable,
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
      "forIsNullFilter" -> FDMContainerPropertyTypes.JsonNonListWithDefaultValueNullable,
    )

    for {
      cAll <- createContainerIfNotExists(Usage.All, containerProps, containerFilterByProps)
      viewAll <- createViewIfNotExists(cAll, viewFilterByProps, viewFilterByProps)
      extIds <- setupInstancesForFiltering(viewAll)
    } yield (viewAll, extIds)
  }

  private def setupInstancesForFiltering(view: ViewDefinition): IO[Seq[String]] = {
    val viewRef = view.toViewReference
    val viewExtId = view.externalId

    Vector(
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = s"${viewExtId}Node1",
        space = viewRef.space,
        sources = Some(Seq(InstanceSource(viewRef)))
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = s"${viewExtId}Node1",
        space = spaceExternalId,
        sources = Some(Seq(InstanceSource(viewRef)))
      )
    ).traverse { i =>
        createTestNodeInstancesForFiltering(i, viewRef)
      }
      .map(_.flatten)
  }

  // scalastyle:off method.length
  private def createTestNodeInstancesForFiltering(
      i: InstanceRetrieve,
      viewRef: ViewReference): IO[Seq[String]] =
    bluefieldAlphaClient.instances.retrieveByExternalIds(Vector(i)).map(_.items).flatMap { instances =>
      if (instances.length === 2) {
        IO.delay(instances.collect { case n: InstanceDefinition.NodeDefinition => n.externalId })
      } else {
        bluefieldAlphaClient.instances
          .createItems(
            instance = InstanceCreate(
              items = Seq(
                NodeWrite(
                  spaceExternalId,
                  s"${viewRef.externalId}Node1",
                  Seq(EdgeOrNodeData(
                    viewRef,
                    Some(Map(
                      "forEqualsFilter" -> InstancePropertyValue.String("str1"),
                      "forInFilter" -> InstancePropertyValue.String("str2"),
                      "forGteFilter" -> InstancePropertyValue.Int32(1),
                      "forGtFilter" -> InstancePropertyValue.Int32(2),
                      "forLteFilter" -> InstancePropertyValue.Int64(2),
                      "forLtFilter" -> InstancePropertyValue.Int64(3),
                      "forOrFilter1" -> InstancePropertyValue.Float64(5.1),
                      "forOrFilter2" -> InstancePropertyValue.Float64(6.1),
                      "forIsNotNullFilter" -> InstancePropertyValue.Date(LocalDate.now())
                    ))
                  ))
                ),
                NodeWrite(
                  spaceExternalId,
                  s"${viewRef.externalId}Node2",
                  Seq(EdgeOrNodeData(
                    viewRef,
                    Some(Map(
                      "forEqualsFilter" -> InstancePropertyValue.String("str2"),
                      "forInFilter" -> InstancePropertyValue.String("str2"),
                      "forGteFilter" -> InstancePropertyValue.Int32(1),
                      "forGtFilter" -> InstancePropertyValue.Int32(2),
                      "forLteFilter" -> InstancePropertyValue.Int64(2),
                      "forLtFilter" -> InstancePropertyValue.Int64(3),
                      "forOrFilter1" -> InstancePropertyValue.Float64(5.1),
                      "forOrFilter2" -> InstancePropertyValue.Float64(6.1),
                      "forIsNotNullFilter" -> InstancePropertyValue.Date(LocalDate.now()),
                      "forIsNullFilter" -> InstancePropertyValue.Object(Json.fromJsonObject(
                        JsonObject("a" -> Json.fromString("a"), "b" -> Json.fromInt(1))))
                    ))
                  ))
                )
              ),
              replace = Some(true)
            )
          )
          .map(_.collect { case n: SlimNodeOrEdge.SlimNodeDefinition => n.externalId })
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
      .option("type", FlexibleDataModelsRelation.ResourceType)
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
      .option("type", FlexibleDataModelsRelation.ResourceType)
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
    bluefieldAlphaClient.containers
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
          bluefieldAlphaClient.containers.createItems(containers = Seq(containerToCreate))
        } else {
          IO.delay(containers)
        }
      }
      .map(_.head)

  private def createViewIfNotExists(
      container: ContainerDefinition,
      viewExternalId: String,
      viewVersion: String): IO[ViewDefinition] =
    bluefieldAlphaClient.views
      .retrieveItems(items = Seq(DataModelReference(spaceExternalId, viewExternalId, viewVersion)))
      .flatMap { views =>
        if (views.isEmpty) {
          val containerRef = container.toContainerReference
          val viewToCreate = ViewCreateDefinition(
            space = spaceExternalId,
            externalId = viewExternalId,
            version = Some(viewVersion),
            name = Some(s"Test-View-Spark-DS"),
            description = Some("Test View For Spark Datasource"),
            filter = None,
            properties = container.properties.map {
              case (pName, _) => pName -> CreatePropertyReference(containerRef, pName)
            },
            implements = None,
          )

          bluefieldAlphaClient.views.createItems(items = Seq(viewToCreate))
        } else {
          IO.delay(views)
        }
      }
      .map(_.head)

  // scalastyle:off method.length
  private def createStartAndEndNodesForEdgesIfNotExists: IO[Unit] =
    // TODO: Move to a single call after they fixed 501
    Vector(
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = startNodeExtId,
        space = spaceExternalId,
        sources = Some(Seq(InstanceSource(viewStartAndEndNodes.toViewReference)))
      ),
      InstanceRetrieve(
        instanceType = InstanceType.Node,
        externalId = endNodeExtId,
        space = spaceExternalId,
        sources = Some(Seq(InstanceSource(viewStartAndEndNodes.toViewReference)))
      )
    ).traverse(i => bluefieldAlphaClient.instances.retrieveByExternalIds(Vector(i), false))
      .flatMap { response =>
        val nodes = response.flatMap(_.items).collect {
          case n: InstanceDefinition.NodeDefinition =>
            n
        }
        if (nodes.size === 2) {
          IO.unit
        } else {
          bluefieldAlphaClient.instances
            .createItems(instance = InstanceCreate(
              items = Seq(
                NodeWrite(
                  spaceExternalId,
                  startNodeExtId,
                  Seq(EdgeOrNodeData(
                    viewStartAndEndNodes.toViewReference,
                    Some(Map(
                      "stringProp1" -> InstancePropertyValue.String("stringProp1Val"),
                      "stringProp2" -> InstancePropertyValue.String("stringProp2Val")))
                  ))
                ),
                NodeWrite(
                  spaceExternalId,
                  endNodeExtId,
                  Seq(EdgeOrNodeData(
                    viewStartAndEndNodes.toViewReference,
                    Some(Map(
                      "stringProp1" -> InstancePropertyValue.String("stringProp1Val"),
                      "stringProp2" -> InstancePropertyValue.String("stringProp2Val")))
                  ))
                )
              ),
              replace = Some(true)
            )) *> IO.unit
        }
      }
  // scalastyle:off method.length

  private def apiCompatibleRandomString(): String =
    UUID.randomUUID().toString.replaceAll("_|-|x|0", "").substring(0, 5)

  private def generateNodeExternalId: String = s"randomId${apiCompatibleRandomString()}"

  private def getUpsertedMetricsCount(viewDef: ViewDefinition) =
    getNumberOfRowsUpserted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelsRelation.ResourceType)
}
