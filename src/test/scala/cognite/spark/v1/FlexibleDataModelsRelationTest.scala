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
import com.cognite.sdk.scala.v1.fdm.instances.SlimNodeOrEdge.SlimNodeDefinition
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.util.Try

class FlexibleDataModelsRelationTest extends FlatSpec with Matchers with SparkTest {

  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val bluefieldAlphaClient = getBlufieldClient(Some("alpha"))

  private val spaceExternalId = "test-space-scala-sdk"

  private val containerAllExternalId = "sparkDatasourceTestContainerAll1"
  private val containerNodesExternalId = "sparkDatasourceTestContainerNodes1"
  private val containerEdgesExternalId = "sparkDatasourceTestContainerEdges1"

  private val viewAllExternalId = "sparkDatasourceTestViewAll1"
  private val viewNodesExternalId = "sparkDatasourceTestViewNodes1"
  private val viewEdgesExternalId = "sparkDatasourceTestViewEdges1"

  private val containerAllNumericProps = "sparkDatasourceTestContainerNumericProps1"
  private val viewAllNumericProps = "sparkDatasourceTestViewNumericProps1"

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

  it should "succeed when inserting all nullable & non nullable non list values" in {
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

    result.isSuccess shouldBe true
    result.get.size shouldBe 3
  }

  it should "successfully cast numeric properties" in {
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
              |4.3 as doubleProp
              |
              |union all
              |
              |select
              |'$nodeExtId2' as externalId,
              |null as intProp,
              |null as longProp,
              |null as floatProp,
              |null as doubleProp
              |""".stripMargin)

    val result = Try {
      insertRows(
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = viewDef.externalId,
        viewVersion = viewDef.version,
        instanceSpaceExternalId = spaceExternalId,
        df
      )
    }

    result.isSuccess shouldBe true

    val propertyMapForInstances = (IO.sleep(5.seconds) *> Vector(
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
    ).traverse(i => bluefieldAlphaClient.instances.retrieveByExternalIds(Vector(i), false))
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
  }

  ignore should "delete" in {
    bluefieldAlphaClient.containers
      .delete(Seq(
        ContainerId(spaceExternalId, containerAllExternalId),
        ContainerId(spaceExternalId, containerNodesExternalId),
        ContainerId(spaceExternalId, containerEdgesExternalId),
      ))
      .unsafeRunSync()

    bluefieldAlphaClient.views
      .deleteItems(Seq(
        DataModelReference(spaceExternalId, viewAllExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewNodesExternalId, viewVersion),
        DataModelReference(spaceExternalId, viewEdgesExternalId, viewVersion),
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

  private def setupNumericConversionTest: IO[ViewDefinition] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "intProp" -> FDMContainerPropertyTypes.Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "longProp" -> FDMContainerPropertyTypes.Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
      "floatProp" -> FDMContainerPropertyTypes.Float32NonListWithoutDefaultValueNullable,
      "doubleProp" -> FDMContainerPropertyTypes.Float64NonListWithoutDefaultValueNullable,
    )

    for {
      container <- createContainerIfNotExists(Usage.All, containerProps, containerAllNumericProps)
      view <- createViewIfNotExists(container, viewEdgesExternalId, viewVersion)
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

}
