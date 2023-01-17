package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cognite.spark.v1.utils.fdm.FDMContainerPropertyTypes
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.containers.{
  ContainerCreateDefinition,
  ContainerDefinition,
  ContainerId
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances.{
  EdgeOrNodeData,
  InstanceCreate,
  InstancePropertyValue,
  InstanceRetrieve,
  InstanceSource,
  InstanceType
}
import com.cognite.sdk.scala.v1.fdm.views.{
  CreatePropertyReference,
  DataModelReference,
  ViewCreateDefinition,
  ViewDefinition,
  ViewReference
}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

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

  private val viewVersion = "v1"

  val containerProps: Map[String, ContainerPropertyDefinition] = Map(
    "stringProp" -> FDMContainerPropertyTypes.TextPropertyNonListWithoutAutoIncrementWithDefaultValueNonNullable,
    "longProp" -> FDMContainerPropertyTypes.Int64NonListWithoutAutoIncrementWithDefaultValueNullable,
  )

//  bluefieldAlphaClient.containers
//    .delete(
//      Seq(
//        ContainerId(spaceExternalId, "sparkDatasourceTestContainer1"),
//        ContainerId(spaceExternalId, "sparkDatasourceTestContainer2")
//      ))
//    .unsafeRunSync()
//
//  bluefieldAlphaClient.views
//    .deleteItems(
//      Seq(
//        DataModelReference(spaceExternalId, "sparkDatasourceTestView1", "v1"),
//        DataModelReference(spaceExternalId, "sparkDatasourceTestView2", "v1")
//      ))
//    .unsafeRunSync()

  private val containerAll: ContainerDefinition =
    createContainer(Usage.All, containerProps, containerAllExternalId)
  private val containerNodes: ContainerDefinition =
    createContainer(Usage.Node, containerProps, containerNodesExternalId)
  private val containerEdges: ContainerDefinition =
    createContainer(Usage.Edge, containerProps, containerEdgesExternalId)

  private val viewAll: ViewDefinition = createView(containerAll, viewAllExternalId)
  private val viewNodes: ViewDefinition = createView(containerNodes, viewNodesExternalId)
  private val viewEdges: ViewDefinition = createView(containerEdges, viewEdgesExternalId)

  private def generateNodeExternalId: String = s"node_ext_id_${shortRandomString()}"

  ignore should "pass2" in {
    val i1 = InstanceCreate(
      Vector(
        NodeWrite(
          "test-space-scala-sdk",
          "node_ext_id_01a86e9c1",
          List(EdgeOrNodeData(
            ViewReference("test-space-scala-sdk", "sparkDatasourceTestView1", "v1"),
            Some(Map(
              "longProp" -> InstancePropertyValue.Int64(1010),
              "stringProp" -> InstancePropertyValue.String("stringProp1")))
          ))
        )),
      Some(false),
      Some(false),
      Some(true)
    )

    val i2 = InstanceCreate(
      Vector(
        NodeWrite(
          "test-space-scala-sdk",
          "node_ext_id_370d3b142",
          List(EdgeOrNodeData(
            ViewReference("test-space-scala-sdk", "sparkDatasourceTestView1", "v1"),
            Some(Map(
              "longProp" -> InstancePropertyValue.Int64(1011),
              "stringProp" -> InstancePropertyValue.String("stringProp2")))
          ))
        )),
      Some(false),
      Some(false),
      Some(true)
    )

    val r1 = bluefieldAlphaClient.instances.createItems(i1).unsafeRunSync()
    val r2 = bluefieldAlphaClient.instances.createItems(i2).unsafeRunSync()

    println(s"$r1, $r2")

    val retrieved = bluefieldAlphaClient.instances
      .retrieveByExternalIds(
        items = Seq(
          InstanceRetrieve(
            instanceType = InstanceType.Node,
            externalId = "node_ext_id_01a86e9c1",
            space = spaceExternalId,
            sources = Some(Seq(
              InstanceSource(ViewReference("test-space-scala-sdk", "sparkDatasourceTestView1", "v1"))))
          ),
          InstanceRetrieve(
            instanceType = InstanceType.Node,
            externalId = "node_ext_id_370d3b142",
            space = spaceExternalId,
            sources = Some(Seq(
              InstanceSource(ViewReference("test-space-scala-sdk", "sparkDatasourceTestView1", "v1"))))
          ),
        )
      )
      .unsafeRunSync()
      .items

    retrieved.length shouldBe 2
  }

  ignore should "pass" in {
    val nodeExtId1 = s"${generateNodeExternalId}1"
    val nodeExtId2 = s"${generateNodeExternalId}2"

    val df = spark
      .sql(s"""
              |select 
              |'$nodeExtId1' as externalId,
              |1010 as longProp,
              |'stringProp1' as stringProp
              |union all
              |select 
              |'$nodeExtId2' as externalId,
              |1011 as longProp,
              |'stringProp2' as stringProp
              |""".stripMargin)

    insertRows(
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = viewAll.externalId,
      viewVersion = viewAll.version,
      instanceSpaceExternalId = spaceExternalId,
      df
    )

    val instances = (IO.sleep(5.seconds) *> bluefieldAlphaClient.instances
      .retrieveByExternalIds(
        items = Seq(
          InstanceRetrieve(
            instanceType = InstanceType.Node,
            externalId = nodeExtId1,
            space = spaceExternalId,
            sources = Some(Seq(InstanceSource(viewAll.toViewReference)))),
          InstanceRetrieve(
            instanceType = InstanceType.Node,
            externalId = nodeExtId2,
            space = spaceExternalId,
            sources = Some(Seq(InstanceSource(viewAll.toViewReference)))),
        )
      ))
      .unsafeRunSync()
      .items

    instances.size shouldBe 1
  }

  it should "fail when property has a wrong type" in {
    val nodeExtId1 = s"${generateNodeExternalId}1"
    val nodeExtId2 = s"${generateNodeExternalId}2"

    val df = spark
      .sql(s"""
              |select 
              |'$nodeExtId1' as externalId,
              |1010 as longProp,
              |'stringProp1' as stringProp
              |union all
              |select 
              |'$nodeExtId2' as externalId,
              |1011.5 as longProp,
              |'stringProp2' as stringProp
              |""".stripMargin)

    val result = Try {
      insertRows(
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = viewAll.externalId,
        viewVersion = viewAll.version,
        instanceSpaceExternalId = spaceExternalId,
        df
      )
    }

    result.isSuccess shouldBe false
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

  private def createContainer(
      usage: Usage,
      properties: Map[String, ContainerPropertyDefinition],
      containerExternalId: String): ContainerDefinition = {
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

    bluefieldAlphaClient.containers.createItems(containers = Seq(containerToCreate)).unsafeRunSync().head
  }

  private def createView(container: ContainerDefinition, viewExternalId: String): ViewDefinition = {
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

    bluefieldAlphaClient.views.createItems(items = Seq(viewToCreate)).unsafeRunSync().head
  }
}
