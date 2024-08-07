package cognite.spark.v1

import cats.{Applicative, Apply}
import cats.effect.IO
import com.cognite.sdk.scala.v1.{GenericClient, SpaceById, SpaceCreateDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ContainerPropertyDefinition,
  ViewCorePropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.containers.{
  ContainerCreateDefinition,
  ContainerDefinition,
  ContainerId
}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.util.Random

trait FlexibleDataModelsTestBase extends FlatSpec with Matchers with SparkTest {

  protected val clientId = sys.env("TEST_CLIENT_ID")
  protected val clientSecret = sys.env("TEST_CLIENT_SECRET")
  protected val cluster = sys.env("TEST_CLUSTER")
  protected val project = sys.env("TEST_PROJECT")
  protected val tokenUri: String = sys.env
    .get("TEST_TOKEN_URL")
    .orElse(
      sys.env
        .get("TEST_AAD_TENANT")
        .map(tenant => s"https://login.microsoftonline.com/$tenant/oauth2/v2.0/token"))
    .getOrElse("https://sometokenurl")
  protected val audience = s"https://${cluster}.cognitedata.com"
  protected val client: GenericClient[IO] = getTestClient()

  protected val spaceExternalId = "testSpaceForSparkDatasource"

  protected val viewVersion = "v1"

  protected def createTestInstancesForView(
      viewDef: ViewDefinition,
      directNodeReference: DirectRelationReference,
      typeNode: Option[DirectRelationReference],
      startNode: Option[DirectRelationReference],
      endNode: Option[DirectRelationReference]): IO[Seq[String]] = {
    val randomPrefix = apiCompatibleRandomString()
    val writeData = viewDef.usedFor match {
      case Usage.Node =>
        createNodeWriteInstances(viewDef, directNodeReference, None, randomPrefix)
      case Usage.Edge =>
        Apply[Option].map3(typeNode, startNode, endNode)(Tuple3.apply).toSeq.flatMap {
          case (t, s, e) =>
            createEdgeWriteInstances(
              viewDef,
              typeNode = t,
              startNode = s,
              endNode = e,
              directNodeReference,
              randomPrefix)
        }
      case Usage.All =>
        createNodeWriteInstances(viewDef, directNodeReference, None, randomPrefix) ++ Apply[Option]
          .map3(typeNode, startNode, endNode)(Tuple3.apply)
          .toSeq
          .flatMap {
            case (t, s, e) =>
              createEdgeWriteInstances(
                viewDef,
                typeNode = t,
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

  protected def createEdgeWriteInstances(
      viewDef: ViewDefinition,
      typeNode: DirectRelationReference,
      startNode: DirectRelationReference,
      endNode: DirectRelationReference,
      directNodeReference: DirectRelationReference,
      randomPrefix: String): Seq[EdgeWrite] = {
    val viewRef = viewDef.toSourceReference
    val edgeExternalIdPrefix = s"${viewDef.externalId}${randomPrefix}Edge"
    Seq(
      EdgeWrite(
        `type` = typeNode,
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
        `type` = typeNode,
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

  protected def createNodeWriteInstances(
      viewDef: ViewDefinition,
      directNodeReference: DirectRelationReference,
      typeNode: Option[DirectRelationReference],
      randomPrefix: String): Seq[NodeWrite] = {
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
        ),
        `type` = typeNode
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
        ),
        `type` = typeNode
      )
    )
  }

  protected def createSpaceIfNotExists(space: String): IO[Unit] =
    for {
      existing <- client.spacesv3.retrieveItems(Seq(SpaceById(space)))
      _ <- Applicative[IO].whenA(existing.isEmpty) {
        client.spacesv3.createItems(
          Seq(
            SpaceCreateDefinition(space)
          ))
      }
    } yield ()

  protected def createContainerIfNotExists(
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

  protected def createViewWithCorePropsIfNotExists(
      container: ContainerDefinition,
      viewExternalId: String,
      viewVersion: String): IO[ViewDefinition] =
    client.views
      .retrieveItems(items = Seq(DataModelReference(spaceExternalId, viewExternalId, Some(viewVersion))))
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

  protected def createTypedNodesIfNotExists(
      typedNodeNullTypeExtId: String,
      typedNodeExtId: String,
      typeNodeExtId: String,
      typeNodeSourceReference: SourceReference,
      sourceReference: SourceReference
  ): IO[Unit] =
    client.instances
      .createItems(instance = InstanceCreate(
        items = Seq(
          NodeWrite(
            spaceExternalId,
            typeNodeExtId,
            Some(
              Seq(EdgeOrNodeData(
                typeNodeSourceReference,
                None
              ))
            ),
            `type` = None
          ),
          NodeWrite(
            spaceExternalId,
            typedNodeNullTypeExtId,
            Some(
              Seq(EdgeOrNodeData(
                sourceReference,
                None
              ))
            ),
            `type` = None
          ),
          NodeWrite(
            spaceExternalId,
            typedNodeExtId,
            Some(
              Seq(EdgeOrNodeData(
                sourceReference,
                None
              ))
            ),
            `type` = Some(DirectRelationReference(spaceExternalId, typeNodeExtId))
          )
        ),
        replace = Some(true)
      ))
      .flatTap(_ => IO.sleep(3.seconds)) *> IO.unit
  // scalastyle:off method.length
  protected def createStartAndEndNodesForEdgesIfNotExists(
      startNodeExtId: String,
      endNodeExtId: String,
      sourceReference: SourceReference): IO[Unit] =
    client.instances
      .createItems(instance = InstanceCreate(
        items = Seq(
          NodeWrite(
            spaceExternalId,
            startNodeExtId,
            Some(
              Seq(EdgeOrNodeData(
                sourceReference,
                Some(Map(
                  "stringProp1" -> Some(InstancePropertyValue.String("stringProp1Val")),
                  "stringProp2" -> Some(InstancePropertyValue.String("stringProp2Val"))))
              ))
            ),
            `type` = None
          ),
          NodeWrite(
            spaceExternalId,
            endNodeExtId,
            Some(
              Seq(EdgeOrNodeData(
                sourceReference,
                Some(Map(
                  "stringProp1" -> Some(InstancePropertyValue.String("stringProp1Val")),
                  "stringProp2" -> Some(InstancePropertyValue.String("stringProp2Val"))))
              ))
            ),
            `type` = None
          )
        ),
        replace = Some(true)
      ))
      .flatTap(_ => IO.sleep(3.seconds)) *> IO.unit

  protected def createNodesForEdgesIfNotExists(
      startNodeExtId: String,
      endNodeExtId: String,
      sourceReference: SourceReference): IO[Unit] =
    client.instances
      .createItems(instance = InstanceCreate(
        items = Seq(
          NodeWrite(
            spaceExternalId,
            startNodeExtId,
            Some(
              Seq(EdgeOrNodeData(
                sourceReference,
                Some(Map("stringProp1" -> Some(InstancePropertyValue.String("stringProp1StartNode"))))
              ))
            ),
            `type` = None
          ),
          NodeWrite(
            spaceExternalId,
            endNodeExtId,
            Some(
              Seq(EdgeOrNodeData(
                sourceReference,
                Some(Map("stringProp1" -> Some(InstancePropertyValue.String("stringProp1EndNode"))))
              ))
            ),
            `type` = None
          )
        ),
        replace = Some(true)
      ))
      .flatTap(_ => IO.sleep(3.seconds)) *> IO.unit


  protected def apiCompatibleRandomString(): String =
    UUID.randomUUID().toString.replaceAll("[_\\-x0]", "").substring(0, 5)

  protected def generateNodeExternalId: String = s"randomId${apiCompatibleRandomString()}"

  protected def getUpsertedMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsUpserted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  protected def getReadMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsRead(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  protected def getDeletedMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsDeleted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  protected def createInstancePropertyValue(
      propName: String,
      propType: PropertyType,
      directNodeReference: DirectRelationReference
  ): Option[InstancePropertyValue] =
    propType match {
      case d: DirectNodeRelationProperty =>
        val ref = d.container.map(_ => directNodeReference)
        Some(InstancePropertyValue.ViewDirectNodeRelation(value = ref))
      case p =>
        if (p.isList) {
          Some(listContainerPropToInstanceProperty(propName, p))
        } else {
          Some(nonListContainerPropToInstanceProperty(propName, p))
        }
    }

  protected def listContainerPropToInstanceProperty(
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

  protected def nonListContainerPropToInstanceProperty(
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
      case _: PropertyType.TimeSeriesReference =>
        InstancePropertyValue.TimeSeriesReference("timeseriesExtId1")
      case _: PropertyType.FileReference => InstancePropertyValue.FileReference("fileExtId1")
      case _: PropertyType.SequenceReference => InstancePropertyValue.SequenceReference("sequenceExtId1")
      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }

  def toExternalIds(rows: Array[Row]): Array[String] =
    rows.map(row => row.getString(row.schema.fieldIndex("externalId")))

  def toPropVal(rows: Array[Row], prop: String): Array[String] =
    rows.map(row => row.getString(row.schema.fieldIndex(prop)))

}
