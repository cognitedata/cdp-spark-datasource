package cognite.spark.v1.fdm.utils

import cats.effect.IO
import cats.{Applicative, Apply}
import cognite.spark.v1.fdm.utils.FDMTestConstants._
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{ContainerPropertyDefinition, ViewCorePropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{ListablePropertyType, PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.containers.{ContainerCreateDefinition, ContainerDefinition, ContainerId}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeOrNodeData, InstanceCreate, InstancePropertyValue}
import com.cognite.sdk.scala.v1.fdm.views.{ViewCreateDefinition, ViewDefinition, ViewPropertyCreateDefinition}
import com.cognite.sdk.scala.v1.{SpaceById, SpaceCreateDefinition}
import io.circe.{Json, JsonObject}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import scala.util.Random

trait FDMTestInitializer {

  protected def createInstancePropertyValue(
    propName: String,
    propType: PropertyType,
    directNodeReference: DirectRelationReference
  ): Option[InstancePropertyValue] =
    propType match {
      case d: DirectNodeRelationProperty =>
        val ref = d.container.map(_ => directNodeReference)
        Some(InstancePropertyValue.ViewDirectNodeRelation(value = ref))
      case p: ListablePropertyType if p.isList =>
        Some(listContainerPropToInstanceProperty(propName, p))
      case p =>
        Some(nonListContainerPropToInstanceProperty(propName, p))
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

  protected def createTestInstancesForView(
      viewDef: ViewDefinition,
      directNodeReference: DirectRelationReference,
      typeNode: Option[DirectRelationReference],
      startNode: Option[DirectRelationReference],
      endNode: Option[DirectRelationReference]
  ): IO[Seq[String]] = {
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
  }

  protected def createEdgeWriteInstances(
    viewDef: ViewDefinition,
    typeNode: DirectRelationReference,
    startNode: DirectRelationReference,
    endNode: DirectRelationReference,
    directNodeReference: DirectRelationReference,
    randomPrefix: String
  ): Seq[EdgeWrite] = {
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
    randomPrefix: String
  ): Seq[NodeWrite] = {
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
    containerExternalId: String
  ): IO[ContainerDefinition] =
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
        } else {
          IO.delay(containers)
        }
      }
      .map(_.head)


  protected def createViewWithCorePropsIfNotExists(
    container: ContainerDefinition,
    viewExternalId: String,
    viewVersion: String
  ): IO[ViewDefinition] =
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
                //this property also named type is a real life possibility and needs to be allowed
                Some(Map("type" -> Some(InstancePropertyValue.ViewDirectNodeRelation(Some(DirectRelationReference(spaceExternalId, typeNodeExtId))))))
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
                Some(Map("type" -> Some(InstancePropertyValue.ViewDirectNodeRelation(Some(DirectRelationReference(spaceExternalId, typeNodeExtId))))))
              ))
            ),
            `type` = Some(DirectRelationReference(spaceExternalId, typeNodeExtId))
          )
        ),
        replace = Some(true)
      )) *> IO.unit

  // scalastyle:off method.length
  protected def createStartAndEndNodesForEdgesIfNotExists(
    startNodeExtId: String,
    endNodeExtId: String,
    sourceReference: SourceReference
  ): IO[Unit] =
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
      )) *> IO.unit

  protected def createNodesForEdgesIfNotExists(
    startNodeExtId: String,
    endNodeExtId: String,
    sourceReference: SourceReference
  ): IO[Unit] =
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
      )) *> IO.unit
}
