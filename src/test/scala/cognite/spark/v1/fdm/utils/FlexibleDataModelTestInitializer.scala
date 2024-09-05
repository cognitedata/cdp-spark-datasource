package cognite.spark.v1.fdm.utils

import cats.effect.IO
import cats.{Applicative, Apply}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{ContainerPropertyDefinition, ViewCorePropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.common.{DataModelReference, DirectRelationReference, Usage}
import com.cognite.sdk.scala.v1.fdm.containers.{ContainerCreateDefinition, ContainerDefinition, ContainerId}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeOrNodeData, InstanceCreate, InstancePropertyValue}
import com.cognite.sdk.scala.v1.fdm.views.{ViewCreateDefinition, ViewDefinition, ViewPropertyCreateDefinition}
import com.cognite.sdk.scala.v1.{SpaceById, SpaceCreateDefinition}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

trait FlexibleDataModelTestInitializer extends FlexibleDataModelTestBase {
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
}
