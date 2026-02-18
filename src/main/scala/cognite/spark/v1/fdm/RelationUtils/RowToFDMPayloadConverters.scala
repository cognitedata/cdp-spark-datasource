package cognite.spark.v1.fdm.RelationUtils

import cats.implicits._
import cognite.spark.v1.CdfSparkException
import cognite.spark.v1.fdm.RelationUtils.RowDataExtractors.{extractEdgeEndNodeDirectRelation, extractEdgeStartNodeDirectRelation, extractEdgeTypeDirectRelation, extractExternalId, extractInstancePropertyValues, extractNodeTypeDirectRelation, extractSpaceOrDefault, rowToString, toAndFilter}
import cognite.spark.v1.fdm.RelationUtils.Validators.validateSourceSchema
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition.HasData
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition._
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.{EdgeDeletionRequest, NodeDeletionRequest}
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeOrNodeData, InstanceDeletionRequest, InstancePropertyValue, InstanceType, NodeOrEdgeCreate, SourceSelector}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object RowToFDMPayloadConverters {
  private[spark] def createNodes(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      source: Option[SourceReference],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean = true): Either[CdfSparkException, Vector[NodeWrite]] =
    validateSourceSchema(source, schema, propertyDefMap) *> createNodeWriteData(
      schema,
      source,
      propertyDefMap,
      rows,
      instanceSpace,
      ignoreNullFields)

  private[spark] def createEdges(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      source: Option[SourceReference],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean = true): Either[CdfSparkException, Vector[EdgeWrite]] =
    validateSourceSchema(source, schema, propertyDefMap) *> createEdgeWriteData(
      schema,
      source,
      propertyDefMap,
      rows,
      instanceSpace,
      ignoreNullFields)

  private[spark] def createNodesOrEdges(
      rows: Seq[Row],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition],
      source: Option[SourceReference],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean = true
  ): Either[CdfSparkException, Vector[NodeOrEdgeCreate]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(
          propertyDefMap,
          schema,
          instanceSpace.orElse(Some(space)),
          ignoreNullFields,
          row)
        writeData <- createNodeOrEdgeWriteData(
          externalId = externalId,
          instanceSpace = space,
          source,
          nodeTypeDirectRelation =
            extractNodeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          edgeTypeDirectRelation =
            extractEdgeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          startNodeRelation =
            extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          endNodeRelation =
            extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption,
          props,
          row
        )
      } yield writeData
    }

  private[spark] def createConnectionInstances(
      edgeType: DirectRelationReference,
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[EdgeWrite]] =
    createConnectionInstanceWriteData(
      schema,
      edgeType,
      rows,
      instanceSpace
    )

  private[spark] def createNodeDeleteData(
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[InstanceDeletionRequest]] =
    rows.toVector.traverse { row =>
      for {
        spaceExtId <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
      } yield NodeDeletionRequest(space = spaceExtId, externalId = extId)
    }

  private[spark] def createEdgeDeleteData(
      schema: StructType,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[InstanceDeletionRequest]] =
    rows.toVector.traverse { row =>
      for {
        spaceExtId <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
      } yield
        EdgeDeletionRequest(
          space = spaceExtId,
          externalId = extId
        )
    }

  private def createEdgeWriteData(
      schema: StructType,
      source: Option[SourceReference],
      propertyDefMap: Map[String, ViewPropertyDefinition],
      rows: Seq[Row],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean): Either[CdfSparkException, Vector[EdgeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
        edgeType <- extractEdgeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        startNode <- extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        endNode <- extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        props <- extractInstancePropertyValues(
          propertyDefMap,
          schema,
          instanceSpace.orElse(Some(space)),
          ignoreNullFields,
          row)
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = space,
          externalId = extId,
          startNode = startNode,
          endNode = endNode,
          sources = source.map(
            src =>
              Seq(
                EdgeOrNodeData(
                  source = src,
                  properties = Some(props.toMap)
                )
            ))
        )
    }

  private def createNodeOrEdgeWriteData(
      externalId: String,
      instanceSpace: String,
      source: Option[SourceReference],
      nodeTypeDirectRelation: Option[DirectRelationReference],
      edgeTypeDirectRelation: Option[DirectRelationReference],
      startNodeRelation: Option[DirectRelationReference],
      endNodeRelation: Option[DirectRelationReference],
      props: Vector[(String, Option[InstancePropertyValue])],
      row: Row): Either[CdfSparkException, NodeOrEdgeCreate] =
    (edgeTypeDirectRelation, startNodeRelation, endNodeRelation) match {
      case (Some(edgeType), Some(startNode), Some(endNode)) =>
        Right(
          EdgeWrite(
            `type` = edgeType,
            space = instanceSpace,
            externalId = externalId,
            startNode = startNode,
            endNode = endNode,
            sources = source.map(
              src =>
                Seq(
                  EdgeOrNodeData(
                    source = src,
                    properties = Some(props.toMap)
                  )
              ))
          )
        )
      case (_, None, None) =>
        Right(
          NodeWrite(
            space = instanceSpace,
            externalId = externalId,
            sources = source.map(
              src =>
                Seq(
                  EdgeOrNodeData(
                    source = src,
                    properties = Some(props.toMap)
                  )
              )),
            `type` = nodeTypeDirectRelation
          )
        )
      case _ =>
        val relationRefNames = Vector(
          edgeTypeDirectRelation.map(_ => "'type'"),
          startNodeRelation.map(_ => "'startNode'"),
          endNodeRelation.map(_ => "'endNode'")
        ).flatten
        Left(new CdfSparkException(s"""
          |Fields 'type', 'externalId', 'startNode' & 'endNode' fields are required to create an Edge.
          |Field 'externalId' is required to create a Node
          |Only found: 'externalId', ${relationRefNames.mkString(", ")}
          |in data row: ${rowToString(row)}
          |""".stripMargin))
    }

  private def createNodeWriteData(
      schema: StructType,
      source: Option[SourceReference],
      propertyDefMap: Map[String, ViewPropertyDefinition],
      rows: Seq[Row],
      instanceSpace: Option[String],
      ignoreNullFields: Boolean): Either[CdfSparkException, Vector[NodeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        externalId <- extractExternalId(schema, row)
        props <- extractInstancePropertyValues(
          propertyDefMap,
          schema,
          instanceSpace.orElse(Some(space)),
          ignoreNullFields,
          row)
      } yield
        NodeWrite(
          space = space,
          externalId = externalId,
          sources = source.map(
            src =>
              Seq(
                EdgeOrNodeData(
                  source = src,
                  properties = Some(props.toMap)
                )
            )),
          `type` = extractNodeTypeDirectRelation(schema, instanceSpace.orElse(Some(space)), row).toOption
        )
    }

  private def createConnectionInstanceWriteData(
      schema: StructType,
      edgeType: DirectRelationReference,
      rows: Seq[Row],
      instanceSpace: Option[String]): Either[CdfSparkException, Vector[EdgeWrite]] =
    rows.toVector.traverse { row =>
      for {
        space <- extractSpaceOrDefault(schema, row, instanceSpace)
        extId <- extractExternalId(schema, row)
        startNode <- extractEdgeStartNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
        endNode <- extractEdgeEndNodeDirectRelation(schema, instanceSpace.orElse(Some(space)), row)
      } yield
        EdgeWrite(
          `type` = edgeType,
          space = space,
          externalId = extId,
          startNode = startNode,
          endNode = endNode,
          sources = None
        )
    }
  def queryFilterWithHasData(
    instanceFilter: Option[FilterDefinition],
    viewReference: Option[ViewReference]): Option[FilterDefinition] =
    toAndFilter(
      viewReference.map(ref => HasData(Seq(ref))).toVector ++ instanceFilter.toVector
    )

  private def reservedPropertyNames(instanceType: InstanceType): Seq[String] = {
    val result = Seq("space", "externalId", "_type")
    instanceType match {
      case InstanceType.Node => result
      case InstanceType.Edge => result ++ Seq("startNode", "endNode", "type")
    }
  }

  def sourceReference(
    instanceType: InstanceType,
    viewReference: Option[ViewReference],
    selectedInstanceProps: Array[String]): Seq[SourceSelector] =
    viewReference
      .map(
        r =>
          SourceSelector(
            source = r,
            properties = selectedInstanceProps.toIndexedSeq.filter(p =>
              !p.startsWith("node.") && !p.startsWith("edge.") && !p
                .startsWith("metadata.") && !reservedPropertyNames(instanceType).contains(p))
          ))
      .toSeq
}
