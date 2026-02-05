package cognite.spark.v1.fdm

import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.instances.{
  EdgeTableExpression,
  InstanceType,
  NodesTableExpression,
  SourceSelector,
  TableExpression
}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference

object FlexibleDataModelQuery {
  def generateTableExpression(
      instanceType: InstanceType,
      filters: Option[FilterDefinition],
      limit: Option[Int] = Some(1000)): TableExpression =
    instanceType match {
      case InstanceType.Edge =>
        TableExpression(edges = Some(EdgeTableExpression(filter = filters)), limit = limit)
      case InstanceType.Node =>
        TableExpression(nodes = Some(NodesTableExpression(filter = filters)), limit = limit)
    }

  def reservedPropertyNames(instanceType: InstanceType): Seq[String] = {
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
