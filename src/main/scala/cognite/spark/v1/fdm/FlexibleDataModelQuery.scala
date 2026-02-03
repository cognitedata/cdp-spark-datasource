package cognite.spark.v1.fdm

import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeTableExpression, InstanceType, NodesTableExpression, TableExpression}

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
}
