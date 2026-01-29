package cognite.spark.v1.fdm

import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeTableExpression, InstanceType, NodesTableExpression, TableExpression}

object FlexibleDataModelQuery {
  def generateTableExpression(
    instanceType: InstanceType,
    filters: FilterDefinition): TableExpression =
    instanceType match {
      case InstanceType.Edge =>
        TableExpression(edges = Some(EdgeTableExpression(filter = Some(filters))))
      case InstanceType.Node =>
        TableExpression(nodes = Some(NodesTableExpression(filter = Some(filters))))
    }
}
