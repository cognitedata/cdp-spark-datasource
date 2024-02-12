package cognite.spark.v1

import cats.effect.{Async, IO}
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.{
  ViewCorePropertyConfig,
  ViewSyncCorePropertyConfig
}
import com.cognite.sdk.scala.common.ItemsWithCursor
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition.HasData
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

/**
  * Flexible Data Model Relation for syncing Nodes or Edges with properties
  *
  * @param config common relation configs
  * @param corePropConfig view core property config
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelCorePropertySyncRelation(
    config: RelationConfig,
    corePropConfig: ViewSyncCorePropertyConfig)(sqlContext: SQLContext)
    extends FlexibleDataModelCorePropertyRelation(
      config,
      ViewCorePropertyConfig(
        corePropConfig.intendedUsage,
        corePropConfig.viewReference,
        corePropConfig.instanceSpace)
    )(sqlContext) {

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val syncRequests = compatibleInstanceTypes(intendedUsage).map { instanceType =>
      val hasData: Option[HasData] = viewReference.map { viewRef =>
        HasData(List(viewRef))
      }
      val tableExpression = instanceType match {
        case InstanceType.Edge => TableExpression(edges = Some(EdgeTableExpression(filter = hasData)))
        case InstanceType.Node => TableExpression(nodes = Some(NodesTableExpression(filter = hasData)))
      }
      val sourceReference: Seq[SourceSelector] = viewReference
        .map(
          r =>
            SourceSelector(
              source = r,
              properties = selectedInstanceProps.filter(p =>
                p != "startNode" && p != "endNode" && p != "space" && p != "externalId" && p != "type")))
        .toList

      InstanceSyncRequest(
        `with` = Map("sync" -> tableExpression),
        cursors = None,
        select = Map("sync" -> SelectExpression(sources = sourceReference)))
    }
    syncRequests.distinct.map { sr =>
      syncOut(sr).map(toProjectedInstance(_, selectedInstanceProps))
    }
  }

  private def syncOut(syncRequest: InstanceSyncRequest)(
      implicit F: Async[IO]): Stream[IO, InstanceDefinition] =
    Stream.eval {
      syncWithCursor(syncRequest).map { sr =>
        val items = sr.items
        val nextCursor = sr.nextCursor

        val next = (nextCursor, items.nonEmpty) match {
          case (Some(cursor), true) => syncOut(syncRequest.copy(cursors = Some(Map("sync" -> cursor))))
          case _ => fs2.Stream.empty
        }
        fs2.Stream.emits(items) ++ next
      }
    }.flatten

  private def syncWithCursor(syncRequest: InstanceSyncRequest): IO[ItemsWithCursor[InstanceDefinition]] =
    client.instances.syncRequest(syncRequest).map { sr =>
      val itemDefinitions = sr.items.getOrElse(Map.empty).get("sync").getOrElse(Vector.empty)
      val nextCursor = sr.nextCursor.getOrElse(Map.empty).get("sync")
      ItemsWithCursor(itemDefinitions, nextCursor)
    }
}
