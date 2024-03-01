package cognite.spark.v1

import cats.effect.{Async, IO}
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.{
  ViewCorePropertyConfig,
  ViewSyncCorePropertyConfig
}
import com.cognite.sdk.scala.common.ItemsWithCursor
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition.HasData
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Flexible Data Model Relation for syncing Nodes or Edges with properties
  *
  * @param config common relation configs
  * @param corePropConfig view core property config
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelCorePropertySyncRelation(
    cursor: String,
    config: RelationConfig,
    corePropConfig: ViewSyncCorePropertyConfig)(sqlContext: SQLContext)
    extends FlexibleDataModelCorePropertyRelation(
      config,
      ViewCorePropertyConfig(
        corePropConfig.intendedUsage,
        corePropConfig.viewReference,
        corePropConfig.instanceSpace)
    )(sqlContext) {

  protected override def metadataAttributes(): Array[StructField] =
    Array(
      DataTypes.createStructField("metadata.cursor", DataTypes.StringType, true),
      DataTypes.createStructField("metadata.version", DataTypes.LongType, true),
      DataTypes.createStructField("metadata.createdTime", DataTypes.LongType, true),
      DataTypes.createStructField("metadata.lastUpdatedTime", DataTypes.LongType, true),
      DataTypes.createStructField("metadata.deletedTime", DataTypes.LongType, true)
    )

  private def createSyncFilter(
      filters: Array[Filter],
      instanceType: InstanceType): FilterDefinition.And = {
    val hasData: Option[HasData] = viewReference.map { viewRef =>
      HasData(List(viewRef))
    }
    val requestFilters: Seq[FilterDefinition] = (filters.map {
      toNodeOrEdgeAttributeFilter(instanceType, _).toOption
    } ++ Array(hasData)).flatten.toSeq
    FilterDefinition.And(requestFilters)
  }

  // scalastyle:off cyclomatic.complexity
  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceType = intendedUsage match {
      case Usage.Edge => InstanceType.Edge
      case Usage.Node => InstanceType.Node
      case Usage.All => throw new CdfSparkIllegalArgumentException("Cannot sync both nodes and edges")
    }

    val syncFilter = createSyncFilter(filters, instanceType)
    val tableExpression = instanceType match {
      case InstanceType.Edge =>
        TableExpression(edges = Some(EdgeTableExpression(filter = Some(syncFilter))))
      case InstanceType.Node =>
        TableExpression(nodes = Some(NodesTableExpression(filter = Some(syncFilter))))
    }

    val sourceReference: Seq[SourceSelector] = viewReference
      .map(
        r =>
          SourceSelector(
            source = r,
            properties = selectedInstanceProps.toIndexedSeq.filter(p =>
              !p.startsWith("metadata.") && p != "startNode" && p != "endNode" && p != "space" && p != "externalId" && p != "type")
        ))
      .toSeq

    val initialCursor = if (cursor.nonEmpty) {
      Some(Map("sync" -> cursor))
    } else {
      None
    }

    val instanceSyncRequest = InstanceSyncRequest(
      `with` = Map("sync" -> tableExpression),
      cursors = initialCursor,
      select = Map("sync" -> SelectExpression(sources = sourceReference)))

    Seq(syncOut(instanceSyncRequest, selectedColumns))
  }
  // scalastyle:on cyclomatic.complexity

  private def syncOut(syncRequest: InstanceSyncRequest, selectedProps: Array[String])(
      implicit F: Async[IO]): Stream[IO, ProjectedFlexibleDataModelInstance] =
    Stream.eval {
      syncWithCursor(syncRequest).map { sr =>
        val items = sr.items
        val nextCursor = sr.nextCursor
        val next = (nextCursor, items.nonEmpty) match {
          case (Some(cursor), true) =>
            syncOut(syncRequest.copy(cursors = Some(Map("sync" -> cursor))), selectedProps)
          case _ => fs2.Stream.empty
        }

        val projected = items.map(toProjectedInstance(_, nextCursor, selectedProps))
        fs2.Stream.emits(projected) ++ next
      }
    }.flatten

  private def syncWithCursor(syncRequest: InstanceSyncRequest): IO[ItemsWithCursor[InstanceDefinition]] =
    client.instances.syncRequest(syncRequest).map { sr =>
      val itemDefinitions = sr.items.getOrElse(Map.empty).get("sync").getOrElse(Vector.empty)
      val nextCursor = sr.nextCursor.getOrElse(Map.empty).get("sync")
      ItemsWithCursor(itemDefinitions, nextCursor)
    }
}
