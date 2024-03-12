package cognite.spark.v1

import cats.effect.{Async, IO}
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.{
  ViewCorePropertyConfig,
  ViewSyncCorePropertyConfig
}
import com.cognite.sdk.scala.common.{CdpApiException, ItemsWithCursor}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition.HasData
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.log4s.getLogger

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

  private val logger = getLogger
  // request no more data after seeing items with node/edge lastUpdatedTime >= terminationTimeStamp
  private val terminationTimeStamp = System.currentTimeMillis()
  private val mandatoryViewFields = schema.fields.filter(f =>
    f.name != "space" && f.name != "externalId" && f.name != "type" && f.name != "startNode" && f.name != "endNode" && !f.nullable)

  protected override def metadataAttributes(): Array[StructField] = {
    val nodeAttributes = Array(
      DataTypes.createStructField("node.version", DataTypes.LongType, true),
      DataTypes.createStructField("node.createdTime", DataTypes.LongType, true),
      DataTypes.createStructField("node.lastUpdatedTime", DataTypes.LongType, true),
      DataTypes.createStructField("node.deletedTime", DataTypes.LongType, true)
    )

    val edgeAttributes = Array(
      DataTypes.createStructField("edge.version", DataTypes.LongType, true),
      DataTypes.createStructField("edge.createdTime", DataTypes.LongType, true),
      DataTypes.createStructField("edge.lastUpdatedTime", DataTypes.LongType, true),
      DataTypes.createStructField("edge.deletedTime", DataTypes.LongType, true)
    )
    val metadataAttributes = Array(
      DataTypes.createStructField("metadata.cursor", DataTypes.StringType, true)
    )

    metadataAttributes ++ (intendedUsage match {
      case Usage.Edge => edgeAttributes
      case Usage.Node => nodeAttributes
      case Usage.All => edgeAttributes ++ nodeAttributes
    })
  }

  private def createSyncFilter(
      hasDataFilter: Boolean,
      filters: Array[Filter],
      instanceType: InstanceType): Option[FilterDefinition] = {
    val hasData: Option[HasData] =
      if (!hasDataFilter) {
        None
      } else {
        viewReference.map { viewRef =>
          HasData(List(viewRef))
        }
      }
    val requestFilters: Seq[FilterDefinition] = (filters.map {
      toNodeOrEdgeAttributeFilter(instanceType, _).toOption
    } ++ Array(hasData)).flatten.toSeq

    if (requestFilters.isEmpty) {
      None
    } else {
      Some(FilterDefinition.And(requestFilters))
    }
  }

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val currentCursor: Option[String] = if (cursor == "") None else Some(cursor)
    Seq(
      syncOut(
        hasDataFilter = true,
        filters = filters,
        selectedColumns = selectedColumns,
        currentCursor = currentCursor))
  }

  // scalastyle:off cyclomatic.complexity
  private def buildRequest(
      hasDataFilter: Boolean,
      filters: Array[Filter],
      selectedColumns: Array[String],
      currentCursor: Option[String]): InstanceSyncRequest = {
    val selectedInstanceProps = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceType = intendedUsage match {
      case Usage.Edge => InstanceType.Edge
      case Usage.Node => InstanceType.Node
      case Usage.All =>
        throw new CdfSparkIllegalArgumentException("Cannot sync both nodes and edges at the same time")
    }

    val syncFilter = createSyncFilter(hasDataFilter, filters, instanceType)
    val tableExpression = instanceType match {
      case InstanceType.Edge =>
        TableExpression(edges = Some(EdgeTableExpression(filter = syncFilter)))
      case InstanceType.Node =>
        TableExpression(nodes = Some(NodesTableExpression(filter = syncFilter)))
    }

    val sourceReference: Seq[SourceSelector] = viewReference
      .map(
        r =>
          SourceSelector(
            source = r,
            properties = selectedInstanceProps.toIndexedSeq.filter(p =>
              !p.startsWith("node.") && !p.startsWith("edge.") && !p.startsWith("metadata.") &&
                p != "startNode" && p != "endNode" && p != "space" && p != "externalId" && p != "type")
        )
      )
      .toSeq

    InstanceSyncRequest(
      `with` = Map("sync" -> tableExpression),
      cursors = currentCursor.flatMap(c => Some(Map("sync" -> c))),
      select = Map("sync" -> SelectExpression(sources = sourceReference)),
      includeTyping = Some(true)
    )
  }

  private def syncOut(
      hasDataFilter: Boolean,
      filters: Array[Filter],
      selectedColumns: Array[String],
      currentCursor: Option[String])(
      implicit F: Async[IO]): Stream[IO, ProjectedFlexibleDataModelInstance] = {

    val synced: IO[(ItemsWithCursor[InstanceDefinition], Boolean)] =
      syncWithCursor(hasDataFilter, filters, selectedColumns, currentCursor)
    val stream: Stream[IO, ProjectedFlexibleDataModelInstance] = Stream.eval(synced).flatMap {
      case (ItemsWithCursor(items, nextCursor), continueWithHasData) =>
        val projectedItems =
          items.filter(clientsideHasDataFilter).map(toProjectedInstance(_, nextCursor, selectedColumns))

        val nextStream = nextCursor match {
          case Some(c) if items.nonEmpty && items.last.lastUpdatedTime < terminationTimeStamp =>
            syncOut(continueWithHasData, filters, selectedColumns, Some(c))
          case _ => Stream.empty
        }
        Stream.emits(projectedItems) ++ nextStream
    }
    stream
  }

  private def clientsideHasDataFilter(item: InstanceDefinition): Boolean = {
    val values = item.properties.getOrElse(Map.empty).values.flatMap(_.values).fold(Map.empty)(_ ++ _)
    values.nonEmpty &&
    mandatoryViewFields.forall { field =>
      values.contains(field.name)
    }
  }

  private def syncWithCursor(
      hasDataFilter: Boolean,
      filters: Array[Filter],
      selectedColumns: Array[String],
      currentCursor: Option[String]): IO[(ItemsWithCursor[InstanceDefinition], Boolean)] = {
    val syncRequest = buildRequest(hasDataFilter, filters, selectedColumns, currentCursor)
    var continueWithHasData = hasDataFilter
    val response = client.instances
      .syncRequest(syncRequest)
      .redeemWith(
        {
          // if cursor has expired, restart from empty cursor.
          case e: CdpApiException if e.code == 400 && e.message.startsWith("Cursor has expired") => {
            logger.warn(e)("Cursor has expired. Starting from scratch.")
            client.instances.syncRequest(syncRequest.copy(cursors = None))
          }
          case e: CdpApiException
              if continueWithHasData && e.code == 408 && e.message.startsWith(
                "Graph query timed out.") => {
            logger.warn(e)("Graph query timed out. Retrying without hasData filter.")
            // drop the hasData filter and retry
            continueWithHasData = false
            client.instances.syncRequest(
              buildRequest(continueWithHasData, filters, selectedColumns, currentCursor))
          }
          case e => IO.raiseError(e)
        },
        IO.pure
      )

    response.map { sr =>
      val itemDefinitions = sr.items.getOrElse(Map.empty).getOrElse("sync", Vector.empty)
      val nextCursor = sr.nextCursor.getOrElse(Map.empty).get("sync")
      (ItemsWithCursor(itemDefinitions, nextCursor), continueWithHasData)
    }
  }
}
