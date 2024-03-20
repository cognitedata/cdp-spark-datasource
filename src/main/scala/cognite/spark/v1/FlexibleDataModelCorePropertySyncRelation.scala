package cognite.spark.v1

import cats.effect.{Async, IO}
import cognite.spark.v1.CdpConnector.ioRuntime
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.{
  ViewCorePropertyConfig,
  ViewSyncCorePropertyConfig
}
import com.cognite.sdk.scala.common.{CdpApiException, ItemsWithCursor}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition.{HasData, MatchAll}
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import io.circe.JsonObject
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
  // scalastyle:off method.length
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
      case Usage.All =>
        throw new CdfSparkIllegalArgumentException("Cannot sync both nodes and edges at the same time")
    }

    val syncFilter = createSyncFilter(filters, instanceType)
    val tableExpression = generateTableExpression(instanceType, syncFilter)
    val sourceReference: Seq[SourceSelector] = viewReference
      .map(
        r =>
          SourceSelector(
            source = r,
            properties = selectedInstanceProps.toIndexedSeq.filter(p =>
              !p.startsWith("node.") && !p.startsWith("edge.") && !p.startsWith("metadata.") &&
                p != "startNode" && p != "endNode" && p != "space" && p != "externalId" && p != "type")
        ))
      .toSeq
    val cursors = if (cursor.nonEmpty) Some(Map("sync" -> cursor)) else None
    val select = Map("sync" -> SelectExpression(sources = sourceReference))
    val (futureItemsCursor, executeBackFill) =
      generateFutureItemsCursor(cursors, instanceType, select).unsafeRunSync()

    Seq(
      syncOut(
        futureItemsCursor,
        cursors = if (executeBackFill) None else cursors,
        `with` = Map("sync" -> tableExpression),
        select,
        selectedColumns
      ))
  }

  private val matchNothingFilter: FilterDefinition =
    FilterDefinition.Not(MatchAll(JsonObject()))

  /**
    * Generate future items cursor and validate cursor expiration.
    * If the cursor has expired, generate a new cursor and do a full back fill.
    */
  private def generateFutureItemsCursor(
      cursors: Option[Map[String, String]],
      instanceType: InstanceType,
      select: Map[String, SelectExpression]): IO[(Option[String], Boolean)] =
    fetchData(
      isBackFill = false,
      cursors = cursors,
      `with` = Map(
        "sync" -> generateTableExpression(instanceType, matchNothingFilter)),
      select = select)
      .map { sr =>
        (sr.nextCursor, cursors.isEmpty)
      }
      .redeemWith(
        {
          case e: CdpApiException if e.code == 400 && e.message.startsWith("Cursor has expired") =>
            logger.warn("Cursor has expired, doing a full backfill")
            generateFutureItemsCursor(None, instanceType, select)
          case e => IO.raiseError(e)
        },
        IO.pure
      )

  private def generateTableExpression(
      instanceType: InstanceType,
      filters: FilterDefinition): TableExpression =
    instanceType match {
      case InstanceType.Edge =>
        TableExpression(edges = Some(EdgeTableExpression(filter = Some(filters))))
      case InstanceType.Node =>
        TableExpression(nodes = Some(NodesTableExpression(filter = Some(filters))))
    }

  private def fetchData(
      isBackFill: Boolean,
      cursors: Option[Map[String, String]],
      `with`: Map[String, TableExpression],
      select: Map[String, SelectExpression]): IO[ItemsWithCursor[InstanceDefinition]] = {
    val response = if (isBackFill) {
      client.instances.queryRequest(
        InstanceQueryRequest(
          `with` = `with`,
          cursors = cursors,
          select = select,
          includeTyping = Some(true)
        )
      )
    } else {
      client.instances.syncRequest(
        InstanceSyncRequest(
          `with` = `with`,
          cursors = cursors,
          select = select,
          includeTyping = Some(true)
        )
      )
    }
    response.map { qr =>
      val itemDefinitions = qr.items.getOrElse(Map.empty).get("sync").getOrElse(Vector.empty)
      val nextCursor = qr.nextCursor.getOrElse(Map.empty).get("sync")
      ItemsWithCursor(itemDefinitions, nextCursor)
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def syncOut(
      futureItems: Option[String],
      cursors: Option[Map[String, String]],
      `with`: Map[String, TableExpression],
      select: Map[String, SelectExpression],
      selectedProps: Array[String])(
      implicit F: Async[IO]): Stream[IO, ProjectedFlexibleDataModelInstance] =
    Stream.eval {
      fetchData(futureItems.nonEmpty, cursors, `with`, select).map { sr =>
        val items = sr.items
        val nextCursor = sr.nextCursor
        val next =
          (nextCursor, items.nonEmpty && items.last.lastUpdatedTime < terminationTimeStamp) match {
            case (Some(cursor), true) =>
              syncOut(futureItems, Some(Map("sync" -> cursor)), `with`, select, selectedProps)
            case _ => fs2.Stream.empty
          }
        val projectedCursorId = if (futureItems.isEmpty) nextCursor else futureItems
        val projected = items.map(toProjectedInstance(_, projectedCursorId, selectedProps))
        fs2.Stream.emits(projected) ++ next
      }
    }.flatten

}
