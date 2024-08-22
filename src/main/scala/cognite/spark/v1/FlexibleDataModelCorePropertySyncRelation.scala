package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.CdpConnector.ioRuntime
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.ViewCorePropertyConfig
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

sealed trait SyncMode {}

// /query all items first, report futureItemsCursor as cursor for next run
final case class BackFillMode(futureItemsSyncCursor: String) extends SyncMode

// sync starting from a non-empty non-expired /sync cursor from a previous run
final case class StreamMode(syncCursors: Map[String, String]) extends SyncMode

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
    cursorName: Option[String],
    jobId: Option[String],
    syncCursorSaveCallbackUrl: Option[String],
    corePropConfig: ViewCorePropertyConfig)(sqlContext: SQLContext)
    extends FlexibleDataModelCorePropertyRelation(
      config,
      corePropConfig
    )(sqlContext) {

  private val logger = getLogger

  // request no more data after seeing items with node/edge lastUpdatedTime >= terminationTimeStamp
  private val terminationTimeStamp = System.currentTimeMillis()

  protected override def metadataAttributes(): Array[StructField] =
    Array(
      DataTypes.createStructField("metadata.cursor", DataTypes.StringType, true)
    )

  private def createSyncFilter(
      filters: Array[Filter],
      instanceType: InstanceType): FilterDefinition.And = {
    val hasData: Option[HasData] = viewReference.map { viewRef =>
      HasData(List(viewRef))
    }
    val requestFilters: Seq[FilterDefinition] = (filters.map {
      // don't specify viewReference to avoid filter pushdown for non-reserved attributes
      // for performance reasons, sync is incremental and filtering is done in spark to
      // avoid edge cases where too much needs to be filtered out and service request would
      // time out filtering it
      toFilter(instanceType, _, viewReference = None, isSyncRequest = true).toOption
    } ++ Array(hasData)).flatten.toSeq
    FilterDefinition.And(requestFilters)
  }

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
    def sourceReference(instanceType: InstanceType): Seq[SourceSelector] =
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

    def reservedPropertyNames(instanceType: InstanceType): Seq[String] = {
      val result = Seq("space", "externalId", "_type")
      instanceType match {
        case InstanceType.Node => result
        case InstanceType.Edge => result ++ Seq("startNode", "endNode", "type")
      }
    }

    val cursors = if (cursor.nonEmpty) Some(Map("sync" -> cursor)) else None
    def select(instanceType: InstanceType) =
      Map("sync" -> SelectExpression(sources = sourceReference(instanceType)))
    val syncMode =
      decideSyncMode(cursors, instanceType, select(instanceType)).unsafeRunSync()

    Seq(
      syncOut(
        syncMode,
        `with` = Map("sync" -> tableExpression),
        select(instanceType),
        selectedColumns
      ))
  }

  private val matchNothingFilter: FilterDefinition =
    FilterDefinition.Not(MatchAll(JsonObject()))

  /**
    * Validate cursors validity if present
    * If cursors are invalid or not present generate sync cursor that would match _future_
    * updates, demand initial full backfill fetch via query endpoint.
    * Note: cursors validitiy is checked via future cursor fetch attempt so an attempt to fetch
    *       future cursor is always made
    */
  private def decideSyncMode(
      cursors: Option[Map[String, String]],
      instanceType: InstanceType,
      select: Map[String, SelectExpression]): IO[SyncMode] =
    fetchData(
      useQueryEndpoint = false,
      cursors = cursors,
      `with` = Map("sync" -> generateTableExpression(instanceType, matchNothingFilter)),
      select = select)
      .flatMap { sr =>
        (sr.nextCursor, cursors) match {
          case (None, _) =>
            IO.raiseError(
              new IllegalArgumentException("/sync response must have " +
                "nextCursor"))
          // sync cursors are present and non-expired
          case (Some(_), Some(syncCursors)) => IO.pure(StreamMode(syncCursors))
          // otherwise fallback to backfill first
          case (Some(futureItemsSyncCursor), None) => IO.pure(BackFillMode(futureItemsSyncCursor))
        }
      }
      .redeemWith(
        {
          case e: CdpApiException if e.code == 400 && e.message.startsWith("Cursor has expired") =>
            logger.warn("Cursor has expired, doing a full backfill")
            decideSyncMode(None, instanceType, select)
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
      useQueryEndpoint: Boolean,
      cursors: Option[Map[String, String]],
      `with`: Map[String, TableExpression],
      select: Map[String, SelectExpression]): IO[ItemsWithCursor[InstanceDefinition]] =
    if (useQueryEndpoint) {
      val response = client.instances.queryRequest(
        InstanceQueryRequest(
          `with` = `with`,
          cursors = cursors,
          select = select,
          includeTyping = Some(true)
        )
      )
      response.map { qr =>
        val itemDefinitions = qr.items.flatMap(_.get("sync")).getOrElse(Vector.empty)
        val nextCursor = qr.nextCursor.flatMap(_.get("sync"))
        ItemsWithCursor(itemDefinitions, nextCursor)
      }
    } else {
      val response = client.instances.syncRequest(
        InstanceSyncRequest(
          `with` = `with`,
          cursors = cursors,
          select = select,
          includeTyping = Some(true)
        )
      )
      response.map { qr =>
        val itemDefinitions = qr.items.flatMap(_.get("sync")).getOrElse(Vector.empty)
        val nextCursor = qr.nextCursor.get("sync")
        ItemsWithCursor(itemDefinitions, nextCursor)
      }
    }

  private def syncOut(
      syncMode: SyncMode,
      `with`: Map[String, TableExpression],
      select: Map[String, SelectExpression],
      selectedProps: Array[String]): Stream[IO, ProjectedFlexibleDataModelInstance] = {
    val (isBackFill, cursors, toProjectedCursor) = syncMode match {
      case StreamMode(syncCursors) =>
        (false, Some(syncCursors), (nextCursor: Option[String]) => nextCursor)
      case BackFillMode(futureItemsSyncCursor) =>
        (true, None, (_: Option[String]) => Some(futureItemsSyncCursor))
    }
    val projectInstance = (nextCursor: Option[String]) =>
      toProjectedInstance(_, toProjectedCursor(nextCursor), selectedProps)
    val projectFinalCursor = (nextCursor: Option[String]) => toProjectedCursor(nextCursor)
    val dataFetcher = (cursors: Option[Map[String, String]]) =>
      fetchData(useQueryEndpoint = isBackFill, cursors, `with`, select)
    syncOut(
      cursors,
      dataFetcher,
      projectInstance,
      projectFinalCursor,
      applyTimestampTermination = !isBackFill)
  }

  private def syncOut(
      cursors: Option[Map[String, String]],
      fetchData: Option[Map[String, String]] => IO[ItemsWithCursor[InstanceDefinition]],
      projectInstance: Option[String] => InstanceDefinition => ProjectedFlexibleDataModelInstance,
      projectFinalCursor: Option[String] => Option[String],
      applyTimestampTermination: Boolean
  ): Stream[IO, ProjectedFlexibleDataModelInstance] =
    Stream.eval {
      fetchData(cursors).map { sr =>
        val items = sr.items
        val nextCursor = sr.nextCursor
        val shouldStopEarly = items.nonEmpty && applyTimestampTermination && items.last.lastUpdatedTime >= terminationTimeStamp
        val shouldStop = items.isEmpty || shouldStopEarly // || nextCursor.isEmpty is handled by matching
        val next =
          (nextCursor, shouldStop) match {
            case (Some(cursor), false) =>
              syncOut(
                Some(Map("sync" -> cursor)),
                fetchData,
                projectInstance,
                projectFinalCursor,
                applyTimestampTermination)
            case (None, _) | (_, true) =>
              fs2.Stream.exec(saveLastCursor(projectFinalCursor(nextCursor)))
          }
        val projection = projectInstance(nextCursor)
        val projected = items.map(projection)
        fs2.Stream.emits(projected) ++ next
      }
    }.flatten

  private def saveLastCursor(cursorValue: Option[String]): IO[Unit] =
    (syncCursorSaveCallbackUrl, jobId, cursorName, cursorValue) match {
      case (Some(syncCursorSaveCallbackUrl), Some(jobId), Some(cursorName), Some(cursorValue)) =>
        SyncCursorCallback
          .lastCursorCallback(syncCursorSaveCallbackUrl, cursorName, cursorValue, jobId)
          .void
      case _ => IO.unit
    }
}
