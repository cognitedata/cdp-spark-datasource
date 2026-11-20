package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.fdm.FlexibleDataModelBaseRelation
import cognite.spark.v1.fdm.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import com.cognite.sdk.scala.common.BearerTokenAuth
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDefinition
import fs2.Stream
import natchez.Kernel
import natchez.Trace.Implicits.noop
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import sttp.client3.impl.cats.implicits.asyncMonadError
import sttp.client3.testing.SttpBackendStub
import sttp.client3.{Response, StringBody}
import sttp.model.StatusCode
import sttp.monad.MonadAsyncError

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/** Shared stubs and FDM reference relation for BatchApi vs FDM comparison tests. */
object BatchApiTestSupport {
  final case class RecordedRequest(
      method: String,
      path: Seq[String],
      body: String,
      resourceType: Option[Any])

  def relationConfig(
      project: String = "test-project",
      collectMetrics: Boolean = false,
      metricsPrefix: String = "",
      applicationId: String = "BatchApiTest"
  ): RelationConfig =
    RelationConfig(
      auth = CdfSparkAuth.Static(BearerTokenAuth("token")),
      clientTag = None,
      applicationName = Some(applicationId),
      projectName = project,
      batchSize = Some(Constants.DefaultBatchSize),
      limitPerPartition = None,
      partitions = Constants.DefaultPartitions,
      maxRetries = Constants.DefaultMaxRetries,
      maxRetryDelaySeconds = Constants.DefaultMaxRetryDelaySeconds,
      initialRetryDelayMillis = Constants.DefaultInitialRetryDelay.toMillis.toInt,
      collectMetrics = collectMetrics,
      collectTestMetrics = false,
      metricsTrackAttempts = false,
      metricsPrefix = metricsPrefix,
      baseUrl = "https://api.cognitedata.com",
      onConflict = OnConflictOption.Abort,
      applicationId = applicationId,
      parallelismPerPartition = Constants.DefaultParallelismPerPartition,
      ignoreUnknownIds = true,
      deleteMissingAssets = false,
      subtrees = AssetSubtreeOption.Ingest,
      ignoreNullFields = true,
      rawEnsureParent = false,
      enableSinglePartitionDeleteAssetHierarchy = false,
      tracingConfig = TracingConfig(new Kernel(Map.empty), None, None),
      serverSideFilterNullValuesOnNonSchemaRawQueries = false,
      maxOutstandingRawInsertRequests = None,
      sendDebugFlag = false
    )

  def clientFor(
      backend: SttpBackendStub[IO, Any],
      project: String = "test-project",
      applicationName: String = "BatchApiTest"): GenericClient[IO] =
    new GenericClient[IO](
      applicationName = applicationName,
      projectName = project,
      baseUrl = "https://api.cognitedata.com",
      auth = BearerTokenAuth("token"),
      sttpBackend = backend
    )

  // An FDM relation with the schema and row mapping FDM itself uses, for comparing with batch.
  // It does not read anything.
  class FdmReference(
      config: RelationConfig,
      usage: Usage,
      viewProps: Map[String, ViewPropertyDefinition])(val sqlContext: SQLContext)
      extends FlexibleDataModelBaseRelation(config, sqlContext) {
    override def schema: StructType =
      deriveViewPropertySchemaWithUsageSpecificAttributes(usage, viewProps)

    override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
        client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = Seq.empty

    override def insert(rows: Seq[Row]): IO[Unit] = IO.unit
    override def upsert(rows: Seq[Row]): IO[Unit] = IO.unit
    override def update(rows: Seq[Row]): IO[Unit] = IO.unit
    override def delete(rows: Seq[Row]): IO[Unit] = IO.unit

    def row(instance: InstanceDefinition): Row =
      toRow(toProjectedInstance(instance, None, schema.fieldNames))
  }

  /**
    * Serves scripted poll responses (repeating the last), the given cursors, and one page per read
    * cursor. Anything else gets a 404; every request is recorded.
    */
  final class FakeBatchApi(
      pollResponses: Seq[String],
      readCursors: Seq[String],
      pages: Map[String, String]) {
    val requests = new ConcurrentLinkedQueue[RecordedRequest]()
    private val polls = new AtomicInteger(0)

    private def readCursorOf(body: String): String =
      io.circe.parser
        .parse(body)
        .flatMap(_.hcursor.get[String]("readCursor"))
        .getOrElse("")

    val backend: SttpBackendStub[IO, Any] =
      SttpBackendStub(implicitly[MonadAsyncError[IO]]).whenRequestMatchesPartial {
        case request =>
          val body = request.body match {
            case StringBody(s, _, _) => s
            case _ => ""
          }
          requests.add(
            RecordedRequest(
              request.method.method,
              request.uri.path,
              body,
              request.tag(GenericClient.RESOURCE_TYPE_TAG)))
          request.uri.path.dropWhile(_ != "swamp").drop(1) match {
            case Seq("poll", _) =>
              Response.ok(pollResponses(math.min(polls.getAndIncrement(), pollResponses.size - 1)))
            case Seq("cursors", _) =>
              Response.ok(s"""{"readCursors":[${readCursors.map(c => s""""$c"""").mkString(",")}]}""")
            case Seq("read") =>
              pages
                .get(readCursorOf(body))
                .map(Response.ok(_))
                .getOrElse(Response("", StatusCode.NotFound))
            case _ => Response("", StatusCode.NotFound)
          }
      }

    def recorded: List[RecordedRequest] = requests.asScala.toList
  }

  object FakeBatchApi {
    def readyPoll(readyId: UUID): String =
      s"""{"status":"ready","readyId":"$readyId"}"""

    /** Serve DM instances as a ready batch (via [[BatchApiViaDm]] page encoding). */
    def fromInstances(
        items: Seq[InstanceDefinition],
        readyId: UUID = UUID.randomUUID(),
        pageSize: Int = 100): FakeBatchApi = {
      val (cursors, pages) = BatchApiViaDm.asSwampPages(items, pageSize)
      new FakeBatchApi(
        pollResponses = Seq(readyPoll(readyId)),
        readCursors = cursors,
        pages = pages)
    }
  }
}
