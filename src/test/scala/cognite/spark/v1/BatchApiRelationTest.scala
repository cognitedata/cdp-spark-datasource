package cognite.spark.v1

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.cognite.sdk.scala.common.BearerTokenAuth
import com.cognite.sdk.scala.v1.GenericClient
import natchez.Kernel
import natchez.Trace.Implicits.noop
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import sttp.client3.impl.cats.implicits.asyncMonadError
import sttp.client3.testing.SttpBackendStub
import sttp.client3.{Response, StringBody}
import sttp.model.StatusCode
import sttp.monad.MonadAsyncError

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object BatchApiRelationTest {
  final case class RecordedRequest(
      method: String,
      path: Seq[String],
      body: String,
      resourceType: Option[Any])
}

class BatchApiRelationTest extends FlatSpec with Matchers {
  import BatchApiRelationTest._

  private val project = "test-project"
  private val statusId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val readyId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val basePath = Seq("api", "v1", "projects", project, "models", "swamp")

  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("BatchApiRelationTest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private def config(collectMetrics: Boolean = false, metricsPrefix: String = ""): RelationConfig =
    RelationConfig(
      auth = CdfSparkAuth.Static(BearerTokenAuth("token")),
      clientTag = None,
      applicationName = Some("BatchApiRelationTest"),
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
      applicationId = "BatchApiRelationTest",
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

  private def node(externalId: String): String =
    s"""{"instanceType":"node","space":"space","externalId":"$externalId","version":1,"createdTime":1,"lastUpdatedTime":2,"properties":{}}"""

  private def page(externalIds: Seq[String], next: Option[String]): String =
    s"""{"items":[${externalIds.map(node).mkString(",")}]${next
      .map(c => s""","nextReadCursor":"$c"""")
      .getOrElse("")}}"""

  private val pollPending = """{"status":"pending"}"""
  private val pollBackfilling = """{"status":"backfilling"}"""
  private val pollProgressing = """{"status":"progressing"}"""
  private val pollFailed = """{"status":"failed"}"""
  private val pollReady = s"""{"status":"ready","readyId":"$readyId"}"""

  // Serves the scripted poll responses in order (repeating the last one), the given cursors,
  // and one page per read cursor. Anything else gets a 404, and every request is recorded.
  private final class FakeBatchApi(
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
              pages.get(readCursorOf(body)).map(Response.ok(_)).getOrElse(Response("", StatusCode.NotFound))
            case _ => Response("", StatusCode.NotFound)
          }
      }

    def recorded: List[RecordedRequest] = requests.asScala.toList
  }

  private def clientFor(backend: SttpBackendStub[IO, Any]): GenericClient[IO] =
    new GenericClient[IO](
      applicationName = "BatchApiRelationTest",
      projectName = project,
      baseUrl = "https://api.cognitedata.com",
      auth = BearerTokenAuth("token"),
      sttpBackend = backend
    )

  private def relation(config: RelationConfig) =
    new BatchApiRelation(config, statusId, Some(Map.empty), pollInterval = 1.millisecond)(
      spark.sqlContext)

  private def readAll(relation: BatchApiRelation, api: FakeBatchApi): List[String] = {
    val streams = relation.getStreams(clientFor(api.backend))
    streams should have size 1
    streams.head.compile.toList.unsafeRunSync().map(_.externalId)
  }

  it should "follow nextReadCursor until it is absent" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollReady),
      readCursors = Seq("c1"),
      pages = Map(
        "c1" -> page(Seq("a", "b"), Some("c2")),
        "c2" -> page(Seq("c"), None)
      )
    )

    readAll(relation(config()), api) shouldBe List("a", "b", "c")

    api.recorded.map(r => (r.method, r.path, r.body)) shouldBe List(
      ("GET", basePath :+ "poll" :+ statusId.toString, ""),
      ("GET", basePath :+ "cursors" :+ readyId.toString, ""),
      ("POST", basePath :+ "read", """{"readCursor":"c1"}"""),
      ("POST", basePath :+ "read", """{"readCursor":"c2"}""")
    )
  }

  it should "read several starting cursors one after another in a single stream" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollReady),
      readCursors = Seq("c1", "c2"),
      pages = Map(
        "c1" -> page(Seq("a"), None),
        "c2" -> page(Seq("b"), None)
      )
    )

    readAll(relation(config()), api) shouldBe List("a", "b")
  }

  it should "poll until the batch is ready, counting each status" in {
    val prefix = s"batchapi-test-${UUID.randomUUID()}"
    val api = new FakeBatchApi(
      pollResponses = Seq(pollPending, pollPending, pollBackfilling, pollProgressing, pollReady),
      readCursors = Seq("c1"),
      pages = Map("c1" -> page(Seq("a"), None))
    )

    readAll(relation(config(collectMetrics = true, metricsPrefix = prefix)), api) shouldBe List("a")

    api.recorded.count(_.path.contains("poll")) shouldBe 5
    def pollCount(status: String): Long =
      MetricsSource.metricsMap.get(s"$prefix.batchapi.poll.$status").value.getCount
    pollCount("pending") shouldBe 2
    pollCount("backfilling") shouldBe 1
    pollCount("progressing") shouldBe 1
    pollCount("ready") shouldBe 1
  }

  it should "fail without fetching cursors when the batch fails" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollPending, pollFailed),
      readCursors = Seq("c1"),
      pages = Map.empty
    )

    val e = intercept[CdfSparkException] {
      readAll(relation(config()), api)
    }

    e.getMessage should include(statusId.toString)
    api.recorded.map(_.path.dropWhile(_ != "swamp").drop(1).head) shouldBe List("poll", "poll")
  }

  it should "tag its requests with the BATCHAPI resource type" in {
    val api = new FakeBatchApi(
      pollResponses = Seq(pollReady),
      readCursors = Seq("c1"),
      pages = Map("c1" -> page(Seq("a"), None))
    )

    readAll(relation(config()), api) shouldBe List("a")

    api.recorded should have size 3
    api.recorded.map(_.resourceType) shouldBe List.fill(3)(Some("BATCHAPI"))
  }
}
