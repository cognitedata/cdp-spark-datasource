package cognite.spark.v1

import java.io.IOException
import java.util.UUID
import java.util.concurrent.Executors
import cats.Id
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.ApiKeyAuth
import org.apache.spark.sql.{Encoder, SparkSession}
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.{Matchers, Tag}
import org.apache.spark.datasource.MetricsSource

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration._
import scala.util.Random
import cats.effect.{IO, Timer}
import cats.implicits.catsSyntaxFlatMapOps
import com.cognite.sdk.scala.v1._
import org.apache.spark.SparkException
import org.apache.spark.sql.types.StructType
import org.scalactic.{Prettifier, source}
import org.scalatest.prop.TableFor1

import scala.reflect.{ClassTag, classTag}

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")

trait SparkTest {
  implicit lazy val timer: Timer[IO] = IO.timer(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1)))

  implicit def single[A](implicit c: ClassTag[OptionalField[A]], inner: Encoder[Option[A]]): Encoder[OptionalField[A]] =
    new Encoder[OptionalField[A]] {
      override def schema: StructType = inner.schema

      override def clsTag: ClassTag[OptionalField[A]] = c
    }

  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")
  assert(writeApiKey != null && !writeApiKey.isEmpty, "Environment variable \"TEST_API_KEY_WRITE\" was not set")
  implicit val writeApiKeyAuth: ApiKeyAuth = ApiKeyAuth(writeApiKey)
  val writeClient = GenericClient.forAuth[Id]("cdp-spark-datasource-test", writeApiKeyAuth)

  val readApiKey = System.getenv("TEST_API_KEY_READ")
  assert(readApiKey != null && !readApiKey.isEmpty, "Environment variable \"TEST_API_KEY_READ\" was not set")
  implicit val readApiKeyAuth: ApiKeyAuth = ApiKeyAuth(readApiKey)
  val readClient = GenericClient.forAuth[Id]("cdp-spark-datasource-test", readApiKeyAuth)

  // not needed to run tests, only for replicating some problems specific to this tenant
  lazy val jetfiretest2ApiKey = System.getenv("TEST_APU_KEY_JETFIRETEST2")

  val testDataSetId = 86163806167772L

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    // https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.storeAssignmentPolicy", "legacy")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random * 1000).toLong.toString)
    .getOrCreate()

  // We have many tests with expected Spark errors. Remove this if you're troubleshooting a test.
  spark.sparkContext.setLogLevel("OFF")

  def shortRandomString(): String = UUID.randomUUID().toString.substring(0, 8)

  // scalastyle:off cyclomatic.complexity
  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int, maxDelay: FiniteDuration = 20.seconds): IO[A] = {
    val exponentialDelay = (Constants.DefaultMaxBackoffDelay / 2).min(initialDelay * 2)
    val randomDelayScale = (Constants.DefaultMaxBackoffDelay / 2).min(initialDelay * 2).toMillis
    val nextDelay = Random.nextInt(randomDelayScale.toInt).millis + exponentialDelay
    ioa.handleErrorWith {
      case exception @ (_: TimeoutException | _: IOException) =>
        if (maxRetries > 0) {
          IO.sleep(initialDelay) >> retryWithBackoff(ioa, nextDelay.min(maxDelay), maxRetries - 1)
        } else {
          IO.raiseError(exception)
        }
      case error => IO.raiseError(error)
    }
  }
  // scalastyle:on cyclomatic.complexity

  def retryWhile[A](action: => A, shouldRetry: A => Boolean)(implicit prettifier: Prettifier, pos: source.Position): A =
    retryWithBackoff(
      IO {
        val actionValue = action
        if (shouldRetry(actionValue)) {
          throw new TimeoutException(s"Retry timed out at ${pos.fileName}:${pos.lineNumber}, value = ${prettifier(actionValue)}")
        }
        actionValue
      },
      1.second,
      20
    ).unsafeRunTimed(5.minutes).getOrElse(throw new RuntimeException("Test timed out during retries"))


  val updateAndUpsert: TableFor1[String] = Table("mode", "upsert", "update")

  def getDefaultConfig(auth: CdfSparkAuth): RelationConfig =
    RelationConfig(
      auth,
      Some("SparkDatasourceTestTag"),
      Some("SparkDatasourceTestApp"),
      DefaultSource.getProjectFromAuth(auth, Constants.DefaultMaxRetries, Constants.DefaultMaxRetryDelaySeconds, Constants.DefaultBaseUrl),
      Some(Constants.DefaultBatchSize),
      None,
      Constants.DefaultPartitions,
      Constants.DefaultMaxRetries,
      Constants.DefaultMaxRetryDelaySeconds,
      collectMetrics = false,
      collectTestMetrics = false,
      "",
      Constants.DefaultBaseUrl,
      OnConflict.Abort,
      spark.sparkContext.applicationId,
      Constants.DefaultParallelismPerPartition,
      ignoreUnknownIds = true,
      deleteMissingAssets = false,
      subtrees = AssetSubtreeOption.Ingest,
      ignoreNullFields = true,
      rawEnsureParent = false
    )

  private def getCounter(metricName: String): Long =
    MetricsSource
      .metricsMap(metricName)
      .value
      .asInstanceOf[Counter]
      .getCount

  def getNumberOfRowsRead(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.read")

  def getNumberOfRowsCreated(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.created")

  def getNumberOfRequests(metricsPrefix: String): Long =
    getCounter(s"$metricsPrefix.requestsWithoutRetries")

  def getNumberOfRowsDeleted(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.deleted")

  def getNumberOfRowsUpdated(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.updated")

  def getPartitionSize(metricsPrefix: String, resourceType: String, partitionIndex: Int): Long = {
    val metricName = s"$metricsPrefix.$resourceType.$partitionIndex.partitionSize"
    if (MetricsSource.metricsMap.contains(metricName)) {
      getCounter(metricName)
    } else {
      0
    }
  }

  def sparkIntercept(f: => Any)(implicit pos: source.Position): Throwable = {
    Matchers.intercept[Exception](f)(classTag[Exception], pos) match {
      case ex : SparkException if ex.getCause != null =>
        ex.getCause
      case ex => ex
    }
  }
}
