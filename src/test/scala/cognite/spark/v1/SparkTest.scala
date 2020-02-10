package cognite.spark.v1

import java.io.IOException
import java.util.UUID

import cats.Id
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.common.{ApiKeyAuth, Auth}
import org.apache.spark.sql.SparkSession
import org.scalatest.Tag
import org.apache.spark.datasource.MetricsSource

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration._
import scala.util.Random
import cats.effect.{IO, Timer}
import cats.implicits._
import com.cognite.sdk.scala.v1._

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")
object GreenfieldTest extends Tag("GreenfieldTest")

trait SparkTest {
  implicit lazy val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")
  implicit val writeApiKeyAuth: ApiKeyAuth = ApiKeyAuth(writeApiKey)
  val writeClient = GenericClient.forAuth[Id, Nothing]("cdp-spark-datasource-test", writeApiKeyAuth)

  val readApiKey = System.getenv("TEST_API_KEY_READ")
  implicit val readApiKeyAuth: ApiKeyAuth = ApiKeyAuth(readApiKey)
  val readClient = GenericClient.forAuth[Id, Nothing]("cdp-spark-datasource-test", readApiKeyAuth)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    // https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random * 1000).toLong.toString)
    .getOrCreate()

  enableSparkLogging()

  def shortRandomString(): String = UUID.randomUUID().toString.substring(0, 8)

  // scalastyle:off cyclomatic.complexity
  def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int): IO[A] = {
    val exponentialDelay = (Constants.DefaultMaxBackoffDelay / 2).min(initialDelay * 2)
    val randomDelayScale = (Constants.DefaultMaxBackoffDelay / 2).min(initialDelay * 2).toMillis
    val nextDelay = Random.nextInt(randomDelayScale.toInt).millis + exponentialDelay
    ioa.handleErrorWith {
      case exception @ (_: TimeoutException | _: IOException) =>
        if (maxRetries > 0) {
          IO.sleep(initialDelay) *> retryWithBackoff(ioa, nextDelay, maxRetries - 1)
        } else {
          IO.raiseError(exception)
        }
      case error => IO.raiseError(error)
    }
  }
  // scalastyle:on cyclomatic.complexity

  def retryWhile[A](action: => A, shouldRetry: A => Boolean): A =
    retryWithBackoff(
      IO {
        val actionValue = action
        if (shouldRetry(actionValue)) {
          throw new TimeoutException("Retry")
        }
        actionValue
      },
      Constants.DefaultInitialRetryDelay,
      Constants.DefaultMaxRetries
    ).unsafeRunSync()

  def getDefaultConfig(auth: Auth): RelationConfig =
    RelationConfig(
      auth,
      DefaultSource.getProjectFromAuth(auth, Constants.DefaultMaxRetries, Constants.DefaultBaseUrl),
      Some(Constants.DefaultBatchSize),
      None,
      Constants.DefaultPartitions,
      Constants.DefaultMaxRetries,
      collectMetrics = false,
      "",
      Constants.DefaultBaseUrl,
      OnConflict.Abort,
      spark.sparkContext.applicationId,
      Constants.DefaultParallelismPerPartition,
      ignoreUnknownIds = true,
      deleteMissingAssets = false,
      ignoreDisconnectedAssets = false
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

  def getNumberOfRowsUpdated(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.updated")

  def disableSparkLogging(): Unit =
    spark.sparkContext.setLogLevel("OFF")

  def enableSparkLogging(): Unit =
    spark.sparkContext.setLogLevel("ERROR")
}
