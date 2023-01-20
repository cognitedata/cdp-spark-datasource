package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.wdl.{TestWdlClient, WdlClient}
import com.cognite.sdk.scala.common.{ApiKeyAuth, OAuth2}
import org.apache.spark.SparkException
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Encoder, SparkSession}
import org.scalactic.{Prettifier, source}
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableFor1
import sttp.client3.SttpBackend
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend

import java.io.IOException
import java.util.UUID
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.util.Random

trait WDLSparkTest {
  import CdpConnector.ioRuntime

  val useLocalWellDataLayer = true

  implicit def single[A](
      implicit c: ClassTag[OptionalField[A]],
      inner: Encoder[Option[A]]): Encoder[OptionalField[A]] =
    new Encoder[OptionalField[A]] {
      override def schema: StructType = inner.schema

      override def clsTag: ClassTag[OptionalField[A]] = c
    }

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false") // comment this out to use Spark UI during tests, on https://localhost:4040 by default
    // https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.storeAssignmentPolicy", "legacy")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random() * 1000).toLong.toString)
    //    .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", value = true)
    .getOrCreate()

  // We have many tests with expected Spark errors. Remove this if you're troubleshooting a test.
  spark.sparkContext.setLogLevel("OFF")

  object OIDCWrite {
    val clientId: String = sys.env("TEST_CLIENT_ID_BLUEFIELD")
    val clientSecret: String = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
    private val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
    val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
    val project = "jetfiretest2"
    val scopes = "https://api.cognitedata.com/.default"
  }

  val writeCredentials: OAuth2.ClientCredentials = OAuth2.ClientCredentials(
    tokenUri = sttp.model.Uri.unsafeParse(OIDCWrite.tokenUri),
    clientId = OIDCWrite.clientId,
    clientSecret = OIDCWrite.clientSecret,
    scopes = List(OIDCWrite.scopes),
    OIDCWrite.project
  )

  protected val config: RelationConfig = if (useLocalWellDataLayer) {
    getDefaultConfig(CdfSparkAuth.Static(ApiKeyAuth("something something")))
  } else {
    implicit val sttpBackend: SttpBackend[IO, Any] = AsyncHttpClientCatsBackend[IO]().unsafeRunSync()
    getDefaultConfig(CdfSparkAuth.OAuth2ClientCredentials(writeCredentials))
  }
  protected val wdlClient: WdlClient = WdlClient.fromConfig(config)
  protected val client = new TestWdlClient(wdlClient)

  implicit class DataFrameWriterHelper[T](df: DataFrameWriter[T]) {
    def useOIDCWrite: DataFrameWriter[T] =
      df.option("tokenUri", OIDCWrite.tokenUri)
        .option("clientId", OIDCWrite.clientId)
        .option("clientSecret", OIDCWrite.clientSecret)
        .option("project", OIDCWrite.project)
        .option("scopes", OIDCWrite.scopes)
  }

  implicit class DataFrameReaderHelper(df: DataFrameReader) {
    def useOIDCWrite: DataFrameReader =
      df.option("tokenUri", OIDCWrite.tokenUri)
        .option("clientId", OIDCWrite.clientId)
        .option("clientSecret", OIDCWrite.clientSecret)
        .option("project", OIDCWrite.project)
        .option("scopes", OIDCWrite.scopes)
  }

  def shortRandomString(): String = UUID.randomUUID().toString.substring(0, 8)

  // scalastyle:off cyclomatic.complexity
  def retryWithBackoff[A](
      ioa: IO[A],
      initialDelay: FiniteDuration,
      maxRetries: Int,
      maxDelay: FiniteDuration = 20.seconds): IO[A] = {
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

  def retryWhile[A](action: => A, shouldRetry: A => Boolean)(
      implicit prettifier: Prettifier,
      pos: source.Position): A =
    retryWithBackoff(
      IO {
        val actionValue = action
        if (shouldRetry(actionValue)) {
          throw new TimeoutException(
            s"Retry timed out at ${pos.fileName}:${pos.lineNumber}, value = ${prettifier(actionValue)}")
        }
        actionValue
      },
      1.second,
      20
    ).unsafeRunTimed(5.minutes).getOrElse(throw new RuntimeException("Test timed out during retries"))

  val updateAndUpsert: TableFor1[String] = Table(heading = "mode", "upsert", "update")

  def getDefaultConfig(auth: CdfSparkAuth): RelationConfig =
    RelationConfig(
      auth,
      Some("SparkDatasourceTestTag"),
      Some("SparkDatasourceTestApp"),
      "jetfiretest2",
//      DefaultSource.getProjectFromAuth(auth, Constants.DefaultBaseUrl)(
//        CdpConnector
//          .retryingSttpBackend(Constants.DefaultMaxRetries, Constants.DefaultMaxRetryDelaySeconds)),
      Some(Constants.DefaultBatchSize),
      None,
      Constants.DefaultPartitions,
      Constants.DefaultMaxRetries,
      Constants.DefaultMaxRetryDelaySeconds,
      collectMetrics = false,
      collectTestMetrics = false,
      "",
      if (useLocalWellDataLayer) {
        "http://localhost:8080"
      } else {
        Constants.DefaultBaseUrl
      },
      OnConflictOption.Abort,
      spark.sparkContext.applicationId,
      Constants.DefaultParallelismPerPartition,
      ignoreUnknownIds = true,
      deleteMissingAssets = false,
      subtrees = AssetSubtreeOption.Ingest,
      ignoreNullFields = true,
      rawEnsureParent = false
    )

  private def getCounter(metricName: String): Long =
    MetricsSource.metricsMap
      .get(metricName)
      .value
      .getCount

  def getNumberOfRowsRead(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.read")

  def getNumberOfRowsCreated(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.created")

  def getNumberOfRowsUpserted(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.upserted")

  def getNumberOfRequests(metricsPrefix: String): Long =
    getCounter(s"$metricsPrefix.requestsWithoutRetries")

  def getNumberOfRowsDeleted(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.deleted")

  def getNumberOfRowsUpdated(metricsPrefix: String, resourceType: String): Long =
    getCounter(s"$metricsPrefix.$resourceType.updated")

  def getPartitionSize(metricsPrefix: String, resourceType: String, partitionIndex: Long): Long = {
    val metricName = s"$metricsPrefix.$resourceType.$partitionIndex.partitionSize"
    if (MetricsSource.metricsMap.containsKey(metricName)) {
      getCounter(metricName)
    } else {
      0
    }
  }

  def sparkIntercept(f: => Any)(implicit pos: source.Position): Throwable =
    Matchers.intercept[Throwable](f)(classTag[Throwable], pos) match {
      case ex: SparkException if ex.getCause != null =>
        ex.getCause
      case ex => ex
    }
}
