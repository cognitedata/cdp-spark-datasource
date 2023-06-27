package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.common.OAuth2
import com.cognite.sdk.scala.v1._
import org.apache.spark.SparkException
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Encoder, SparkSession}
import org.scalactic.{Prettifier, source}
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableFor1
import org.scalatest.{Matchers, Tag}
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import sttp.client3.{SttpBackend, UriContext}

import java.io.IOException
import java.util.UUID
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.util.Random

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")

trait SparkTest {
  import CdpConnector.ioRuntime
  import natchez.Trace.Implicits.noop

  implicit def single[A](
      implicit c: ClassTag[OptionalField[A]],
      inner: Encoder[Option[A]]): Encoder[OptionalField[A]] =
    new Encoder[OptionalField[A]] {
      override def schema: StructType = inner.schema

      override def clsTag: ClassTag[OptionalField[A]] = c
    }

  object OIDCWrite {
    val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
    val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
    private val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
    val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
    val project = "jetfiretest2"
    val scopes = "https://api.cognitedata.com/.default"
  }

  val writeCredentials = OAuth2.ClientCredentials(
    tokenUri = sttp.model.Uri.unsafeParse(OIDCWrite.tokenUri),
    clientId = OIDCWrite.clientId,
    clientSecret = OIDCWrite.clientSecret,
    scopes = List(OIDCWrite.scopes),
    OIDCWrite.project
  )
  implicit val sttpBackend: SttpBackend[IO, Any] = AsyncHttpClientCatsBackend[IO]().unsafeRunSync()

  val writeAuthProvider =
    OAuth2.ClientCredentialsProvider[IO](writeCredentials).unsafeRunTimed(1.second).get
  val writeClient: GenericClient[IO] = new GenericClient(
    applicationName = "jetfire-test",
    projectName = writeCredentials.cdfProjectName,
    baseUrl = s"https://api.cognitedata.com",
    authProvider = writeAuthProvider,
    apiVersion = None,
    clientTag = None,
    cdfVersion = None
  )

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

  private val readClientId = System.getenv("TEST_OIDC_READ_CLIENT_ID")
  // readClientSecret has to be renewed every 180 days at https://hub.cognite.com/open-industrial-data-211
  private val readClientSecret = System.getenv("TEST_OIDC_READ_CLIENT_SECRET")
  private val readAadTenant = System.getenv("TEST_OIDC_READ_TENANT")

  assert(
    readClientId != null && !readClientId.isEmpty,
    "Environment variable \"TEST_OIDC_READ_CLIENT_ID\" was not set")
  assert(
    readClientSecret != null && !readClientSecret.isEmpty,
    "Environment variable \"TEST_OIDC_READ_CLIENT_SECRET\" was not set")
  assert(
    readAadTenant != null && !readAadTenant.isEmpty,
    "Environment variable \"TEST_OIDC_READ_TENANT\" was not set")

  private val readTokenUri = s"https://login.microsoftonline.com/$readAadTenant/oauth2/v2.0/token"

  val readOidcCredentials = OAuth2.ClientCredentials(
    tokenUri = uri"$readTokenUri",
    clientId = readClientId,
    clientSecret = readClientSecret,
    scopes = List("https://api.cognitedata.com/.default"),
    cdfProjectName = "publicdata"
  )

  def dataFrameReaderUsingOidc: DataFrameReader =
    spark.read
      .format("cognite.spark.v1")
      .option("tokenUri", readTokenUri)
      .option("clientId", readClientId)
      .option("clientSecret", readClientSecret)
      .option("project", "publicdata")
      .option("scopes", "https://api.cognitedata.com/.default")

  // not needed to run tests, only for replicating some problems specific to this tenant
  lazy val jetfiretest2ApiKey = System.getenv("TEST_APU_KEY_JETFIRETEST2")

  val testDataSetId = 86163806167772L

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false") // comment this out to use Spark UI during tests, on https://localhost:4040 by default
    // https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.storeAssignmentPolicy", "legacy")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random() * 1000).toLong.toString)
    .getOrCreate()

  // We have many tests with expected Spark errors. Remove this if you're troubleshooting a test.
  spark.sparkContext.setLogLevel("OFF")

  val bluefieldClientId: String = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val bluefieldClientSecret: String = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val bluefieldAADTenant: String = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val bluefieldTokenUriStr = s"https://login.microsoftonline.com/$bluefieldAADTenant/oauth2/v2.0/token"
  val bluefieldTokenUri = uri"https://login.microsoftonline.com/$bluefieldAADTenant/oauth2/v2.0/token"

  def getBlufieldClient(cdfVersion: Option[String] = None): GenericClient[IO] = {
    implicit val sttpBackend: SttpBackend[IO, Any] = AsyncHttpClientCatsBackend[IO]().unsafeRunSync()
    val credentials = OAuth2.ClientCredentials(
      tokenUri = bluefieldTokenUri,
      clientId = bluefieldClientId,
      clientSecret = bluefieldClientSecret,
      scopes = List("https://bluefield.cognitedata.com/.default"),
      cdfProjectName = "extractor-bluefield-testing"
    )

    val authProvider =
      OAuth2.ClientCredentialsProvider[IO](credentials).unsafeRunTimed(1.second).get
    new GenericClient[IO](
      applicationName = "CogniteScalaSDK-OAuth-Test",
      projectName = "extractor-bluefield-testing",
      baseUrl = "https://bluefield.cognitedata.com",
      authProvider = authProvider,
      apiVersion = None,
      clientTag = None,
      cdfVersion = cdfVersion
    )
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

  def getDefaultConfig(auth: CdfSparkAuth, projectName: String): RelationConfig =
    RelationConfig(
      auth,
      Some("SparkDatasourceTestTag"),
      Some("SparkDatasourceTestApp"),
      projectName,
      Some(Constants.DefaultBatchSize),
      None,
      partitions = Constants.DefaultPartitions,
      Constants.DefaultMaxRetries,
      Constants.DefaultMaxRetryDelaySeconds,
      collectMetrics = false,
      collectTestMetrics = false,
      "",
      Constants.DefaultBaseUrl,
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
