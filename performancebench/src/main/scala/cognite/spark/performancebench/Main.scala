package cognite.spark.performancebench

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.effect.{ExitCode, IO, IOApp}
import io.prometheus.client.Histogram
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait SparkUtil {
  val spark = SparkUtil.spark

  def read(): DataFrameReader =
    spark.read
      .format("cognite.spark.v1")
      .option(
        "apiKey",
        sys.env.getOrElse("COGNITE_API_KEY", throw new Exception("'COGNITE_API_KEY' is not set")))

  def write(df: DataFrame): DataFrameWriter[Row] =
    df.write
      .format("cognite.spark.v1")
      .option(
        "apiKey",
        sys.env.getOrElse("COGNITE_API_KEY", throw new Exception("'COGNITE_API_KEY' is not set")))
}

object SparkUtil {
  lazy val spark = SparkSession.builder
    .appName("CDF Spark Performance Benchmark")
    .master("local[*]")
    .getOrCreate()
}

case class PerformanceTest[A](testName: String, beforeTest: () => A, test: A => Unit)

object Metrics {
  val testTimeHistogram = Histogram
    .build()
    .name("cognite_cdf_spark_test_seconds")
    .buckets(1, 5, 10, 20, 40, 50, 60)
    .labelNames("testName")
    .help("The number of seconds a benchmark test takes. The 'testName' label stores the test ran.")
    .register()
}

abstract class PerformanceSuite extends SparkUtil {
  val tests: mutable.MutableList[PerformanceTest[Any]] = new mutable.MutableList[PerformanceTest[Any]]()
  def registerTest(testName: String, test: () => Unit): Unit =
    tests += PerformanceTest(testName, () => (), _ => test())

  def registerTest[A](testName: String, beforeTest: () => A, test: A => Unit): Unit =
    tests += PerformanceTest[Any](testName, beforeTest, a => test(a.asInstanceOf[A]))

  def run(): Unit =
    tests.foreach(perfTest => {
      println(s"Starting ${perfTest.testName}")
      val testResult = for {
        beforeTestResult <- Try(perfTest.beforeTest())
        testResult <- Try(
          Metrics.testTimeHistogram
            .labels(perfTest.testName)
            .time(new Runnable() { def run() = perfTest.test(beforeTestResult) }))
      } yield testResult
      println(s"Done with ${perfTest.testName}")

      testResult match {
        case Success(value) => ()
        case Failure(exception) => println(s"Failed to run ${perfTest.testName}: $exception")
      }
    })
}

class EventPerformance extends PerformanceSuite {
  import spark.implicits._
  private def readEvents() =
    read()
      .option("type", "events")
      .load()

  private val eventsTestSchema =
    Seq("type", "subtype", "externalId", "description", "startTime", "endTime")
  val externalIdPrefix = s"cdf-spark-perf-test"
  private val eventsTestData = (0 until 1000000)
    .map(
      id =>
        (
          "cdf-spark-perf-test",
          "test",
          s"$externalIdPrefix$id",
          s"This is a test row ($id)",
          java.sql.Timestamp.from(Instant.now().minus(Math.min(id, 100), ChronoUnit.HOURS)),
          java.sql.Timestamp.from(Instant.now().plus(Math.min(id, 100), ChronoUnit.HOURS))))

  private def writeEvents(): Unit =
    write(spark.sparkContext.parallelize(eventsTestData, 300).toDF(eventsTestSchema: _*))
      .option("type", "events")
      .option("onconflict", "upsert")
      .save()

  private def prepareForDelete(): Array[Row] =
    readEvents()
      .where(s"externalId LIKE 'externalIdPrefix%'")
      .select($"id")
      .collect()

  private def deleteEvents(rows: Array[Row]): Unit =
    write(spark.sparkContext.parallelize(rows.map(r => r.getAs[Long](0))).toDF("id"))
      .option("type", "events")
      .option("onconflict", "delete")
      .save()

  registerTest("writeEvents", writeEvents)
  registerTest("readEvents", () => readEvents().collect())
  registerTest("upsertEvents", writeEvents)
  registerTest("deleteEvents", prepareForDelete, deleteEvents)
}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val eventPerf = new EventPerformance()
    eventPerf.run()
    sys.exit(0)
    IO.pure(ExitCode.Success)
  }
}
