package cognite.spark.performancebench

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class PerformanceTest[A](testName: String, beforeTest: () => A, test: A => Unit)

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
