package cognite.spark.performancebench

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import org.log4s._

case class PerformanceTest[A](testName: String, beforeTest: () => A, test: A => Unit)

abstract class PerformanceSuite extends SparkUtil {
  val logger = getLogger
  val tests: mutable.MutableList[PerformanceTest[Any]] = new mutable.MutableList[PerformanceTest[Any]]()
  def registerTest(testName: String, test: () => Unit): Unit =
    tests += PerformanceTest(testName, () => (), _ => test())

  def registerTest[A](testName: String, beforeTest: () => A, test: A => Unit): Unit =
    tests += PerformanceTest[Any](testName, beforeTest, a => test(a.asInstanceOf[A]))

  def run(): Unit =
    tests.foreach(perfTest => {
      val testResult = for {
        beforeTestResult <- Try(perfTest.beforeTest())
        testResult <- {
          logger.info(s"${perfTest.testName}: starting")
          val startTime = System.currentTimeMillis()
          val res = Try(perfTest.test(beforeTestResult))
          val duration = (System.currentTimeMillis() - startTime).toDouble / 1000.0
          val wasSuccess: Boolean = res.toOption.isDefined
          Metrics.testTimeMetric
            .labels(perfTest.testName, wasSuccess.toString)
            .set(duration)
          val durationAsString = f"$duration%1.2f"
          logger.info(s"${perfTest.testName}: finished after ${durationAsString} secs")
          res
        }
      } yield testResult

      testResult match {
        case Success(_) => ()
        case Failure(exception) => logger.error(exception)(s"${perfTest.testName}: failed to run")
      }
    })
}
