package org.apache.spark.datasource

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession

class MetricsSourceTest extends FlatSpec with Matchers {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false") // comment this out to use Spark UI during tests, on https://localhost:4040 by default
    // https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.storeAssignmentPolicy", "legacy")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random() * 1000).toLong.toString)
    .getOrCreate()

  "A MetricsSource" should "register a new metric only once" in {
    val metric = MetricsSource.getOrCreateCounter("prefix", "name")
    val sameMetric = MetricsSource.getOrCreateCounter("prefix", "name")

    metric.getCount() should equal(0)
    sameMetric.getCount() should equal(0)

    metric.inc()

    metric.getCount() should equal(1)
    sameMetric.getCount() should equal(1)
  }

  it should "deregister a metric only once" in {
    MetricsSource.getOrCreateCounter("removePrefix.jobId", "name")
    MetricsSource.getOrCreateCounter("removePrefix.otherJobId", "name")

    Option(MetricsSource.metricsMap.get("removePrefix.jobId.name")).isDefined shouldBe true
    Option(MetricsSource.metricsMap.get("removePrefix.otherJobId.name")).isDefined shouldBe true
    SparkEnv.get.metricsSystem.getSourcesByName("removePrefix.jobId").size should equal(1)
    SparkEnv.get.metricsSystem.getSourcesByName("removePrefix.otherJobId").size should equal(1)

    MetricsSource.removeJobMetrics("removePrefix.jobId")

    Option(MetricsSource.metricsMap.get("removePrefix.jobId.name")).isDefined shouldBe false
    Option(MetricsSource.metricsMap.get("removePrefix.otherJobId.name")).isDefined shouldBe true
    SparkEnv.get.metricsSystem.getSourcesByName("removePrefix.jobId").size should equal(0)
    SparkEnv.get.metricsSystem.getSourcesByName("removePrefix.otherJobId").size should equal(1)
  }
}
