package cognite.spark.performancebench

import io.prometheus.client.Summary

object Metrics {
  // We don't really know the buckets, so summary is a better metric type than histogram
  val testTimeSummary = Summary
    .build()
    .name("cognite_cdf_spark_test_seconds")
    .labelNames("testName")
    .help("The number of seconds a benchmark test takes. The 'testName' label stores the test ran.")
    .register()
}
