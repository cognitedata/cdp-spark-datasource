package cognite.spark.performancebench

import io.prometheus.client.Histogram

object Metrics {
  val testTimeHistogram = Histogram
    .build()
    .name("cognite_cdf_spark_test_seconds")
    .buckets(1, 5, 10, 20, 40, 50, 60)
    .labelNames("testName")
    .help("The number of seconds a benchmark test takes. The 'testName' label stores the test ran.")
    .register()
}
