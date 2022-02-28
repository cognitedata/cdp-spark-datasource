package cognite.spark.performancebench

import io.prometheus.client.{Counter, Gauge}

object Metrics {
  val testTimeMetric = Gauge
    .build()
    .name("cognite_cdf_spark_test_seconds")
    .labelNames("test_name", "success")
    .help("The number of seconds a benchmark test takes. The 'test_name' label stores the test ran.")
    .register()

  val testFailedCounter = Counter
    .build()
    .name("cognite_cdf_spark_test_failed")
    .help("The number of failed benchmark tests.")
    .register()
}
