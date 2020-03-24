package cognite.spark.performancebench

import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.{DefaultExports => PrometheusDefaultExports}
import org.log4s._

object Main extends App {
  val logger = getLogger
  val metricsServer = new HTTPServer(8123, true)
  PrometheusDefaultExports.initialize()
  try {
    val eventPerf = new EventPerformance()
    eventPerf.run()
  } finally {
    logger.info("Sleeping for 20 seconds before exiting, so that all metrics are scraped.")
    Thread.sleep(20 * 1000) // Sleep for 20 seconds to make sure metrics are scraped
    metricsServer.stop()
  }
}
