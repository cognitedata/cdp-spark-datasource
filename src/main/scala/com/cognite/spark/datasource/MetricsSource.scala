package org.apache.spark.datasource

import scala.collection.JavaConverters._

import com.codahale.metrics._
import org.apache.spark._

class MetricsSource(val metricNamespace: String) extends Serializable {
  def getOrElseUpdate(metricName: String, metric: Metric): Metric = {
    val sources = SparkEnv.get.metricsSystem.getSourcesByName(metricNamespace)
    val x = sources.map(_.metricRegistry.getCounters.asScala.get(metricName)).flatten

    if (x.isEmpty) {
      registerMetricSource(metricName, metric)
      metric
    } else {
      x.head
    }
  }

  def getOrCreateCounter(metricName: String): Counter =
    getOrElseUpdate(metricName, new Counter).asInstanceOf[Counter]

  /**
    * Register a [[Metric]] with Spark's [[org.apache.spark.metrics.MetricsSystem]].
    *
    * Since updates to an external [[MetricRegistry]] that is already registered with the
    * [[org.apache.spark.metrics.MetricsSystem]] aren't propagated to Spark's internal [[MetricRegistry]] instance, a new
    * [[MetricRegistry]] must be created for each new [[Metric]] that needs to be published.
    *
    * @param metricName name of the Metric
    * @param metric [[Metric]] instance to be published
    */
  def registerMetricSource(metricName: String, metric: Metric): Unit = {
    val env = SparkEnv.get
    env.metricsSystem.registerSource(
      new Source {
        override val sourceName = s"$metricNamespace"
        override def metricRegistry: MetricRegistry = {
          val metrics = new MetricRegistry
          metrics.register(metricName, metric)
          metrics
        }
      }
    )
  }
}
