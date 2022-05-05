package org.apache.spark.datasource

import cats.Eval
import com.codahale.metrics._
import org.apache.spark._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class MetricsSource {
  // Add metricNamespace to differentiate with spark system metrics.
  // Keeps track of all the Metric instances that are being published
  val metricsMap = new ConcurrentHashMap[String, Eval[Metric]]().asScala

  def getOrElseUpdate(metricNamespace: String, metricName: String, metric: => Metric): Metric = {
    val wrapped = Eval.later(metric)
    metricsMap.putIfAbsent(s"${metricNamespace}.${metricName}", wrapped) match {
      case Some(wrappedMetric) => wrappedMetric.value
      case None => {
        registerMetricSource(metricNamespace, metricName, wrapped.value)
        wrapped.value
      }
    }
  }

  def getOrCreateCounter(metricNamespace: String, metricName: String): Counter =
    getOrElseUpdate(metricNamespace, metricName, new Counter).asInstanceOf[Counter]

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
  def registerMetricSource(metricNamespace: String, metricName: String, metric: Metric): Unit = {
    val env = SparkEnv.get
    env.metricsSystem.registerSource(
      new Source {
        override val sourceName = s"${metricNamespace}"
        override def metricRegistry: MetricRegistry = {
          val metrics = new MetricRegistry
          metrics.register(metricName, metric)
          metrics
        }
      }
    )
  }
}

// Singleton to make sure each metric is only registered once.
object MetricsSource extends MetricsSource
