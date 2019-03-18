package org.apache.spark.datasource

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap

import com.codahale.metrics._
import org.apache.spark._

class MetricsSource(val metricNamespace: String) {

  class LazyWrapper[T](wrapped: => T) {
    lazy val value = wrapped
  }

  private def wrap[T](value: => T): LazyWrapper[T] =
    new LazyWrapper[T](value)

  private def unwrap[T](lazyWrapper: LazyWrapper[T]): T =
    lazyWrapper.value

  // Keeps track of all the Metric instances that are being published
  val metricsMap = new ConcurrentHashMap[String, LazyWrapper[Metric]]().asScala

  def getOrElseUpdate(metricName: String, metric: => Metric): Metric = {
    val wrapped = wrap(metric)
    metricsMap.putIfAbsent(metricName, wrapped) match {
      case Some(wrappedMetric) => wrappedMetric.value
      case None => {
        registerMetricSource(metricName, wrapped.value)
        wrapped.value
      }
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
        override val sourceName = s"${env.conf.getAppId}.$metricNamespace.${env.executorId}"
        override def metricRegistry: MetricRegistry = {
          val metrics = new MetricRegistry
          metrics.register(metricName, metric)
          metrics
        }
      }
    )
  }
}
