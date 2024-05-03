package org.apache.spark.datasource

import cats.Eval
import com.codahale.metrics._
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.SparkEnv
import org.log4s.getLogger
import scala.collection.JavaConverters._

class MetricsSource {
  // Add metricPrefix to differentiate with spark system metrics.
  // Keeps track of all the Metric instances that are being published
  val metricsMap = new ConcurrentHashMap[String, Eval[SourceCounter]]

  def getOrCreateCounter(metricPrefix: String, metricName: String): Counter = {
    val key = s"$metricPrefix.$metricName"

    val wrapped = Eval.later {
      getLogger.info(s"Creating and registering counter for $key")
      val counter = new Counter
      val source = registerMetricSource(metricPrefix, metricName, counter)
      SourceCounter(source, counter)
    }

    metricsMap.putIfAbsent(key, wrapped)
    metricsMap.get(key).value.counter
  }

  /**
    * Register a metric with Spark's org.apache.spark.metrics.MetricsSystem.
    *
    * Since updates to an external MetricRegistry that is already registered with the
    * org.apache.spark.metrics.MetricsSystem aren't propagated to Spark's internal MetricRegistry instance,
    * a new MetricRegistry must be created for each new Metric that needs to be published.
    *
    * @param metricName name of the Metric
    * @param metric com.codahale.metrics.Metric instance to be published
    */
  def registerMetricSource(metricPrefix: String, metricName: String, metric: Metric): Source = {
    val env = SparkEnv.get
    val source = new Source {
      override val sourceName = s"${metricPrefix}"
      override def metricRegistry: MetricRegistry = {
        val metrics = new MetricRegistry
        metrics.register(metricName, metric)
        metrics
      }
    }
    env.metricsSystem.registerSource(source)
    source
  }

  /**
    * Remove the metrics from a job.
    *
    * This method will deregister the metric from Spark's org.apache.spark.metrics.MetricsSystem
    * and stops tracking that it was published
    */
  def removeJobMetrics(metricPrefix: String): Unit = {
    val removed =
      metricsMap
        .keys()
        .asScala
        .filter(k => k.startsWith(metricPrefix))
        .map(k => (k, metricsMap.remove(k)))

    if (removed.nonEmpty) {
      val logger = getLogger
      val metricsSystem = SparkEnv.get.metricsSystem
      removed
        .map({ case (k, v) => (k, v.value.source) })
        .foreach({
          case (key, source) => {
            logger.info(s"Deleting and removing counter for $key")
            metricsSystem.removeSource(source)
          }
        })
    }
  }
}

// Singleton to make sure each metric is only registered once.
object MetricsSource extends MetricsSource

case class SourceCounter(source: Source, counter: Counter)
