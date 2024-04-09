package org.apache.spark.datasource

import cats.Eval
import com.codahale.metrics._
import org.apache.spark._

import java.util.concurrent.ConcurrentHashMap

class MetricsSource {
  // Add metricNamespace to differentiate with spark system metrics.
  // Keeps track of all the Metric instances that are being published
  val metricsMap = new ConcurrentHashMap[String, Eval[Counter]]

  def getOrCreateCounter(metricNamespace: String, name: String): Counter = {
    val ctx = Option(TaskContext.get())

    def getNumberOrEmpty(getter: TaskContext => AnyVal): String =
      ctx.map(getter).map(_.toString()).getOrElse("")

    // The number of fields in the name should be consistent, even if there is no value for
    // the attempt numbers
    val stageId = getNumberOrEmpty(_.stageId())
    val stageAttempt = getNumberOrEmpty(_.stageAttemptNumber())
    val partitionId = getNumberOrEmpty(_.partitionId())
    val taskAttempt = getNumberOrEmpty(_.taskAttemptId())

    val metricName = s"$stageId.$stageAttempt.$partitionId.$taskAttempt.$name"
    val key = s"$metricNamespace.$metricName"

    val wrapped = Eval.later {
      val counter = new Counter
      registerMetricSource(metricNamespace, metricName, counter)
      counter
    }

    metricsMap.putIfAbsent(key, wrapped)
    metricsMap.get(key).value
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
