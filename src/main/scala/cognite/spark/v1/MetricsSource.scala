package org.apache.spark.datasource

import cats.Eval
import com.codahale.metrics._
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark._

class MetricsSource {
  // Add metricNamespace to differentiate with spark system metrics.
  // Keeps track of all the Metric instances that are being published
  val metricsMap = new ConcurrentHashMap[String, Eval[Counter]]

  def getOrCreateAttemptTrackingCounter(
      trackAttempts: Boolean,
      metricNamespace: String,
      name: String,
      ctx: Option[TaskContext]): Counter = {

    val metricName = if (trackAttempts) {
      def getNumberOrEmpty(getter: TaskContext => Long): String =
        ctx.map(getter).map(_.toString()).getOrElse("")

      // The number of fields in the name should be consistent, even if there is no value for
      // the attempt numbers
      val stageId = getNumberOrEmpty(_.stageId().toLong)
      val stageAttempt = getNumberOrEmpty(_.stageAttemptNumber().toLong)
      val partitionId = getNumberOrEmpty(_.partitionId().toLong)
      val taskAttempt = getNumberOrEmpty(_.taskAttemptId())

      s":sid:$stageId:sat:$stageAttempt:pid:$partitionId:tat:$taskAttempt:$name"
    } else {
      name
    }
    val wrapped = Eval.later {
      val counter = new Counter
      registerMetricSource(metricNamespace, metricName, counter)
      counter
    }
    val key = s"$metricNamespace.$metricName"
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
object MetricsSource extends MetricsSource {}

case class AttemptTrackingMetricName(
    stageId: Option[String],
    stageAttempt: Option[String],
    partitionId: Option[String],
    taskAttempt: Option[String],
    name: String) {}

object AttemptTrackingMetricName {
  private val regex = "^(?::sid:([^:]*):sat:([^:]*):pid:([^:]*):tat:([^:]*):)?(.*)$".r

  def parse(name: String): AttemptTrackingMetricName =
    name match {
      case regex(stageId, stageAttempt, partitionId, taskAttempt, name) => {
        AttemptTrackingMetricName(
          Option(stageId).filterNot(_.isEmpty),
          Option(stageAttempt).filterNot(_.isEmpty),
          Option(partitionId).filterNot(_.isEmpty),
          Option(taskAttempt).filterNot(_.isEmpty),
          name
        )
      }
      case _ => {
        AttemptTrackingMetricName(None, None, None, None, name)
      }
    }
}
