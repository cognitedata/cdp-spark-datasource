package org.apache.spark.datasource

import cats.Eval
import com.codahale.metrics._
import java.util.concurrent.ConcurrentHashMap
import java.util.{Timer, TimerTask}
import org.apache.spark._
import scala.concurrent.duration._
import scala.language.postfixOps

class MetricsSource {
  // Add metricNamespace to differentiate with spark system metrics.
  // Keeps track of all the Metric instances that are being published
  val metricsMap = new ConcurrentHashMap[String, Eval[ExpiringCounter]]

  def getOrCreateCounter(
      metricNamespace: String,
      stageAttempt: Int,
      taskAttempt: Int,
      name: String): Counter = {
    val metricName = s"$stageAttempt.$taskAttempt.$name"
    val key = s"$metricNamespace.$metricName"

    val wrapped = Eval.later {
      val counter = new Counter
      val source = registerMetricSource(metricNamespace, metricName, counter)
      new ExpiringCounter(key, source)
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
  private def registerMetricSource(
      metricNamespace: String,
      metricName: String,
      metric: Metric): Source = {
    val source =
      new Source {
        override val sourceName = s"${metricNamespace}"
        override def metricRegistry: MetricRegistry = {
          val metrics = new MetricRegistry
          metrics.register(metricName, metric)
          metrics
        }
      }

    SparkEnv.get.metricsSystem.registerSource(source)

    source
  }

  def deregisterMetricSource(key: String, source: Source): Unit = {
    metricsMap.remove(key)
    SparkEnv.get.metricsSystem.removeSource(source)
  }
}

// Singleton to make sure each metric is only registered once.
object MetricsSource extends MetricsSource

class ExpiringCounter(key: String, source: Source) extends Counter {
  private var (deathTimer, deathTimerTask): (Timer, TimerTask) = createTimer

  private def createTimer = {
    val task = new TimerTask {
      override def run =
        MetricsSource.deregisterMetricSource(key, source)
    }

    val timer = new Timer(key)
    timer.schedule(task, (1 hour).toMillis)

    (timer, task)
  }

  private def resetTimer() = {
    deathTimerTask.cancel()
    deathTimer.cancel()

    val (timer, task) = createTimer
    deathTimer = timer
    deathTimerTask = task
  }

  override def inc(n: Long): Unit = {
    resetTimer()
    super.inc(n)
  }

  override def dec(n: Long): Unit = {
    resetTimer()
    super.dec(n)
  }

}
