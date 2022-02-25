package cognite.spark.cdfdump

import cats.effect.IO
import com.codahale.metrics.Counting
import org.apache.spark.datasource.MetricsSource
import org.log4s.getLogger

import java.time.{Duration, Instant}
import java.util.Locale
import scala.concurrent.duration.FiniteDuration
import scala.math.pow

class MetricsWatcher {
  private var lastState: Map[String, Long] = Map()
  private val startTime = Instant.now
  private var lastTime = startTime

  private def getCurrentValues(): Map[String, Long] =
    MetricsSource.metricsMap
      .map { case(k, v) => k.stripPrefix(".") -> v.value }
      // all metrics are counters currently, but better to check it in case we want to add something later
      .filter(_._2.isInstanceOf[Counting])
      .mapValues(_.asInstanceOf[Counting].getCount)
      .toMap

  def formatNumber(n: Double): String = {
    // from https://stackoverflow.com/questions/45885151/bytes-in-human-readable-format-with-idiomatic-scala
    val tera = pow(10, 12)
    val giga = pow(10, 9)
    val mega = pow(10, 6)
    val kilo = pow(10, 3)

    val (value, unit) = {
      if (n >= tera) {
        (n / tera, "T")
      } else if (n >= giga) {
        (n / giga, "G")
      } else if (n >= mega) {
        (n / mega, "M")
      } else if (n >= kilo) {
        (n / kilo, "k")
      } else {
        (n, "")
      }
    }
    "%.1f%s".formatLocal(Locale.US, value, unit)
  }

  private def formatMessage(values: Seq[(String, Long)], time: Duration, isIncremental: Boolean): String =
    values
      .filter { case (_, v) => v != 0 }
      .sortBy(-_._2)
      .map {
        case (k, v) =>
          val perSecond = (v.toDouble / time.toMillis * 1000 * 60).toLong
          val plus = if (isIncremental) "+" else ""
          s"$k $plus${formatNumber(v)} (${formatNumber(perSecond)}/min)"
      }.mkString(",       ")

  def createMessage(): String = {
    val lastState = this.lastState
    val currentState = this.getCurrentValues()
    this.lastState = currentState

    val now = Instant.now
    val time = java.time.Duration.between(this.lastTime, now)
    this.lastTime = now

    formatMessage(
      currentState.toVector
        .map { case (k, v) => k -> (v - lastState.getOrElse(k, 0L)) },
      time,
      true
    )
  }

  def startLogger(rate: FiniteDuration): Unit = {
    import cognite.spark.v1.CdpConnector.ioRuntime
    val logger = getLogger
    fs2.Stream.repeatEval {
      IO(logger.info(createMessage()))
    }
      .metered(rate)
      .compile.drain.unsafeRunCancelable()
  }


  /** Returns a message of all metrics including average throughput over the entire runtime */
  def getFullMessage(): String = {
    val time = Duration.between(this.startTime, Instant.now)
    formatMessage(getCurrentValues().toVector, time, false)
  }
}
