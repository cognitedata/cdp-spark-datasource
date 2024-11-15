package sttp.client3.asynchttpclient

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.util.HashedWheelTimer
import org.asynchttpclient.AsyncHttpClient
import sttp.client3.SttpBackendOptions

object SttpClientBackendFactory {
  def create(prefix: String = "Cdf-Spark", requestTimeout: Option[Int] = None): AsyncHttpClient = {
    // It's important that the threads made by the async http client is daemon threads,
    // so that we don't hang applications using our library during exit.
    // See for more info https://github.com/cognitedata/cdp-spark-datasource/pull/415/files#r396774391
    lazy val clientThreadFactory =
      new ThreadFactoryBuilder()
        .setNameFormat(s"$prefix-AsyncHttpClient-%d")
        .setDaemon(true)
        .build()
    lazy val timerThreadFactory =
      new ThreadFactoryBuilder()
        .setNameFormat(s"$prefix-AsyncHttpClient-%d-timer")
        .setDaemon(true)
        .build()
    AsyncHttpClientBackend.clientWithModifiedOptions(
      SttpBackendOptions.Default,
      options => {
        options
          .setThreadFactory(clientThreadFactory)
          .setNettyTimer(new HashedWheelTimer(timerThreadFactory))
        //Timeout override for potentially long stream operation
        requestTimeout.foreach(options.setRequestTimeout)
        options
      }
    )
  }
}