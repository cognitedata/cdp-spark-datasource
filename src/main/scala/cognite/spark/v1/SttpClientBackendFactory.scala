package com.softwaremill.sttp.asynchttpclient

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.softwaremill.sttp.SttpBackendOptions
import io.netty.util.HashedWheelTimer
import org.asynchttpclient.AsyncHttpClient

object SttpClientBackendFactory {
  def create(): AsyncHttpClient = {
    // It's important that the threads made by the async http client is daemon threads,
    // so that we don't hang applications using our library during exit.
    // See for more info https://github.com/cognitedata/cdp-spark-datasource/pull/415/files#r396774391
    lazy val clientThreadFactory =
      new ThreadFactoryBuilder()
        .setNameFormat("AsyncHttpClient-%d")
        .setDaemon(true)
        .build()
    lazy val timerThreadFactory =
      new ThreadFactoryBuilder()
        .setNameFormat("AsyncHttpClient-%d-timer")
        .setDaemon(true)
        .build()
    AsyncHttpClientBackend.clientWithModifiedOptions(
      SttpBackendOptions.Default,
      options =>
        options
          .setThreadFactory(clientThreadFactory)
          .setNettyTimer(new HashedWheelTimer(timerThreadFactory))
    )
  }
}
