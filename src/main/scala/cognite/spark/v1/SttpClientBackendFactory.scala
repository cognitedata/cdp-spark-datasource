package com.softwaremill.sttp.asynchttpclient

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.softwaremill.sttp.SttpBackendOptions
import io.netty.util.HashedWheelTimer
import org.asynchttpclient.AsyncHttpClient

object SttpClientBackendFactory {
  def create(): AsyncHttpClient = {
    lazy val clientThreadFactory =
      new ThreadFactoryBuilder().setNameFormat("AsyncHttpClient-%d").setDaemon(true).build()
    lazy val timerThreadFactory =
      new ThreadFactoryBuilder().setNameFormat("AsyncHttpClient-%d-timer").setDaemon(true).build()
    AsyncHttpClientBackend.clientWithModifiedOptions(
      SttpBackendOptions.Default,
      options =>
        options
          .setThreadFactory(clientThreadFactory)
          .setNettyTimer(new HashedWheelTimer(timerThreadFactory))
    )
  }
}
