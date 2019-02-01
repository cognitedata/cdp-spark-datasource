package com.cognite.spark.datasource

import scala.concurrent.duration._

object Constants {
  val DefaultBatchSize = 1000
  val DefaultMaxRetries = 10
  val DefaultDataPointsBatchSize = 100000
  val DefaultDataPointsAggregationBatchSize = 10000
  val DefaultInitialRetryDelay = 30.millis
}
