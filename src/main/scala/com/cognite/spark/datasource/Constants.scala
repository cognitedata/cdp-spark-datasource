package com.cognite.spark.datasource

import scala.concurrent.duration._

object Constants {
  val DefaultMaxRetries = 10
  val DefaultBatchSize = 1000
  val DefaultRawBatchSize = 10000
  val DefaultDataPointsBatchSize = 100000
  val DefaultDataPointsAggregationBatchSize = 10000
  val DefaultPartitions = 20
  val DefaultDataPointsPartitions = 20
  val DefaultInitialRetryDelay: FiniteDuration = 150.millis
  val DefaultMaxBackoffDelay: FiniteDuration = 120.seconds
  val DefaultBaseUrl = "https://api.cognitedata.com"
  val MetadataValuePostMaxLength = 512
  val MaxConcurrentRequests = 1000
}
