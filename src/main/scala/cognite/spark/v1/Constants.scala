package cognite.spark.v1

import BuildInfo.BuildInfo

import scala.concurrent.duration._

object Constants {
  val CreateDataPointsLimit = 100000
  val DefaultMaxRetries = 10
  val DefaultMaxRetryDelaySeconds = 30
  val DefaultBatchSize = 1000
  val DefaultRawBatchSize = 10000
  val DefaultSequenceRowsBatchSize = 10000
  val DefaultInferSchemaLimit = 10000
  val DefaultDataPointsLimit = 100000
  val DefaultSequencesLimit = 10000
  val DefaultPartitions = 200
  val DefaultDataPointsPartitions = 20
  val DefaultParallelismPerPartition = 10
  val DefaultInitialRetryDelay: FiniteDuration = 150.millis
  val DefaultMaxBackoffDelay: FiniteDuration = 120.seconds
  val DefaultBaseUrl = "https://api.cognitedata.com"
  val SparkDatasourceVersion = s"${BuildInfo.organization}-${BuildInfo.version}"
  val millisSinceEpochIn2100 = 4102448400000L
}
