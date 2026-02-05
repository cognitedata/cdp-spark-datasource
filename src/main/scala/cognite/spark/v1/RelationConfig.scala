package cognite.spark.v1

import natchez.Kernel

import java.time.Instant

final case class TracingConfig(
    tracingParent: Kernel,
    maxRequests: Option[Long],
    maxTime: Option[Instant],
)

final case class RelationConfig(
    auth: CdfSparkAuth,
    clientTag: Option[String],
    applicationName: Option[String],
    projectName: String,
    batchSize: Option[Int],
    limitPerPartition: Option[Int],
    partitions: Int, // number of CDF partitions
    maxRetries: Int,
    maxRetryDelaySeconds: Int,
    collectMetrics: Boolean,
    collectTestMetrics: Boolean,
    metricsPrefix: String,
    metricsTrackAttempts: Boolean,
    baseUrl: String,
    onConflict: OnConflictOption,
    applicationId: String,
    parallelismPerPartition: Int, // max parallelism of CDF operations (per Spark partition)
    ignoreUnknownIds: Boolean,
    deleteMissingAssets: Boolean,
    subtrees: AssetSubtreeOption,
    ignoreNullFields: Boolean,
    rawEnsureParent: Boolean,
    enableSinglePartitionDeleteAssetHierarchy: Boolean, // flag to test whether single partition helps avoid NPE in asset hierarchy builder
    tracingConfig: TracingConfig,
    initialRetryDelayMillis: Int,
    useSharedThrottle: Boolean,
    serverSideFilterNullValuesOnNonSchemaRawQueries: Boolean,
    maxOutstandingRawInsertRequests: Option[Int],
    sendDebugFlag: Boolean,
    useQuery: Boolean
) {

  /** Desired number of Spark partitions ~= partitions / parallelismPerPartition */
  def sparkPartitions: Int = Math.max(1, partitions / parallelismPerPartition)
}

sealed trait OnConflictOption extends Serializable
object OnConflictOption {
  object Abort extends OnConflictOption
  object Update extends OnConflictOption
  object Upsert extends OnConflictOption
  object Delete extends OnConflictOption
  val fromString: Map[String, OnConflictOption] = Map(
    "abort" -> Abort,
    "update" -> Update,
    "upsert" -> Upsert,
    "delete" -> Delete
  )

  def withNameOpt(s: String): Option[OnConflictOption] =
    fromString.get(s.toLowerCase)
}

sealed trait AssetSubtreeOption extends Serializable
object AssetSubtreeOption {
  object Ingest extends AssetSubtreeOption
  object Ignore extends AssetSubtreeOption
  object Error extends AssetSubtreeOption
  val fromString: Map[String, AssetSubtreeOption] = Map(
    "ingest" -> Ingest,
    "ignore" -> Ignore,
    "error" -> Error
  )
  def withNameOpt(s: String): Option[AssetSubtreeOption] =
    fromString.get(s.toLowerCase)
}
