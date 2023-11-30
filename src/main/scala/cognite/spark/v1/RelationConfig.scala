package cognite.spark.v1

import cats.effect.IO
import natchez.{EntryPoint, Kernel, Span}

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
    tracingEntryPoint: EntryPoint[IO],
    tracingParent: Kernel
) {

  /** Desired number of Spark partitions ~= partitions / parallelismPerPartition */
  def sparkPartitions: Int = Math.max(1, partitions / parallelismPerPartition)

  def traceS[B](name: String)(f: Span[IO] => IO[B]): IO[B] =
    tracingEntryPoint.continueOrElseRoot(name, tracingParent).use(f)

  def trace[B](name: String)(f: TracedIO[B]): IO[B] =
    traceS(name)(f.run)

  def tracePure[B](name: String)(f: Span[IO] => B): IO[B] =
    traceS(name)(span => IO.delay(f(span)))
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
