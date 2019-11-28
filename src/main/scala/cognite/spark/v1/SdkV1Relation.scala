package cognite.spark.v1

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.codahale.metrics.Counter
import com.cognite.sdk.scala.v1.{GenericClient, TimeSeriesUpdate}
import com.cognite.sdk.scala.common._
import com.softwaremill.sttp.SttpBackend
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import fs2.Stream
import io.scalaland.chimney.Transformer
import org.apache.spark.datasource.MetricsSource
import io.scalaland.chimney.dsl._
import CdpConnector._

abstract class SdkV1Relation[A <: Product, I](config: RelationConfig, shortName: String)
    extends BaseRelation
    with CdpConnector
    with Serializable
    with TableScan
    with PrunedFilteredScan {
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")
  @transient lazy private val itemsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.created")

  @transient lazy implicit val retryingSttpBackend: SttpBackend[IO, Nothing] =
    CdpConnector.retryingSttpBackend(config.maxRetries)
  implicit val auth: Auth = config.auth
  @transient lazy val client =
    new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion, config.baseUrl)

  def schema: StructType

  def toRow(a: A): Row

  def uniqueId(a: A): I

  def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] =
    sys.error(s"Resource type $shortName does not support writing.")

  def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, A]]

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[A, I](
      sqlContext.sparkContext,
      config,
      (a: A) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(a, requiredColumns)
      },
      uniqueId,
      getStreams(filters)
    )

  def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      import CdpConnector._
      val batches = rows.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup
          .parTraverse(getFromRowAndCreate)
          .flatTap { _ =>
            IO {
              if (config.collectMetrics) {
                itemsCreated.inc(batchGroup.foldLeft(0: Int)(_ + _.length))
              }
            }
          }
          .unsafeRunSync()
      }
      ()
    })

  def toRow(item: A, requiredColumns: Array[String]): Row =
    if (requiredColumns.isEmpty) {
      toRow(item)
    } else {
      val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
      val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf[String](f))
      val rowOfAllFields = toRow(item)
      Row.fromSeq(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
    }

  // scalastyle:off no.whitespace.after.left.bracket
  def updateByIdOrExternalId[
      P <: WithExternalId with WithId[Option[Long]],
      U <: WithSetExternalId,
      T <: UpdateById[R, U, IO] with UpdateByExternalId[R, U, IO],
      R <: WithId[Long]](updates: Seq[P], resource: T)(
      implicit transform: Transformer[P, U]): IO[Unit] = {
    require(
      updates.forall(u => u.id.isDefined || u.externalId.isDefined),
      "Update requires an id or externalId to be set for each row.")
    val (updatesById, updatesByExternalId) = updates.partition(_.id.isDefined)
    val updateIds = if (updatesById.isEmpty) { IO.unit } else {
      resource.updateById(updatesById.map(u => u.id.get -> u.transformInto[U]).toMap)
    }
    val updateExternalIds = if (updatesByExternalId.isEmpty) { IO.unit } else {
      resource.updateByExternalId(
        updatesByExternalId
          .map(u => u.externalId.get -> u.into[U].withFieldComputed(_.externalId, _ => None).transform)
          .toMap)
    }
    (updateIds, updateExternalIds).parMapN((_, _) => ())
  }

  // scalastyle:off no.whitespace.after.left.bracket
  def upsertAfterConflict[
      R <: WithId[Long],
      U <: WithSetExternalId,
      C <: WithExternalId,
      T <: UpdateByExternalId[R, U, IO] with Create[R, C, IO]](
      existingExternalIds: Seq[String],
      resourceCreates: Seq[C],
      resource: T)(implicit transform: Transformer[C, U]): IO[Unit] = {
    val (resourcesToUpdate, resourcesToCreate) = resourceCreates.partition(
      p => if (p.externalId.isEmpty) { false } else { existingExternalIds.contains(p.externalId.get) }
    )
    val create = if (resourcesToCreate.isEmpty) { IO.unit } else {
      resource.create(resourcesToCreate)
    }
    val update = if (resourcesToUpdate.isEmpty) { IO.unit } else {
      resource.updateByExternalId(
        resourcesToUpdate
          .map(u => u.externalId.get -> u.into[U].withFieldComputed(_.externalId, _ => None).transform)
          .toMap)
    }
    (create, update).parMapN((_, _) => ())
  }

  def deleteWithIgnoreUnknownIds(
      resource: DeleteByIdsWithIgnoreUnknownIds[IO, Long],
      deletes: Seq[DeleteItem],
      ignoreUnknownIds: Boolean = true): IO[Unit] = {
    val ids = deletes.map(_.id)
    resource.deleteByIds(ids, ignoreUnknownIds)
  }
}

trait WritableRelation {
  def insert(rows: Seq[Row]): IO[Unit]
  def upsert(rows: Seq[Row]): IO[Unit]
  def update(rows: Seq[Row]): IO[Unit]
  def delete(rows: Seq[Row]): IO[Unit]
}
