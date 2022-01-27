package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import CdpConnector._

abstract class SdkV1Relation[A <: Product, I](config: RelationConfig, shortName: String)
    extends CdfRelation(config, shortName)
    with Serializable
    with TableScan
    with PrunedFilteredScan {
  def schema: StructType

  def toRow(a: A): Row

  def uniqueId(a: A): I

  def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] =
    sys.error(s"Resource type $shortName does not support writing.")

  def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, A]]

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[A, I](
      sqlContext.sparkContext,
      config,
      (a: A, None) => {
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
      val batches =
        rows.grouped(config.batchSize.getOrElse(cognite.spark.v1.Constants.DefaultBatchSize)).toVector
      batches
        .parTraverse_(getFromRowsAndCreate(_))
        .unsafeRunSync()
    })

  def toRow(item: A, requiredColumns: Array[String]): Row =
    if (requiredColumns.isEmpty) {
      toRow(item)
    } else {
      val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
      val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf(f))
      val rowOfAllFields = toRow(item)
      Row.fromSeq(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
    }

  // scalastyle:off no.whitespace.after.left.bracket
  def updateByIdOrExternalId[
      P <: WithExternalIdGeneric[OptionalField] with WithId[Option[Long]],
      U <: WithSetExternalId,
      T <: UpdateById[R, U, IO] with UpdateByExternalId[R, U, IO],
      R <: ToUpdate[U] with WithId[Long]](
      updates: Seq[P],
      resource: T,
      isUpdateEmpty: U => Boolean
  )(implicit transform: Transformer[P, U]): IO[Unit] = {
    if (!updates.forall(u => u.id.isDefined || u.getExternalId.isDefined)) {
      throw new CdfSparkException("Update requires an id or externalId to be set for each row.")
    }

    val (rawUpdatesById, updatesByExternalId) = updates.partition(u => u.id.exists(_ > 0))
    val updatesById =
      rawUpdatesById
        .map(u => u.id.get -> u.transformInto[U])
        .filter {
          case (_, update) => !isUpdateEmpty(update)
        }
        .toMap
    val updateIds = if (updatesById.isEmpty) { IO.unit } else {
      resource
        .updateById(updatesById)
        .flatMap(_ => incMetrics(itemsUpdated, updatesById.size))
    }
    val updateExternalIds = if (updatesByExternalId.isEmpty) { IO.unit } else {
      val updatesByExternalIdMap = updatesByExternalId
        .map(u => u.getExternalId.get -> u.into[U].withFieldComputed(_.externalId, _ => None).transform)
        .toMap
      resource
        .updateByExternalId(updatesByExternalIdMap)
        .flatMap(_ => incMetrics(itemsUpdated, updatesByExternalIdMap.size))
    }

    (updateIds, updateExternalIds).parMapN((_, _) => ())
  }

  // scalastyle:off no.whitespace.after.left.bracket method.length
  def createOrUpdateByExternalId[
      R <: ToCreate[C],
      U <: WithSetExternalId,
      C <: WithGetExternalId,
      S <: WithExternalIdGeneric[ExternalIdF],
      ExternalIdF[_],
      T <: UpdateByExternalId[R, U, IO] with Create[R, C, IO]](
      existingExternalIds: Set[String],
      resourceCreates: Seq[S],
      resource: T,
      doUpsert: Boolean)(
      implicit transformToUpdate: Transformer[S, U],
      transformToCreate: Transformer[S, C]
  ): IO[Unit] = {
    val (resourcesToUpdate, resourcesToCreate) = resourceCreates.partition(
      p => p.getExternalId.exists(id => existingExternalIds.contains(id))
    )
    val create = if (resourcesToCreate.isEmpty) {
      IO.unit
    } else {
      resource
        .create(resourcesToCreate.map(_.transformInto[C]))
        .flatMap(_ => incMetrics(itemsCreated, resourcesToCreate.size))
        .recoverWith {
          case CdpApiException(_, 409, _, _, Some(duplicated), _, requestId) if doUpsert =>
            val moreExistingExternalIds = duplicated.flatMap(j => j("externalId")).map(_.asString.get)
            createOrUpdateByExternalId[R, U, C, S, ExternalIdF, T](
              existingExternalIds ++ moreExistingExternalIds.toSet,
              resourcesToCreate,
              resource,
              doUpsert = doUpsert)
        }
    }
    val update = if (resourcesToUpdate.isEmpty) {
      IO.unit
    } else {
      resource
        .updateByExternalId(
          resourcesToUpdate
            .map(u =>
              u.getExternalId.get -> u.into[U].withFieldComputed(_.externalId, _ => None).transform)
            .toMap)
        .flatTap(_ => incMetrics(itemsUpdated, resourcesToUpdate.size))
        .map(_ => ())
    }
    (create, update).parMapN((_, _) => ())
  }

  def deleteWithIgnoreUnknownIds(
      resource: DeleteByCogniteIds[IO],
      ids: Seq[CogniteId],
      ignoreUnknownIds: Boolean = true): IO[Unit] =
    if (ids.nonEmpty) {
      resource
        .deleteWithIgnoreUnknownIds(ids, ignoreUnknownIds)
        .flatTap(_ => incMetrics(itemsDeleted, ids.length))
    } else {
      IO.pure(Unit)
    }

  def genericUpsert[
      // The Item (read) type
      R <: ToUpdate[Up] with ToCreate[C] with WithId[Long],
      // The UpsertSchema type
      U <: WithExternalIdGeneric[OptionalField] with WithId[Option[Long]],
      // The ItemCreate type
      C <: WithExternalId,
      // The ItemUpdate type
      Up <: WithSetExternalId,
      // The resource type (client.<resource>)
      Re <: UpdateById[R, Up, IO] with UpdateByExternalId[R, Up, IO] with Create[R, C, IO]](
      items: Seq[U],
      isUpdateEmpty: Up => Boolean,
      resource: Re,
      mustBeUpdate: U => Boolean = (_: U) => false)(
      implicit transformToUpdate: Transformer[U, Up],
      transformToCreate: Transformer[U, C]): IO[Unit] = {

    val (itemsWithId, itemsWithoutId) = items.partition(r => r.id.exists(_ > 0) || mustBeUpdate(r))

    // In each create batch we must not have duplicated external IDs.
    // Duplicated ids (not external) in eventsToUpdate are ok, however, because
    // we create a map from id -> update, and that map will contain only one
    // update per id.
    val itemsToCreateWithoutDuplicatesByExternalId = itemsWithoutId
      .groupBy(_.getExternalId)
      .flatMap {
        case (None, items) => items
        case (Some(_), items) => items.take(1)
      }
      .toSeq
    val update = updateByIdOrExternalId[U, Up, Re, R](
      itemsWithId,
      resource,
      isUpdateEmpty
    )
    val createOrUpdate = createOrUpdateByExternalId[R, Up, C, U, OptionalField, Re](
      Set.empty,
      itemsToCreateWithoutDuplicatesByExternalId,
      resource,
      doUpsert = true)
    (update, createOrUpdate).parMapN((_, _) => ())
  }
}

trait WritableRelation {
  def insert(rows: Seq[Row]): IO[Unit]
  def upsert(rows: Seq[Row]): IO[Unit]
  def update(rows: Seq[Row]): IO[Unit]
  def delete(rows: Seq[Row]): IO[Unit]
}
