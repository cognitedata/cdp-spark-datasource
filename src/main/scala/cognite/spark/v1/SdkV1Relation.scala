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
import io.circe.JsonObject
import org.log4s._

abstract class SdkV1Relation[A <: Product, I](config: RelationConfig, shortName: String)
    extends CdfRelation(config, shortName)
    with Serializable
    with TableScan
    with PrunedFilteredScan {

  val logger = getLogger
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
      batches
        .parTraverse_(getFromRowsAndCreate(_))
        .unsafeRunSync()
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
      R <: WithId[Long]](
      updates: Seq[P],
      resource: T,
      isUpdateEmpty: U => Boolean
  )(implicit transform: Transformer[P, U]): IO[Unit] = {
    if (!updates.forall(u => u.id.isDefined || u.externalId.isDefined)) {
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
        .map(u => u.externalId.get -> u.into[U].withFieldComputed(_.externalId, _ => None).transform)
        .toMap
      resource
        .updateByExternalId(updatesByExternalIdMap)
        .flatMap(_ => incMetrics(itemsUpdated, updatesByExternalIdMap.size))
    }

    (updateIds, updateExternalIds).parMapN((_, _) => ())
  }

  private def assertNoLegacyNameConflicts(
      duplicated: Seq[JsonObject],
      requestId: Option[String]): Unit = {
    val legacyNameConflicts =
      duplicated.flatMap(j => j("legacyName")).map(_.asString.get)

    if (legacyNameConflicts.nonEmpty) {
      throw new CdfSparkIllegalArgumentException(
        "Found legacyName conflicts, upserts only supported with legacyName when using externalId as legacy name." +
          s" Conflicting legacyNames: ${legacyNameConflicts.mkString(", ")}." +
          requestId.map(id => s" Request ID: $id").getOrElse(""))
    }
  }

  // scalastyle:off no.whitespace.after.left.bracket
  def createOrUpdateByExternalId[
      R <: WithExternalId,
      U <: WithSetExternalId,
      C <: WithExternalId,
      T <: UpdateByExternalId[R, U, IO] with Create[R, C, IO]](
      existingExternalIds: Set[String],
      resourceCreates: Seq[C],
      resource: T,
      doUpsert: Boolean)(implicit transform: Transformer[C, U]): IO[Unit] = {
    logger.info("I'm in createOrUpdateByExternalId now")
    val (resourcesToUpdate, resourcesToCreate) = resourceCreates.partition(
      p => p.externalId.exists(id => existingExternalIds.contains(id))
    )
    logger.info(s"resourceToCreate: ${resourcesToCreate.length}, resourcesToUpdate: ${resourcesToUpdate.length}")
    val create = if (resourcesToCreate.isEmpty) {
      IO.unit
    } else {
      resource
        .create(resourcesToCreate)
        .flatMap(_ => incMetrics(itemsCreated, resourcesToCreate.size))
        .recoverWith {
          case CdpApiException(_, 409, _, _, Some(duplicated), _, requestId) if doUpsert =>
            val moreExistingExternalIds = config.legacyNameSource match {
              case LegacyNameSource.ExternalId =>
                // If we attempt to insert a time series that conflicts on both legacyName and externalId,
                // the API will only return legacyName conflicts. Therefore, we also need to include the
                // conflicting legacyNames (which we know to be externalIds) in the next round of updates.
                duplicated.flatMap(j => j("externalId") ++ j("legacyName")).map(_.asString.get)
              case _ =>
                assertNoLegacyNameConflicts(duplicated, requestId)
                duplicated.flatMap(j => j("externalId")).map(_.asString.get)
            }
            logger.info(s"Duplicates found: ${moreExistingExternalIds.length}")
            createOrUpdateByExternalId[R, U, C, T](
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
            .map(u => u.externalId.get -> u.into[U].withFieldComputed(_.externalId, _ => None).transform)
            .toMap)
        .flatTap(_ => incMetrics(itemsUpdated, resourcesToUpdate.size))
        .map(_ => ())
    }
    (create, update).parMapN((_, _) => ())
  }

  def deleteWithIgnoreUnknownIds(
      resource: DeleteByIdsWithIgnoreUnknownIds[IO, Long],
      deletes: Seq[DeleteItem],
      ignoreUnknownIds: Boolean = true): IO[Unit] = {
    val ids = deletes.map(_.id)
    resource
      .deleteByIds(ids, ignoreUnknownIds)
      .flatTap(_ => incMetrics(itemsDeleted, ids.length))
  }

  def genericUpsert[
      // The Item (read) type
      R <: WithExternalId with WithId[Long],
      // The UpsertSchema type
      U <: WithExternalId with WithId[Option[Long]],
      // The ItemCreate type
      C <: WithExternalId,
      // The ItemUpdate type
      Up <: WithSetExternalId,
      // The resource type (client.<resource>)
      Re <: UpdateById[R, Up, IO] with UpdateByExternalId[R, Up, IO] with Create[R, C, IO]](
      itemsToUpdate: Seq[U],
      itemsToCreate: Seq[C],
      isUpdateEmpty: Up => Boolean,
      resource: Re)(
      implicit transformUpsertToUpdate: Transformer[U, Up],
      transformCreateToUpdate: Transformer[C, Up]): IO[Unit] = {

    // In each create batch we must not have duplicated external IDs.
    // Duplicated ids (not external) in eventsToUpdate are ok, however, because
    // we create a map from id -> update, and that map will contain only one
    // update per id.
    val itemsToCreateWithoutDuplicatesByExternalId = itemsToCreate
      .groupBy(_.externalId)
      .flatMap {
        case (None, items) => items
        case (Some(_), items) => items.take(1)
      }
      .toSeq
    logger.info(s"itemsToCreateWithoutDuplicatesByExternalId: ${itemsToCreateWithoutDuplicatesByExternalId.length}")
    val update = updateByIdOrExternalId[U, Up, Re, R](
      itemsToUpdate,
      resource,
      isUpdateEmpty
    )
    val createOrUpdate = createOrUpdateByExternalId[R, Up, C, Re](
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
