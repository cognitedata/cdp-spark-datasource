package cognite.spark.v1

import cats.effect.IO
import cats.effect.syntax.all._
import cats.implicits._
import cats.syntax._
import cognite.spark.v1.PushdownUtilities.stringSeqToCogniteExternalIdSeq
import cognite.spark.v1.SparkSchemaHelper.{fromRow, structType}
import com.cognite.sdk.scala.common.{CdpApiException, SetValue}
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, AssetUpdate, AssetsFilter, CogniteExternalId, CogniteId, CogniteInternalId, LabelsOnUpdate}
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json, JsonObject}
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.annotation.tailrec
import scala.collection.mutable

final case class AssetsIngestSchema(
    externalId: String,
    parentExternalId: String,
    source: OptionalField[String],
    name: String,
    description: OptionalField[String],
    metadata: Option[Map[String, String]],
    dataSetId: OptionalField[Long],
    labels: Option[Seq[String]]) {
  def optionalParentId: Option[String] = Some(parentExternalId).filter(_.nonEmpty)
}

final case class AssetSubtree(
    // node that contains all the `nodes`
    // might be just a pseudo-root
    root: AssetsIngestSchema,
    nodes: Vector[AssetsIngestSchema]
) {

  /** Nodes including the root node */
  def allNodes: Vector[AssetsIngestSchema] = root +: nodes
}

final case class NoRootException(
    message: String = """Tree has no root. Set parentExternalId to "" for the root Asset.""")
    extends CdfSparkException(message)
final case class InvalidTreeException(message: String = s"The tree has an invalid structure.")
    extends CdfSparkException(message)
final case class NonUniqueAssetId(id: String)
    extends CdfSparkException(s"Asset tree contains asset '$id' multiple times.")
final case class EmptyExternalIdException(message: String = s"ExternalId cannot be an empty String.")
    extends CdfSparkException(message)
final case class CdfDoesNotSupportException(message: String) extends CdfSparkException(message)
final case class InvalidNodeReferenceException(nodeIds: Seq[String], referencedFrom: Seq[String])
    extends CdfSparkException({
      val plural = (sg: String, pl: String) => if (nodeIds.length == 1) sg else pl
      val nodes = nodeIds.map(x => s"'$x'").sorted.mkString(", ")
      val refNodes = referencedFrom.map(x => s"'$x'").sorted.mkString(", ")
      s"${plural("Parent", "Parents")} $nodes referenced from $refNodes ${plural("does", "do")} not exist."
    })
final case class InvalidRootChangeException(
    cause: CdpApiException,
    assetIds: Seq[CogniteId],
    rootId: String)
    extends CdfSparkException(
      {
        val commaSeparatedAssets =
          assetIds
            .map {
              case CogniteExternalId(externalId) => s"(externalId=$externalId)"
              case CogniteInternalId(id) => s"(id=$id)"
            }
            .mkString(", ")

        s"Attempted to move some assets to a different root asset in $rootId. " +
          "If this is intended, the assets must be manually deleted and re-created under the new root asset. " +
          s"The following assets were attempted to be moved: $commaSeparatedAssets."
      },
      cause
    )

object InvalidRootChangeException {
  def createFromTreeList(
      cause: CdpApiException,
      ids: Seq[String],
      trees: Seq[AssetSubtree],
      createdRoots: Seq[Asset]): InvalidRootChangeException = {
    // get the first problematic subtree
    val firstProblematicSubtree = ids.flatMap { id =>
      trees.find(t => t.allNodes.exists(_.externalId == id))
    }.headOption
    val rootId = firstProblematicSubtree
      .map { t =>
        val subtreeId = t.root.externalId
        val rootId = createdRoots
          .find(_.externalId.contains(subtreeId))
          .flatMap(_.rootId)
          .map(_.toString)
          .getOrElse("unknown rootId")
        s"subtree $subtreeId under the rootId=$rootId"
      }
      .getOrElse("unknown subtree, API referenced node which we did not create")

    // filter out only the assets from the first subtree so we don't spam users with irrelevant ids
    val assetsMoved =
      firstProblematicSubtree.toVector
        .flatMap(_.allNodes)
        .map(_.externalId)
        .intersect(ids)
        .map(CogniteExternalId)

    InvalidRootChangeException(cause, assetsMoved, rootId)

  }
}

class AssetHierarchyBuilder(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdfRelation(config, "assethierarchy") {

  import CdpConnector.cdpConnectorContextShift

  def buildFromDf(data: DataFrame): Unit =
    data
      .repartition(numPartitions = 1)
      .foreachPartition((rows: Iterator[Row]) => {
        build(rows).unsafeRunSync()
      })

  private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

  private val deleteMissingAssets = config.deleteMissingAssets
  private val subtreeMode = config.subtrees

  def build(data: Iterator[Row]): IO[Unit] = {
    val sourceTree = data.map(r => fromRow[AssetsIngestSchema](r)).toArray

    val subtrees =
      validateAndOrderInput(sourceTree)
        .sortBy(_.root.externalId) // make potential errors deterministic

    val filteredSubtrees = subtreeMode match {
      case AssetSubtreeOption.Ingest =>
        subtrees
      case AssetSubtreeOption.Ignore =>
        // only take trees with proper roots
        subtrees.filter(t => t.root.parentExternalId.isEmpty)
      case AssetSubtreeOption.Error =>
        val nonRoots = subtrees.filter(t => t.root.parentExternalId.nonEmpty)
        if (nonRoots.nonEmpty) {
          throw InvalidTreeException(
            s"These subtrees are not connected to any root: ${nonRoots.map(_.root.externalId).mkString(", ")}."
              + " Did you mean to set the option ignoreDisconnectedAssets or allowSubtreeIngestion to true?"
          )
        }
        subtrees
    }

    if (subtrees.nonEmpty && filteredSubtrees.isEmpty) {
      // in case all nodes are dropped due to ignoreDisconnectedAssets
      throw NoRootException()
      // we can allow empty source data set, that won't really happen by accident
    }

    for {
      _ <- validateSubtreeRoots(filteredSubtrees.map(_.root))
      _ <- buildSubtrees(filteredSubtrees)
    } yield ()
  }

  def validateSubtreeRoots(roots: Seq[AssetsIngestSchema]): IO[Unit] = {
    // check that all `parentExternalId`s exist
    val ids = roots.map(_.parentExternalId).filter(_.nonEmpty)
    if (ids.isEmpty) {
      IO.unit
    } else {
      // The API calls throw exception when any of the ids do not exist
      ids.distinct
        .grouped(batchSize)
        .toVector
        .parTraverse { idsInGroup =>
          client.assets
            .retrieveByExternalIds(idsInGroup)
            .adaptError({
              case e: CdpApiException if e.code == 400 && e.missing.nonEmpty =>
                val missingNodes = e.missing.get.map(j => j("externalId").get.asString.get).take(10)
                val referencingNodes =
                  missingNodes
                  // always take the one with "lowest" externalId, so the errors are deterministic
                    .map(missing => roots.filter(_.parentExternalId == missing).minBy(_.externalId))
                    .map(_.externalId)
                InvalidNodeReferenceException(missingNodes, referencingNodes)
            })
        }
        .void
    }
  }

  def buildSubtrees(trees: Vector[AssetSubtree]): IO[Unit] =
    for {
      // fetch existing roots and update or insert them first
      cdfRoots <- fetchCdfRoots(trees.map(_.root.externalId))
      upsertedRoots <- upsertRoots(trees.map(_.root), cdfRoots)

      // then fetch all the subtrees to know if we should insert, update or delete them
      cdfTrees <- fetchCdfSubtrees(upsertedRoots)

      (toDelete, toInsert, toUpdate) = trees
        .map(t => nodesToDeleteInsertUpdate(t, cdfTrees(t.root.externalId)))
        .unzip3

      _ <- (
        delete(toDelete.flatten, deleteMissingAssets),
        insert(toInsert.flatten, trees, upsertedRoots),
        update(toUpdate.flatten)
      ).parMapN((_, _, _) => ())
    } yield ()

  def insert(
      toInsert: Seq[AssetsIngestSchema],
      trees: Vector[AssetSubtree],
      createdRoots: Vector[Asset]): IO[Unit] = {
    val assetCreatesToInsert = toInsert.map(toAssetCreate)
    // Traverse batches in order to ensure writing parents first
    assetCreatesToInsert
      .grouped(batchSize)
      .toList
      .traverse(
        client.assets
          .create(_)
          .flatMap(x => incMetrics(itemsCreated, x.size))
      )
      .void
      .adaptErr {
        case ex: CdpApiException if ex.duplicated.isDefined =>
          val ids =
            ex.duplicated.get
              .flatMap(decodeCogniteId)
              .collect { case CogniteExternalId(id) => id }
          // If we've hit this case, the assets already exist in CDF, but under a different root asset.
          // Since we didn't find them under the subtree, we assumed they did not exist, and attempted to to insert them,
          // resulting in this error.
          if (ids.nonEmpty) {
            InvalidRootChangeException.createFromTreeList(ex, ids, trees, createdRoots)
          } else {
            // ids not found, propagate the original error
            ex
          }
      }
  }

  private def decodeCogniteId(jsonObject: JsonObject): Option[CogniteId] = {
    val externalIdDecoder: Decoder[CogniteExternalId] = deriveDecoder
    val internalIdDecoder: Decoder[CogniteInternalId] = deriveDecoder
    val json = Json.fromJsonObject(jsonObject)
    internalIdDecoder.decodeJson(json) match {
      case Right(id: CogniteId) => Some(id)
      case _ =>
        externalIdDecoder.decodeJson(json) match {
          case Right(id) => Some(id)
          case _ => None
        }
    }
  }

  def delete(toDelete: Vector[Asset], deleteMissingAssets: Boolean): IO[Unit] =
    if (deleteMissingAssets) {
      batchedOperation[Long, Nothing](
        toDelete.map(_.id),
        idBatch =>
          client.assets
            .deleteByIds(idBatch, recursive = true, ignoreUnknownIds = true)
            .flatTap(_ => incMetrics(itemsDeleted, idBatch.length))
            .as(Vector())
      ).void
    } else {
      IO.unit
    }

  def update(toUpdate: Vector[AssetsIngestSchema]): IO[Unit] =
    batchedOperation[AssetsIngestSchema, Asset](
      toUpdate,
      updateBatch => {
        client.assets
          .updateByExternalId(updateBatch.map(a => a.externalId -> toAssetUpdate(a)).toMap)
          .flatTap(u => incMetrics(itemsUpdated, u.size))
      }
    ).void

  def fetchCdfRoots(sourceRootExternalIds: Vector[String]): IO[Map[String, Asset]] =
    batchedOperation[String, Asset](
      sourceRootExternalIds,
      batch =>
        client.assets
          .retrieveByExternalIds(batch, ignoreUnknownIds = true)
    ).map(_.map(a => a.externalId.get -> a).toMap)

  def fetchCdfSubtrees(roots: Vector[Asset]): IO[Map[String, Vector[Asset]]] = {

    def collectIntoTrees(roots: Vector[Asset], assets: Vector[Asset]): Vector[(String, Vector[Asset])] = {
      // note that we have to work with internalIds here since we might need to delete some assets which don't have externalId
      val lookupChildren = assets.groupBy(_.parentId)

      // generation by "generation" get all descendants of the specified group of assets
      // buf is the tail recursive result value. List of Vectors because appending to list is cheap
      @tailrec
      def getAllDescendants(roots: Vector[Long], buf: List[Vector[Asset]] = List.empty): Vector[Asset] = {
        val children = roots.flatMap(r => lookupChildren.getOrElse(Some(r), Vector.empty))
        if (children.isEmpty) {
          buf.toVector.flatten
        } else {
          getAllDescendants(children.map(_.id), children :: buf)
        }
      }

      roots.map(a => a.externalId.get -> getAllDescendants(Vector(a.id)))
    }

    batchedOperation[Asset, (String, Vector[Asset])](
      roots,
      roots =>
        // The API will error out if the subtree contains more than 100,000 items
        // If that's a problem, we'll have to do a workaround by downloading its entire hierarchy,
        // but the rootIds filter is deprecated, so we'll have to convince the API team to lift that limitation :/
        client.assets
          .filter(AssetsFilter(assetSubtreeIds = Some(roots.map(r => CogniteInternalId(r.id)))))
          .compile
          .toVector
          .map(collectIntoTrees(roots, _)),
      batchSize = 100 // this is the API limit for filter
    ).map(_.toMap)
  }

  def upsertRoots( // scalastyle:off
      newRoots: Vector[AssetsIngestSchema],
      sourceRoots: Map[String, Asset]): IO[Vector[Asset]] = {

    // Assets without corresponding source root will be created
    val (toUpdateOrIgnore, toCreate) = newRoots.partition(sourceRoots contains _.externalId)
    val createdIO =
      batchedOperation(
        toCreate.map(toAssetCreate),
        (batch: Vector[AssetCreate]) =>
          client.assets
            .create(batch)
            .flatTap(x => incMetrics(itemsCreated, x.size))
      )

    // Ignore assets which have a corresponding sourceRoot, but it has the same data
    val (toIgnore, toUpdate) =
      toUpdateOrIgnore.partition(a => isMostlyEqual(a, sourceRoots(a.externalId)))
    // Update the rest...
    val updatedIO =
      batchedOperation[(String, AssetUpdate), Asset](
        toUpdate.map { newRoot =>
          val oldRoot = sourceRoots(newRoot.externalId)
          val parentIdUpdate = (newRoot.optionalParentId, oldRoot.parentId) match {
            case (None, Some(oldParent)) =>
              throw CdfDoesNotSupportException(
                s"Can not make root from asset '${newRoot.externalId}' which is already inside asset ${oldParent}"
                  + s" (${oldRoot.parentExternalId.getOrElse("without externalId")})."
                  + " You might need to remove the asset and insert it again.")
            case (None, None) => None
            case (Some(newParent), _) if oldRoot.parentExternalId.contains(newParent) => None
            case (Some(newParent), _) => Some(SetValue(newParent))
          }
          newRoot.externalId -> toAssetUpdate(newRoot).copy(parentExternalId = parentIdUpdate)
        },
        batch =>
          client.assets
            .updateByExternalId(batch.toMap)
            .flatTap(x => incMetrics(itemsUpdated, x.size))
      )

    val ignored = toIgnore.map(a => sourceRoots(a.externalId))

    (createdIO, updatedIO).parMapN { (created, updated) =>
      created ++ updated ++ ignored
    }
  }

  def nodesToDeleteInsertUpdate(subtree: AssetSubtree, cdfTree: Vector[Asset])
    : (Vector[Asset], Vector[AssetsIngestSchema], Vector[AssetsIngestSchema]) = {
    val newIds = subtree.nodes.map(_.externalId).toSet
    // Keep assets that are in both source and CDF, delete those only in CDF
    val toDelete =
      cdfTree.filter(a => !a.externalId.exists(newIds.contains))

    val cdfTreeMap = cdfTree.flatMap(a => a.externalId.map((_, a))).toMap
    // Insert assets that are not present in CDF
    val toInsert = subtree.nodes.filter(a => !cdfTreeMap.contains(a.externalId))

    // Filter out assets that would not be changed in an update
    val toUpdate = subtree.nodes.filter(a => needsUpdate(a, cdfTreeMap))

    (toDelete, toInsert, toUpdate)
  }

  def needsUpdate(child: AssetsIngestSchema, cdfTreeByExternalId: Map[String, Asset]): Boolean = {
    // Find the matching asset in CDF
    val cdfAsset = cdfTreeByExternalId.get(child.externalId)
    cdfAsset match {
      case Some(asset) =>
        !isMostlyEqual(child, asset)
      case None => false // this one will be inserted
    }
  }

  def validateSourceTree(source: Array[AssetsIngestSchema]): Unit = {
    val emptyExternalIds = source.filter(_.externalId == "")
    if (emptyExternalIds.nonEmpty) {
      throw EmptyExternalIdException(
        s"""Found empty externalId for children with names: ${emptyExternalIds
          .map(_.name)
          .mkString(", ")}""")
    }
  }

  def isMostlyEqual(updatedAsset: AssetsIngestSchema, asset: Asset): Boolean =
    updatedAsset.description.toOption == asset.description &&
      updatedAsset.metadata.getOrElse(Map()) == asset.metadata.getOrElse(Map()) &&
      updatedAsset.name == asset.name &&
      updatedAsset.source.toOption == asset.source &&
      updatedAsset.dataSetId.toOption == asset.dataSetId &&
      updatedAsset.labels.getOrElse(Seq()).map(CogniteExternalId) == asset.labels.getOrElse(Seq()) &&
      (updatedAsset.parentExternalId == "" && asset.parentId.isEmpty || asset.parentExternalId.contains(
        updatedAsset.parentExternalId))

  def buildAssetMap(source: Array[AssetsIngestSchema]): mutable.HashMap[String, AssetsIngestSchema] = {
    val map = mutable.HashMap[String, AssetsIngestSchema]()
    for (x <- source) {
      if (map.contains(x.externalId)) {
        throw NonUniqueAssetId(x.externalId)
      }
      map(x.externalId) = x
    }
    map
  }

  private def validateAndOrderInput(tree: Array[AssetsIngestSchema]): Vector[AssetSubtree] = {
    validateSourceTree(tree)
    val assetMap = buildAssetMap(tree)
    def getRoot(tree: AssetsIngestSchema, path: Set[String] = Set()): AssetsIngestSchema =
      if (path contains tree.externalId) {
        throw InvalidTreeException(s"The asset tree contains a cycle: ${path.mkString(", ")}")
      } else {
        assetMap
          .get(tree.parentExternalId)
          .map(getRoot(_, path + tree.externalId))
          .getOrElse(tree)
      }

    tree
      .groupBy(getRoot(_))
      .map {
        case (root, items) =>
          assert(items.contains(root))
          AssetSubtree(root, orderChildren(root.externalId, items.filter(_ != root)))
      }
      .toVector
  }

  def iterateChildren(
      currentChildren: Array[AssetsIngestSchema],
      parents: Array[String],
      visited: Seq[AssetsIngestSchema]): Seq[AssetsIngestSchema] = {
    val nextChildren = currentChildren.filter(a => parents.contains(a.parentExternalId))
    if (nextChildren.isEmpty) {
      visited
    } else {
      iterateChildren(currentChildren, nextChildren.map(_.externalId), visited ++ nextChildren)
    }
  }

  def orderChildren(rootId: String, nodes: Array[AssetsIngestSchema]): Vector[AssetsIngestSchema] =
    iterateChildren(nodes, Array(rootId), Seq()).toVector

  override def schema: StructType = structType[AssetsIngestSchema]

  def toAssetCreate(a: AssetsIngestSchema): AssetCreate =
    a.into[AssetCreate]
      .withFieldComputed(_.parentExternalId, _.optionalParentId)
      .withFieldComputed(_.labels, a => stringSeqToCogniteExternalIdSeq(a.labels))
      .transform

  def toAssetUpdate(a: AssetsIngestSchema): AssetUpdate =
    a.into[AssetUpdate]
      .withFieldComputed(_.labels, _.labels match {
        case labelList: Some[Seq[String]] =>
          Some(LabelsOnUpdate(add = stringSeqToCogniteExternalIdSeq(labelList)))
        case _ => None
      })
      .transform

  private def batchedOperation[I, R](list: Vector[I], op: Vector[I] => IO[Seq[R]], batchSize: Int = this.batchSize): IO[Vector[R]] =
    if (list.nonEmpty) {
      list
        .grouped(batchSize)
        .toVector
        .parFlatTraverse(op(_).map(_.toVector))
    } else {
      IO.pure(Vector.empty)
    }
}

object AssetHierarchyBuilder {
  val upsertSchema: StructType = structType[AssetsIngestSchema]
}
