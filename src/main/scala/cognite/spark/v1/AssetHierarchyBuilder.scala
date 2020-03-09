package cognite.spark.v1

import cats.effect.IO
import cats.effect.syntax._
import cats.implicits._
import cats.syntax._
import cognite.spark.v1.SparkSchemaHelper.{fromRow, structType}
import com.cognite.sdk.scala.common.{CdpApiException, SetNull, SetValue}
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, AssetUpdate, AssetsFilter, CogniteExternalId}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

final case class AssetsIngestSchema(
    externalId: String,
    parentExternalId: String,
    source: Option[String],
    name: String,
    description: Option[String],
    metadata: Option[Map[String, String]],
    dataSetId: Option[Long])

object AssetsIngestSchema {
  def toAssetCreate(a: AssetsIngestSchema): AssetCreate =
    AssetCreate(
      name = a.name,
      description = a.description,
      externalId = Some(a.externalId),
      metadata = a.metadata,
      source = a.source,
      parentExternalId = Some(a.parentExternalId),
      dataSetId = a.dataSetId
    )

  def toAssetUpdate(a: AssetsIngestSchema): AssetUpdate =
    AssetUpdate(
      name = Some(SetValue(a.name)),
      description = a.description.map(SetValue(_)),
      externalId = Some(SetValue(a.externalId)),
      metadata = a.metadata.map(SetValue(_)),
      source = a.source.map(SetValue(_)),
      parentExternalId = Some(SetValue(a.parentExternalId)),
      dataSetId = a.dataSetId.map(SetValue(_))
    )
}

private case class AssetSubtree(
    // node that contains all the `nodes`
    // might be just a pseudo-root
    root: AssetsIngestSchema,
    nodes: Array[AssetsIngestSchema]
)

final case class MultipleRootsException(roots: Seq[String])
    extends Exception(s"Tree has more than one root: ${roots.mkString(", ")}")
final case class NoRootException(
    message: String = """Tree has no root. Set parentExternalId to "" for the root Asset.""")
    extends Exception(message)
final case class InvalidTreeException(message: String = s"The tree is has an invalid structure.")
    extends Exception(message)
final case class NonUniqueAssetId(id: String)
    extends Exception(s"Asset tree contains asset '$id' multiple times.")
final case class EmptyExternalIdException(message: String = s"ExternalId cannot be an empty String.")
    extends Exception(message)
final case class CdfDoesNotSupportException(message: String) extends Exception(message)
final case class InvalidNodeReferenceException(nodeIds: Seq[String], referencedFrom: Seq[String])
    extends Exception({
      val plural = (sg: String, pl: String) => if (nodeIds.length == 1) sg else pl
      val nodes = nodeIds.map(x => s"'$x'").mkString(", ")
      val refNodes = referencedFrom.map(x => s"'$x'").mkString(", ")
      s"${plural("Node", "Nodes")} $nodes referenced from $refNodes ${plural("does", "do")} not exist."
    })

class AssetHierarchyBuilder(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdfRelation(config, "assethierarchy") {

  import CdpConnector.cdpConnectorContextShift

  val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

  val deleteMissingAssets = config.deleteMissingAssets
  val ignoreDisconnectedAssets = config.ignoreDisconnectedAssets
  val allowMultipleRoots = config.allowMultipleRoots
  val allowSubtreeIngestion = config.allowSubtreeIngestion

  def build(df: DataFrame): IO[Unit] = {
    val sourceTree = df.collect.map(r => fromRow[AssetsIngestSchema](r))

    val subtrees = validateAndOrderInput(sourceTree).toVector

    val subtrees2 =
      if (allowSubtreeIngestion) {
        subtrees
      } else if (ignoreDisconnectedAssets) {
        // only take trees with proper roots
        subtrees.filter(t => t.root.parentExternalId.isEmpty)
      } else {
        val nonRoots = subtrees.filter(t => t.root.parentExternalId.nonEmpty)
        if (nonRoots.nonEmpty) {
          throw InvalidTreeException(
            s"These subtrees are not connected to any root: ${nonRoots.map(_.root.externalId).mkString(", ")}."
              + " Did you meant to set option ignoreDisconnectedAssets or allowSubtreeIngestion to true?"
          )
        }
        subtrees
      }

    if (subtrees.nonEmpty && subtrees2.isEmpty) {
      // in case all nodes are dropped due to ignoreDisconnectedAssets
      throw NoRootException()
      // we can allow empty source data set, that won't really happen by accident
    }

    if (!allowMultipleRoots && subtrees2.size > 1) {
      throw MultipleRootsException(subtrees2.map(_.root.externalId))
    }

    for {
      _ <- validateSubtreeRoots(subtrees2.map(_.root))
      _ <- subtrees2.map(t => buildSubtree(t.root, t.nodes)).sequence_ // TODO: parallel?
    } yield ()
  }

  def validateSubtreeRoots(roots: Seq[AssetsIngestSchema]): IO[Unit] = {
    // check that all parentExternalId exist
    val ids = roots.map(_.parentExternalId).filter(_.nonEmpty)
    if (ids.isEmpty) {
      IO.unit
    } else {
      // The API calls throws exception when any of the ids does not exist
      client.assets
        .retrieveByExternalIds(ids)
        .adaptError({
          case e: CdpApiException if e.code == 400 && e.missing.nonEmpty => {
            val missingNodes = e.missing.get.map(j => j("externalId").get.asString.get).take(10)
            val referencingNodes =
              missingNodes
                .flatMap(missing => roots.find(_.parentExternalId == missing))
                .map(_.externalId)
            InvalidNodeReferenceException(missingNodes, referencingNodes)
          }
        }) *> IO.unit
    }
  }

  def buildSubtree(root: AssetsIngestSchema, children: Array[AssetsIngestSchema]): IO[Unit] =
    for {
      cdfRoot <- fetchCdfRoot(root.externalId)
      _ <- upsertRoot(root, cdfRoot)
      cdfSubtree <- fetchCdfSubtree(root)
      (toDelete, toInsert, toUpdate) = nodesToDeleteInsertUpdate(root, children, cdfSubtree)
      _ <- insert(toInsert, batchSize)
      _ <- update(toUpdate, batchSize)
      _ <- delete(toDelete, deleteMissingAssets, batchSize)
    } yield ()

  def insert(toInsert: Seq[AssetsIngestSchema], batchSize: Int): IO[Unit] = {
    val assetCreatesToInsert = toInsert.map(AssetsIngestSchema.toAssetCreate)
    // Traverse batches in order to ensure writing parents first
    assetCreatesToInsert
      .grouped(batchSize)
      .toList
      .traverse(client.assets.create)
      .flatTap(x => incMetrics(itemsCreated, x.map(_.size).sum)) *> IO.unit
  }

  def delete(toDelete: Seq[Asset], deleteMissingAssets: Boolean, batchSize: Int): IO[Unit] =
    if (deleteMissingAssets) {
      toDelete.flatMap(_.externalId) match {
        case Nil => IO.unit
        case externalIds =>
          externalIds
            .grouped(batchSize)
            .toVector
            .parTraverse(id => client.assets.deleteByExternalIds(id, true, true))
            // We report the total number of IDs that should not exist from this point, not the number of really deleted rows.
            // The API does not give us this information :/
            .flatTap(_ => incMetrics(itemsDeleted, externalIds.length)) *> IO.unit
      }
    } else {
      IO.unit
    }

  def update(toUpdate: Seq[AssetsIngestSchema], batchSize: Int): IO[Unit] =
    if (toUpdate.nonEmpty) {
      toUpdate
        .map(a => a.externalId -> AssetsIngestSchema.toAssetUpdate(a))
        .grouped(batchSize)
        .toVector
        .parTraverse(a => client.assets.updateByExternalId(a.toMap))
        .flatTap(x => incMetrics(itemsUpdated, x.map(_.size).sum)) *> IO.unit
    } else {
      IO.unit
    }

  def fetchCdfRoot(sourceRootExternalId: String): IO[Option[Asset]] =
    client.assets
      .retrieveByExternalId(sourceRootExternalId)
      .map(Some(_).asInstanceOf[Option[Asset]])
      .handleError {
        case e: CdpApiException if e.code == 400 && e.missing.isDefined =>
          None.asInstanceOf[Option[Asset]]
      }

  def fetchCdfSubtree(root: AssetsIngestSchema): IO[List[Asset]] =
    if (root.parentExternalId.isEmpty) {
      // proper root
      client.assets
        .filter(AssetsFilter(rootIds = Some(Seq(CogniteExternalId(root.externalId)))))
        .compile
        .toList
    } else {
      // This API will error out if the subtree contains more than 100,000 items
      // If that's a problem, we'll have to workaround by downloading it's entire hierarchy
      // (or convince the API team to lift that limitation)
      client.assets
        .filter(AssetsFilter(assetSubtreeIds = Some(Seq(CogniteExternalId(root.externalId)))))
        .compile
        .toList
    }

  def upsertRoot(
      root: AssetsIngestSchema,
      sourceRoot: Option[Asset]
  ): IO[Unit] = {
    val parentId = if (root.parentExternalId.isEmpty) {
      None
    } else {
      Some(root.parentExternalId)
    }

    // TODO: don't do the update when nothing is changed (waiting for Scala SDK parentExternalId)
    sourceRoot match {
      case None =>
        client.assets
          .create(Seq(AssetsIngestSchema.toAssetCreate(root).copy(parentExternalId = parentId)))
          .flatTap(x => incMetrics(itemsCreated, x.size))
          .map(x => x.head)
      case Some(asset) => {
        val parentIdUpdate = (parentId, asset.parentId) match {
          case (None, Some(oldParent)) =>
            throw CdfDoesNotSupportException(
              s"Can not make root from asset '${asset.externalId.get}' which is already inside asset '${oldParent}."
              // TODO: include parentExternalId in the message
                + " You might need to remove the asset and insert it again.")
          case (None, None) => None
          case (Some(newParent), _) => Some(SetValue(newParent))
        }
        client.assets
          .updateByExternalId(
            Seq(
              root.externalId -> AssetsIngestSchema
                .toAssetUpdate(root)
                .copy(parentExternalId = parentIdUpdate)).toMap)
          .flatTap(x => incMetrics(itemsUpdated, x.size))
          .map(x => x.head)
      }
    }
  }

  def nodesToDeleteInsertUpdate(
      root: AssetsIngestSchema,
      children: Seq[AssetsIngestSchema],
      cdfTree: Seq[Asset]): (Seq[Asset], Seq[AssetsIngestSchema], Seq[AssetsIngestSchema]) = {
    val newIds = (Seq(root) ++ children).map(_.externalId).toSet
    // Keep assets that are in both source and CDF, delete those only in CDF
    val toDelete =
      cdfTree.filter(a => !a.externalId.exists(newIds.contains))

    val cdfTreeMap = cdfTree.flatMap(a => a.externalId.map((_, a))).toMap
    // Insert assets that are not present in CDF
    val toInsert = children.filter(a => !cdfTreeMap.contains(a.externalId))

    val cdfTreeIdMap = cdfTree.map(a => (a.id, a)).toMap
    // Filter out assets that would not be changed in an update
    val toUpdate = children.filter(a => needsUpdate(a, cdfTreeMap, cdfTreeIdMap))

    (toDelete, toInsert, toUpdate)
  }

  def needsUpdate(
      child: AssetsIngestSchema,
      cdfTreeByExternalId: Map[String, Asset],
      cdfTreeById: Map[Long, Asset]): Boolean = {
    // Find the matching asset in CDF
    val cdfAsset = cdfTreeByExternalId.get(child.externalId)
    cdfAsset match {
      case Some(asset) if asset.parentId.isEmpty =>
        // it's root and should not be -> update
        true
      case Some(asset) =>
        // Find the parent asset in CDF to be able to get the externalId of the parent
        cdfTreeById.get(asset.parentId.get) match {
          case Some(parentAsset) =>
            !isMostlyEqual(child, asset) || !parentAsset.externalId.contains(child.parentExternalId)
          case None =>
            // this should not happen, except for races
            // in that case just do the update, just to be sure
            true
        }
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

  def isMostlyEqual(child: AssetsIngestSchema, asset: Asset): Boolean =
    child.description == asset.description &&
      child.metadata == asset.metadata &&
      child.name == asset.name &&
      child.source == asset.source &&
      child.dataSetId == asset.dataSetId

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
    //    val roots = tree.filter(a => a.parentExternalId.isEmpty || !assetMap.contains(a.parentExternalId))
//    val rootBuckets = Map(roots.map(r => r -> mutable.ArrayBuffer()): _*)
    tree
      .groupBy(getRoot(_))
      .map({
        case (root, items) => {
          assert(items.contains(root))
          AssetSubtree(root, orderChildren(root.externalId, items.filter(_ != root)))
        }
      })
      .toVector
  }

  def getValidatedTree(
      root: AssetsIngestSchema,
      children: Array[AssetsIngestSchema],
      ignoreDisconnectedAssets: Boolean): Seq[AssetsIngestSchema] = {
    val visited = Seq[AssetsIngestSchema]()
    val insertableTree = iterateChildren(children, Array(root.externalId), visited)
    val insertableTreeExternalIds = insertableTree.map(_.externalId)

    if (insertableTree.map(_.externalId).toSet == children.map(_.externalId).toSet) {
      insertableTree
    } else {
      val (assetsInTree, assetsNotInTree) =
        children.partition(a => insertableTreeExternalIds.contains(a.externalId))
      if (ignoreDisconnectedAssets) {
        assetsInTree
      } else {
        throw InvalidTreeException(
          s"Tree contains assets that are not connected to the root: ${assetsNotInTree.map(_.externalId).mkString(", ")}")
      }
    }
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

  def orderChildren(rootId: String, nodes: Array[AssetsIngestSchema]): Array[AssetsIngestSchema] =
    iterateChildren(nodes, Array(rootId), Seq()).toArray

  override def schema: StructType = structType[AssetsIngestSchema]
}

object AssetHierarchyBuilder {
  val upsertSchema = structType[AssetsIngestSchema]
}
