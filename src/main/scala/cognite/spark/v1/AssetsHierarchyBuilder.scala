package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.SparkSchemaHelper.{fromRow, structType}
import com.cognite.sdk.scala.common.SetValue
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, AssetUpdate, AssetsFilter, CogniteExternalId}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

final case class AssetsIngestSchema(
    externalId: String,
    parentExternalId: String,
    source: Option[String],
    name: String,
    description: Option[String],
    metadata: Option[Map[String, String]])

object AssetsIngestSchema {
  def toAssetCreate(a: AssetsIngestSchema): AssetCreate =
    AssetCreate(
      name = a.name,
      description = a.description,
      externalId = Some(a.externalId),
      metadata = a.metadata,
      source = a.source,
      parentExternalId = Some(a.parentExternalId)
    )

  def toAssetUpdate(a: AssetsIngestSchema): AssetUpdate =
    AssetUpdate(
      name = Some(SetValue(a.name)),
      description = a.description.map(SetValue(_)),
      externalId = Some(SetValue(a.externalId)),
      metadata = a.metadata.map(SetValue(_)),
      source = a.source.map(SetValue(_)),
      parentExternalId = Some(SetValue(a.parentExternalId))
    )
}

final case class MultipleRootsException(message: String = "Tree has more than one root.")
    extends Exception(message)
final case class NoRootException(
    message: String = """Tree has no root. Set parentExternalId to "" for the root Asset.""")
    extends Exception(message)
final case class InvalidTreeException(message: String = s"The tree is has an invalid structure.")
    extends Exception(message)

class AssetsHierarchyBuilder(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdfRelation(config, "assetshierarchy") {

  import CdpConnector.cdpConnectorContextShift

  def build(df: DataFrame): IO[Unit] = {
    val deleteMissingAssets = config.deleteMissingAssets
    val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

    val sourceTree = df.collect.map(r => fromRow[AssetsIngestSchema](r))

    val (root, children) = validateAndOrderInput(sourceTree)

    for {
      rootAndChildren <- getCdfRootAndTree(CogniteExternalId(root.externalId))
      (cdfRoot, cdfChildren) = (rootAndChildren._1, rootAndChildren._2)
      _ <- upsertRoot(root, cdfRoot)
      (toDelete, toInsert, toUpdate) = nodesToDeleteInsertUpdate(root, children, cdfChildren)
      _ <- insert(toInsert, batchSize)
      _ <- update(toUpdate, batchSize)
      _ <- delete(toDelete, deleteMissingAssets, batchSize)
    } yield ()
  }

  def insert(toInsert: Seq[AssetsIngestSchema], batchSize: Int): IO[Unit] =
    if (toInsert.nonEmpty) {
      val assetCreatesToInsert = toInsert.map(AssetsIngestSchema.toAssetCreate)
      // Traverse batches in order to ensure writing parents first
      assetCreatesToInsert
        .grouped(batchSize)
        .toList
        .traverse(client.assets.create) *> IO.unit
    } else {
      IO.unit
    }

  def delete(toDelete: Seq[Asset], deleteMissingAssets: Boolean, batchSize: Int): IO[Unit] =
    if (deleteMissingAssets) {
      toDelete.flatMap(_.externalId) match {
        case Nil => IO.unit
        case externalIds =>
          externalIds
            .grouped(batchSize)
            .toVector
            .parTraverse(id => client.assets.deleteByExternalIds(id, true, true)) *> IO.unit
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
        .parTraverse(a => client.assets.updateByExternalId(a.toMap)) *> IO.unit
    } else {
      IO.unit
    }

  def getCdfRootAndTree(sourceRootExternalId: CogniteExternalId): IO[(Option[Asset], List[Asset])] = {
    val cdfTree = client.assets
      .filter(AssetsFilter(rootIds = Some(Seq(sourceRootExternalId))))
      .compile
      .toList

    for {
      tree <- cdfTree
      root = tree.find(_.externalId.contains(sourceRootExternalId.externalId))
    } yield (root, tree)
  }

  def upsertRoot(
      root: AssetsIngestSchema,
      sourceRoot: Option[Asset]
  ): IO[Unit] =
    sourceRoot match {
      case None =>
        client.assets.create(Seq(AssetsIngestSchema.toAssetCreate(root).copy(parentExternalId = None))) *> IO.unit
      case Some(asset) =>
        if (isMostlyEqual(root, asset)) {
          IO.unit
        } else {
          client.assets.updateByExternalId(
            Seq(
              root.externalId -> AssetsIngestSchema
                .toAssetUpdate(root)
                .copy(parentExternalId = None)).toMap) *> IO.unit
        }
    }

  def nodesToDeleteInsertUpdate(
      root: AssetsIngestSchema,
      children: Seq[AssetsIngestSchema],
      cdfTree: Seq[Asset]): (Seq[Asset], Seq[AssetsIngestSchema], Seq[AssetsIngestSchema]) = {
    // Insert assets that are not present in CDF
    val toInsert = children.filter(a => !cdfTree.map(_.externalId.get).contains(a.externalId))

    // Keep assets that are in both source and CDF, delete those only in CDF
    val (toKeep, toDelete) =
      cdfTree.partition(a => (Seq(root) ++ children).map(_.externalId).contains(a.externalId.get))

    // Filter out assets that would not be changed in an update
    val toUpdate = children.filter(a => needsUpdate(a, toKeep))

    (toDelete, toInsert, toUpdate)
  }

  def needsUpdate(child: AssetsIngestSchema, cdfTree: Seq[Asset]): Boolean = {
    // Find the matching Asset in CDF
    val cdfAsset = cdfTree.find(a => a.externalId.contains(child.externalId))
    cdfAsset match {
      case Some(asset) =>
        // Find the parent Asset in CDF to be able to get the externalId of the parent
        cdfTree.find(a => asset.parentId.contains(a.id)) match {
          case Some(parentAsset) =>
            !isMostlyEqual(child, asset) || !parentAsset.externalId.contains(child.parentExternalId)
          case None =>
            false
        }
      case None => false
    }
  }

  def isMostlyEqual(child: AssetsIngestSchema, asset: Asset): Boolean =
    child.description == asset.description &&
      child.metadata == asset.metadata &&
      child.name == asset.name &&
      child.source == asset.source

  def validateAndOrderInput(
      tree: Array[AssetsIngestSchema]): (AssetsIngestSchema, Seq[AssetsIngestSchema]) = {
    val (root, children) = splitIntoRootAndChildren(tree)

    (root, getValidatedTree(root, children))
  }

  def splitIntoRootAndChildren(
      tree: Array[AssetsIngestSchema]): (AssetsIngestSchema, Array[AssetsIngestSchema]) = {
    val (maybeRoots, children) = tree.partition(_.parentExternalId == "")

    val root = maybeRoots match {
      case Array(singleRoot) => singleRoot
      case Array() => throw NoRootException()
      case Array(_, _*) => throw MultipleRootsException()
    }
    (root, children)
  }

  def getValidatedTree(
      root: AssetsIngestSchema,
      children: Array[AssetsIngestSchema]): Seq[AssetsIngestSchema] = {
    val visited = Seq[AssetsIngestSchema]()
    val insertableTree = iterateChildren(children, Array(root.externalId), visited)
    val insertableTreeExternalIds = insertableTree.map(_.externalId)

    if (insertableTree.map(_.externalId).toSet == children.map(_.externalId).toSet) {
      insertableTree
    } else {
      val assetsNotInTree =
        children.filter(a => insertableTreeExternalIds.contains(a.externalId)).map(_.externalId)
      throw InvalidTreeException(
        s"Tree contains assets that are not connected to the root: ${assetsNotInTree.mkString(", ")}")
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

  override def schema: StructType = structType[AssetsIngestSchema]
}
