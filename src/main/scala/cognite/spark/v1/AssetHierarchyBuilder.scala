package cognite.spark.v1

import cats.effect.IO
import cats.effect.syntax._
import cats.implicits._
import cats.syntax._
import cognite.spark.v1.PushdownUtilities.stringSeqToCogniteExternalIdSeq
import cognite.spark.v1.SparkSchemaHelper.{fromRow, structType}
import com.cognite.sdk.scala.common.{CdpApiException, SetNull, SetValue}
import com.cognite.sdk.scala.v1.{
  Asset,
  AssetCreate,
  AssetUpdate,
  AssetsFilter,
  CogniteExternalId,
  LabelsOnUpdate
}
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
    dataSetId: Option[Long],
    labels: Option[Seq[String]])

object AssetsIngestSchema {
  def toAssetCreate(a: AssetsIngestSchema): AssetCreate =
    AssetCreate(
      name = a.name,
      description = a.description,
      externalId = Some(a.externalId),
      metadata = a.metadata,
      source = a.source,
      parentExternalId = Some(a.parentExternalId),
      dataSetId = a.dataSetId,
      labels = stringSeqToCogniteExternalIdSeq(a.labels)
    )

  def toAssetUpdate(a: AssetsIngestSchema): AssetUpdate =
    AssetUpdate(
      name = Some(SetValue(a.name)),
      description = a.description.map(SetValue(_)),
      externalId = Some(SetValue(a.externalId)),
      metadata = a.metadata.map(SetValue(_)),
      source = a.source.map(SetValue(_)),
      parentExternalId = Some(SetValue(a.parentExternalId)),
      dataSetId = a.dataSetId.map(SetValue(_)),
      labels = a.labels match {
        case labelList: Some[Seq[String]] =>
          Some(LabelsOnUpdate(add = stringSeqToCogniteExternalIdSeq(labelList)))
        case _ => None
      }
    )

}

final case class AssetSubtree(
    // node that contains all the `nodes`
    // might be just a pseudo-root
    root: AssetsIngestSchema,
    nodes: Array[AssetsIngestSchema]
)

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
final case class InvalidRootChangeException(cause: CdpApiException, newSubtreeRoot: Asset)
    extends CdfSparkException(
      {
        val commaSeparatedAssets =
          cause.duplicated.get
            .map {
              _.toIterable
                .map { case (key, value) => s"$key=${value.asString.getOrElse(value.toString)}" }
                .mkString("(", ", ", ")")
            }
            .mkString(", ")

        val withRootAssetId = newSubtreeRoot.rootId.map(id => s" with id $id").getOrElse("")

        s"Attempted to move some assets to a different root asset$withRootAssetId. " +
          "If this is intended, the assets must be manually deleted and re-created under the new root asset. " +
          s"The following assets were attempted to be moved: $commaSeparatedAssets."
      },
      cause
    )

class AssetHierarchyBuilder(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdfRelation(config, "assethierarchy") {

  import CdpConnector.cdpConnectorContextShift

  private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

  private val deleteMissingAssets = config.deleteMissingAssets
  private val subtreeMode = config.subtrees

  def build(df: DataFrame): IO[Unit] = {
    val sourceTree = df.collect.map(r => fromRow[AssetsIngestSchema](r))

    val subtrees =
      validateAndOrderInput(sourceTree)
        .sortBy(_.root.externalId) // make potential errors deterministic

    val subtrees2 = subtreeMode match {
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

    if (subtrees.nonEmpty && subtrees2.isEmpty) {
      // in case all nodes are dropped due to ignoreDisconnectedAssets
      throw NoRootException()
      // we can allow empty source data set, that won't really happen by accident
    }

    for {
      _ <- validateSubtreeRoots(subtrees2.map(_.root))
      _ <- subtrees2.map(t => buildSubtree(t.root, t.nodes)).sequence_ // TODO: parallel?
    } yield ()
  }

  def validateSubtreeRoots(roots: Seq[AssetsIngestSchema]): IO[Unit] = {
    // check that all `parentExternalId`s exist
    val ids = roots.map(_.parentExternalId).filter(_.nonEmpty)
    if (ids.isEmpty) {
      IO.unit
    } else {
      // The API calls throw exception when any of the ids do not exist
      client.assets
        .retrieveByExternalIds(ids.distinct)
        .adaptError({
          case e: CdpApiException if e.code == 400 && e.missing.nonEmpty =>
            val missingNodes = e.missing.get.map(j => j("externalId").get.asString.get).take(10)
            val referencingNodes =
              missingNodes
              // always take the one with "lowest" externalId, so the errors are deterministic
                .map(missing => roots.filter(_.parentExternalId == missing).minBy(_.externalId))
                .map(_.externalId)
            InvalidNodeReferenceException(missingNodes, referencingNodes)
        }) *> IO.unit
    }
  }

  def buildSubtree(root: AssetsIngestSchema, children: Array[AssetsIngestSchema]): IO[Unit] =
    for {
      cdfRoot <- fetchCdfRoot(root.externalId)
      upsertedCdfRoot <- upsertRoot(root, cdfRoot)
      cdfSubtree <- fetchCdfSubtree(root)
      (toDelete, toInsert, toUpdate) = nodesToDeleteInsertUpdate(root, children, cdfSubtree)
      _ <- insert(toInsert, upsertedCdfRoot, batchSize)
      _ <- update(toUpdate, batchSize)
      _ <- delete(toDelete, deleteMissingAssets, batchSize)
    } yield ()

  def insert(toInsert: Seq[AssetsIngestSchema], newSubtreeRoot: Asset, batchSize: Int): IO[Unit] = {
    val assetCreatesToInsert = toInsert.map(AssetsIngestSchema.toAssetCreate)
    // Traverse batches in order to ensure writing parents first
    assetCreatesToInsert
      .grouped(batchSize)
      .toList
      .traverse(client.assets.create)
      .flatTap(x => incMetrics(itemsCreated, x.map(_.size).sum))
      .as(())
      .recoverWith {
        case ex: CdpApiException if ex.duplicated.isDefined =>
          // If we've hit this case, the assets already exist in CDF, but under a different root asset.
          // Since we didn't find them under the subtree, we assumed they did not exist, and attempted to to insert them,
          // resulting in this error.
          IO.raiseError(InvalidRootChangeException(ex, newSubtreeRoot))
      }
  }

  def delete(toDelete: Seq[Asset], deleteMissingAssets: Boolean, batchSize: Int): IO[Unit] =
    if (deleteMissingAssets) {
      val externalIds = toDelete.flatMap(_.externalId)
      externalIds
        .grouped(batchSize)
        .toVector
        .parTraverse(id => client.assets.deleteByExternalIds(id, true, true))
        // We report the total number of IDs that should not exist from this point, not the number of really deleted rows.
        // The API does not give us this information :/
        .flatTap(_ => incMetrics(itemsDeleted, externalIds.length)) *> IO.unit
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
      .map(Some(_): Option[Asset])
      .recover {
        case e: CdpApiException if e.code == 400 && e.missing.isDefined =>
          None: Option[Asset]
      }

  def fetchCdfSubtree(root: AssetsIngestSchema): IO[List[Asset]] =
    if (root.parentExternalId.isEmpty) {
      // proper root
      client.assets
        .filter(AssetsFilter(rootIds = Some(Seq(CogniteExternalId(root.externalId)))))
        .compile
        .toList
    } else {
      // The API will error out if the subtree contains more than 100,000 items
      // If that's a problem, we'll have to do a workaround by downloading its entire hierarchy
      // (or convince the API team to lift that limitation)
      client.assets
        .filter(AssetsFilter(assetSubtreeIds = Some(Seq(CogniteExternalId(root.externalId)))))
        .compile
        .toList
    }

  def upsertRoot( // scalastyle:off
      newRoot: AssetsIngestSchema,
      sourceRoot: Option[Asset]): IO[Asset] = {
    val parentId = if (newRoot.parentExternalId.isEmpty) {
      None
    } else {
      Some(newRoot.parentExternalId)
    }

    sourceRoot match {
      case None =>
        client.assets
          .create(Seq(AssetsIngestSchema.toAssetCreate(newRoot).copy(parentExternalId = parentId)))
          .flatTap(x => incMetrics(itemsCreated, x.size))
          .map(_.head)
      case Some(asset) if isMostlyEqual(newRoot, asset) =>
        IO(asset)
      case Some(asset) =>
        val parentIdUpdate = (parentId, asset.parentId) match {
          case (None, Some(oldParent)) =>
            throw CdfDoesNotSupportException(
              s"Can not make root from asset '${asset.externalId.get}' which is already inside asset ${oldParent}"
                + s" (${asset.parentExternalId.getOrElse("without externalId")})."
                + " You might need to remove the asset and insert it again.")
          case (None, None) => None
          case (Some(newParent), _) if asset.parentExternalId.contains(newParent) => None
          case (Some(newParent), _) => Some(SetValue(newParent))
        }
        client.assets
          .updateByExternalId(
            Seq(
              newRoot.externalId -> AssetsIngestSchema
                .toAssetUpdate(newRoot)
                .copy(parentExternalId = parentIdUpdate)).toMap)
          .flatTap(x => incMetrics(itemsUpdated, x.size))
          .map(_.head)
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

    // Filter out assets that would not be changed in an update
    val toUpdate = children.filter(a => needsUpdate(a, cdfTreeMap))

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
    updatedAsset.description == asset.description &&
      updatedAsset.metadata.getOrElse(Map()) == asset.metadata.getOrElse(Map()) &&
      updatedAsset.name == asset.name &&
      updatedAsset.source == asset.source &&
      updatedAsset.dataSetId == asset.dataSetId &&
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

  def orderChildren(rootId: String, nodes: Array[AssetsIngestSchema]): Array[AssetsIngestSchema] =
    iterateChildren(nodes, Array(rootId), Seq()).toArray

  override def schema: StructType = structType[AssetsIngestSchema]
}

object AssetHierarchyBuilder {
  val upsertSchema: StructType = structType[AssetsIngestSchema]
}
