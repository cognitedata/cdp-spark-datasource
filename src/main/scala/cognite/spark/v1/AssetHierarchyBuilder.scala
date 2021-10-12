package cognite.spark.v1

import cats.effect.IO
import cats.effect.syntax.all._
import cats.implicits._
import cats.syntax._
import cognite.spark.v1.PushdownUtilities.stringSeqToCogniteExternalIdSeq
import cognite.spark.v1.SparkSchemaHelper.{fromRow, structType}
import com.cognite.sdk.scala.common.{CdpApiException, NonNullableSetter, SetValue}
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, AssetUpdate, AssetsFilter, CogniteExternalId}
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

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
final case class InvalidRootChangeException(assetIds: Seq[String], subtreeId: String, rootId: Long)
    extends CdfSparkException(
      {
        val commaSeparatedAssets = assetIds.mkString(", ")

        s"Attempted to move some assets to a different root asset in subtree $subtreeId under the rootId=$rootId. " +
          "If this is intended, the assets must be manually deleted and re-created under the new root asset. " +
          s"Assets with the following externalIds were attempted to be moved: $commaSeparatedAssets."
      }
    )

class AssetHierarchyBuilder(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdfRelation(config, "assethierarchy") {

  import CdpConnector.cdpConnectorContextShift

  def buildFromDf(data: DataFrame): Unit =
    // Do not use .collect to run the builder on one of the executors and not on the driver
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

  def validateSubtreeRoots(roots: Vector[AssetsIngestSchema]): IO[Unit] =
    // check that all `parentExternalId`s exist
    batchedOperation[String, Asset](
      roots.map(_.parentExternalId).filter(_.nonEmpty).distinct,
      // The API calls throw exception when any of the ids do not exist
      ids =>
        client.assets
          .retrieveByExternalIds(ids)
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
    ).void

  def buildSubtrees(trees: Vector[AssetSubtree]): IO[Unit] =
    for {
      // fetch existing roots and update or insert them first
      cdfRoots <- fetchCdfAssets(trees.map(_.root.externalId))
      // fetch existing nodes in CDF in order to know if we need to insert or update them
      cdfAssets <- fetchCdfAssets(trees.flatMap(_.nodes).map(_.externalId))
      (toInsert, toUpdate, _) = nodesToInsertUpdate(trees.flatMap(_.nodes), cdfAssets)

      upsertedRoots <- upsertRoots(trees.map(_.root), cdfRoots)

      _ = trees.foreach(validateNoRootChange(_, upsertedRoots, cdfAssets))

      _ <- deleteMissingChildren(trees, deleteMissingAssets)
      // insert must be done before update, since the updated node can reference node that is to be created
      _ <- insert(toInsert)
      _ <- update(toUpdate)
    } yield ()

  def insert(toInsert: Seq[AssetsIngestSchema]): IO[Vector[Asset]] = {
    val assetCreatesToInsert = toInsert.map(toAssetCreate)
    // Traverse batches in order to ensure writing parents first
    assetCreatesToInsert
      .grouped(batchSize)
      .toVector
      .traverse(
        client.assets
          .create(_)
          .flatTap(x => incMetrics(itemsCreated, x.size))
      )
      .map(_.flatten)
  }

  def deleteMissingChildren(trees: Vector[AssetSubtree], deleteMissingAssets: Boolean): IO[Unit] =
    if (deleteMissingAssets) {
      val ingestedNodeSet = trees.flatMap(_.allNodes).map(_.externalId).toSet
      // list all subtrees of the tree root and filter those which are not in the ingested set
      for {
        idsToDelete <- batchedOperation[String, Long](
          trees.map(_.root.externalId),
          ids =>
            client.assets
              .filter(AssetsFilter(assetSubtreeIds = Some(ids.map(CogniteExternalId(_)))))
              .compile
              .toVector
              .map(_.filter(a => !a.externalId.exists(ingestedNodeSet.contains))
                .map(_.id))
        )
        _ <- batchedOperation[Long, Nothing](
          idsToDelete,
          idBatch =>
            client.assets
              .deleteByIds(idBatch, recursive = true, ignoreUnknownIds = true)
              .flatTap(_ => incMetrics(itemsDeleted, idBatch.length))
              .as(Vector.empty)
        )
      } yield ()
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

  def fetchCdfAssets(sourceRootExternalIds: Vector[String]): IO[Map[String, Asset]] =
    batchedOperation[String, Asset](
      sourceRootExternalIds,
      batch =>
        client.assets
          .retrieveByExternalIds(batch, ignoreUnknownIds = true)
    ).map(_.map(a => a.externalId.get -> a).toMap)

  def upsertRoots( // scalastyle:off
      newRoots: Vector[AssetsIngestSchema],
      sourceRoots: Map[String, Asset]): IO[Vector[Asset]] = {

    // Assets without corresponding source root will be created
    val (toCreate, toUpdate, toIgnore) = nodesToInsertUpdate(newRoots, sourceRoots)

    val createdIO = insert(toCreate)

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

  def nodesToInsertUpdate(nodes: Vector[AssetsIngestSchema], cdfTree: Map[String, Asset])
    : (Vector[AssetsIngestSchema], Vector[AssetsIngestSchema], Vector[AssetsIngestSchema]) = {
    // Insert assets that are not present in CDF
    val (toUpdateOrIgnore, toInsert) = nodes.partition(cdfTree contains _.externalId)

    // Filter out assets that would not be changed in an update
    // Ignore assets which have a corresponding sourceRoot, but it has the same data
    val (rest, toUpdate) =
      toUpdateOrIgnore.partition(a => isMostlyEqual(a, cdfTree(a.externalId)))

    (toInsert, toUpdate, rest)
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

  def validateNoRootChange(
      source: AssetSubtree,
      roots: Iterable[Asset],
      assets: Map[String, Asset]): Unit = {
    // CDF does not support moving assets between roots, so we check that all assets are under the same root id
    val rootId = roots.find(_.externalId.contains(source.root.externalId)).flatMap(_.rootId).get

    val invalidNodes =
      source.nodes.filter(a => assets.get(a.externalId).flatMap(_.rootId).exists(_ != rootId))

    if (invalidNodes.nonEmpty) {
      throw InvalidRootChangeException(
        invalidNodes.map(_.externalId),
        source.root.externalId,
        rootId
      )
    }
  }

  def isMostlyEqual(updatedAsset: AssetsIngestSchema, asset: Asset): Boolean =
    updatedAsset.description.toOption == asset.description &&
      updatedAsset.metadata.getOrElse(Map()) == asset.metadata.getOrElse(Map()) &&
      updatedAsset.name == asset.name &&
      updatedAsset.source.toOption == asset.source &&
      updatedAsset.dataSetId.toOption == asset.dataSetId &&
      updatedAsset.labels.getOrElse(Seq()).map(CogniteExternalId(_)) == asset.labels.getOrElse(Seq()) &&
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
      parents: Set[String],
      visited: Seq[AssetsIngestSchema]): Seq[AssetsIngestSchema] = {
    val nextChildren = currentChildren.filter(a => parents.contains(a.parentExternalId))
    if (nextChildren.isEmpty) {
      visited
    } else {
      iterateChildren(currentChildren, nextChildren.map(_.externalId).toSet, visited ++ nextChildren)
    }
  }

  def orderChildren(rootId: String, nodes: Array[AssetsIngestSchema]): Vector[AssetsIngestSchema] =
    iterateChildren(nodes, Set(rootId), Seq()).toVector

  override def schema: StructType = structType[AssetsIngestSchema]

  def toAssetCreate(a: AssetsIngestSchema): AssetCreate =
    a.into[AssetCreate]
      .withFieldComputed(_.parentExternalId, _.optionalParentId)
      .withFieldComputed(_.labels, a => stringSeqToCogniteExternalIdSeq(a.labels))
      .transform

  def toAssetUpdate(a: AssetsIngestSchema): AssetUpdate =
    a.into[AssetUpdate].transform

  private def batchedOperation[I, R](
      list: Vector[I],
      op: Vector[I] => IO[Seq[R]],
      batchSize: Int = this.batchSize): IO[Vector[R]] =
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
