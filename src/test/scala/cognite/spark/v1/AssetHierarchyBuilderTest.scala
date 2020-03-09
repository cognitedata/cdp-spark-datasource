package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.AssetCreate
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class AssetHierarchyBuilderTest extends FlatSpec with Matchers with SparkTest {
  import spark.implicits._

  private val assetsSourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "assets")
    .load()
  assetsSourceDf.createOrReplaceTempView("assets")

  val testName = "assetHierarchyTest"

  private def ingest(
      tree: Seq[AssetCreate],
      metricsPrefix: Option[String] = None,
      batchSize: Int = 2,
      allowSubtreeIngestion: Boolean = true,
      ignoreDisconnectedAssets: Boolean = false,
      allowMultipleRoots: Boolean = true,
      deleteMissingAssets: Boolean = false): Unit =
      spark.sparkContext.parallelize(tree).toDF().write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assethierarchy")
        .option("collectMetrics", metricsPrefix.isDefined)
        .option("allowSubtreeIngestion", allowSubtreeIngestion)
        .option("ignoreDisconnectedAssets", ignoreDisconnectedAssets)
        .option("allowMultipleRoots", allowMultipleRoots)
        .option("deleteMissingAssets", deleteMissingAssets)
        .option("batchSize", batchSize)
        .option("metricsPrefix", metricsPrefix.getOrElse(""))
        .save

  private def cleanDB() =
    writeClient.assets.deleteByExternalIds(Seq("dad", "dad2", "unusedZero", "son"), true, true)


  it should "throw an error when everything is ignored" in {
    val tree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("someNode"))
    )
    val e = intercept[Exception] {
      ingest(tree, allowSubtreeIngestion = false, ignoreDisconnectedAssets = true)
    }
    e shouldBe an[NoRootException]
  }

  it should "throw an error if some nodes are disconnected from the root" in {
    val brokenTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("othertree", None, None, Some(testName),Some("other"), None, Some("otherDad"))
    )
    val e = intercept[Exception] {
      ingest(brokenTree, allowSubtreeIngestion = false)
    }
    e shouldBe an[InvalidTreeException]
  }

  it should "throw an error if there are multiple roots" in {
    val multiRootTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("")),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("othertree", None, None, Some(testName),Some("other"), None, Some("otherDad"))
    )
    val e = intercept[Exception] {
      ingest(multiRootTree, allowMultipleRoots = false)
    }
    e shouldBe an[MultipleRootsException]
  }

  it should "throw an error if there is a cycle" in {
    val cyclicHierarchy = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("daughter")),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("daughterSon")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("other")),
      AssetCreate("othertree", None, None, Some(testName),Some("other"), None, Some("son"))
    )
    val e = intercept[Exception] {
      ingest(cyclicHierarchy)
    }
    e shouldBe an[InvalidTreeException]
    val errorMessage = e.getMessage
    errorMessage should include("son")
    errorMessage should include("daughter")
    errorMessage should include("daughterSon")
    errorMessage should include("other")
  }

  it should "throw an error if one more externalIds are empty Strings" in {
    val e = intercept[Exception] {
      ingest(Seq(
        AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
        AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
        AssetCreate("daughter", None, None, Some(testName),Some(""), None, Some("dad")),
        AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
        AssetCreate("othertree", None, None, Some(testName),Some(""), None, Some("otherDad"))
      ))
    }
    e shouldBe an[EmptyExternalIdException]
    val errorMessage = e.getMessage
    errorMessage should include("daughter")
    errorMessage should include("othertree")
    errorMessage should not include("daughterSon")
  }

  it should "fail reasonably when parent does not exist" in {
    val tree = Seq(
      AssetCreate("testNode1", None, None, Some(testName),Some("testNode1"), None, Some("nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds")),
      AssetCreate("testNode2", None, None, Some(testName),Some("testNode2"), None, Some(""))
    )

    val ex = intercept[Exception] { ingest(tree) }
    ex shouldBe an[InvalidNodeReferenceException]
    ex.getMessage shouldBe "Node 'nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds' referenced from 'testNode1' does not exist."
  }

  it should "fail when subtree is updated into being root" in {
    cleanDB()

    val assetTree = Seq(
      AssetCreate("dad", None, None, Some(testName), Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName), Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName), Some("daughterSon"), None, Some("daughter"))
    )

    ingest(assetTree)

    val ex = intercept[Exception] {
      ingest(Seq(
        AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("")),
      ))
    }

    ex shouldBe an[CdfDoesNotSupportException]
    ex.getMessage should include("daughter")
  }

  // although these test basically only test what CDF does, we want
  // * to be sure that error message is not swallowed
  // * to be notified if this behavior changes, as then we should also support it
  //   or (at least) fail reasonably
  it should "fail when merging trees" in {
    cleanDB()

    ingest(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("dad2", None, None, Some(testName),Some("dad2"), None, Some("")),
      AssetCreate("son2", None, None, Some(testName),Some("son2"), None, Some("dad2")),
    ))

    val ex = intercept[CdpApiException] {
      ingest(Seq(
        AssetCreate("dad2-updated", None, None, Some(testName),Some("dad2"), None, Some("son")),
      ))
    }
    ex.getMessage should include("Changing from/to being root isn't allowed")
  }

  it should "fail when node moves between trees" in {
    cleanDB()

    ingest(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("dad2", None, None, Some(testName),Some("dad2"), None, Some("")),
    ))

    val ex = intercept[CdpApiException] {
      ingest(Seq(
        AssetCreate("son", None, None, Some(testName), Some("son"), None, Some("dad2")),
      ))
    }
    ex.getMessage should include("Asset must stay within same asset hierarchy")
  }

  it should "ingest an asset tree" in {
    cleanDB()

    val ds = Some(testDataSetId)

    val assetTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some(""), ds),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad"), ds),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad"), ds),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter"), ds)
    )

    ingest(assetTree)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName' and dataSetId = $testDataSetId").collect,
      rows => rows.map(r => r.getString(1)).toSet != assetTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("son")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughterSon")).parentId.contains(extIdMap(Some("daughter")).id))
    assert(extIdMap(Some("dad")).dataSetId == ds)
  }

  it should "ingest an asset tree, then update it" in {
    cleanDB()

    val ds = Some(testDataSetId)

    val originalTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad"), dataSetId = ds),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("sonDaughter", None, None, Some(testName),Some("sonDaughter"), None, Some("son")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("secondDaughterToBeDeleted", None, None, Some(testName),Some("secondDaughter"), None, Some("dad"))
    )

    ingest(originalTree, batchSize = 3)

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some(""), dataSetId = None),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad"), dataSetId = None),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad"), dataSetId = ds),
      AssetCreate("sonDaughter", None, None, Some(testName), Some("sonDaughter"), None, Some("daughter")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter"))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(updatedTree, deleteMissingAssets = true)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("sonDaughter")).parentId.contains(extIdMap(Some("daughter")).id))
    assert(extIdMap.get(Some("secondDaughterToBeDeleted")).isEmpty)
    assert(extIdMap(Some("daughter")).dataSetId == ds)
    assert(extIdMap(Some("son")).dataSetId == ds)
    assert(extIdMap(Some("dad")).dataSetId == None)
  }

  it should "move an asset to another asset that is being moved" in {
    cleanDB()

    val originalTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("sonChild", None, None, Some(testName),Some("sonChild"), None, Some("son"))
    )

    ingest(originalTree, batchSize = 5)

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 4)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("son")),
      AssetCreate("daughterChildUpdated", None, None, Some(testName),Some("sonChild"), None, Some("daughter")),
      AssetCreate("daughterChildTwo", None, None, Some(testName),Some("daughterChildTwo"), None, Some("daughter"))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(updatedTree, batchSize = 1)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("son")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("son")).id))
    assert(extIdMap(Some("sonChild")).parentId.contains(extIdMap(Some("daughter")).id))
    assert(extIdMap(Some("daughterChildTwo")).parentId.contains(extIdMap(Some("daughter")).id))
  }

  it should "avoid deleting assets when deleteMissingAssets is false" in {
    cleanDB()

    val originalTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad"))
    )

    ingest(originalTree, batchSize = 3)

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 3)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("newSibling", None, None, Some(testName),Some("newSibling"), None, Some("dad"))
    )

    ingest(updatedTree, deleteMissingAssets = false)

    val allNames = updatedTree.map(_.name) ++ Seq("son", "daughter")

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != allNames.toSet)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("son")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("newSibling")).parentId.contains(extIdMap(Some("dad")).id))
  }

  it should "allow rearranging orders and depth of assets" in {
    cleanDB()

    val metricsPrefix = "update.assetsHierarchy.subtreeDepth"
    ingest(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName), Some("daughterSon"), None, Some("daughter")),
      AssetCreate("daughterDaughter", None, None, Some(testName), Some("daughterDaughter"), None, Some("daughter")),
      AssetCreate("sonSon", None, None, Some(testName), Some("sonSon"), None, Some("son")),
      AssetCreate("sonDaughter", None, None, Some(testName), Some("sonDaughter"), None, Some("son"))
    ))

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 7)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("daughter_ofDaughterSon", None, None, Some(testName), Some("daughter"), None, Some("daughterSon")),
      AssetCreate("daughterSon_ofDad", None, None, Some(testName), Some("daughterSon"), None, Some("daughterDaughter")),
      AssetCreate("daughterDaughter", None, None, Some(testName), Some("daughterDaughter"), None, Some("dad")),
      AssetCreate("hen", None, None, Some(testName), Some("hen"), None, Some("dad"))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(updatedTree, deleteMissingAssets = true, batchSize = 3, metricsPrefix = Some(metricsPrefix))

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("daughterDaughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughterSon")).parentId.contains(extIdMap(Some("daughterDaughter")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("daughterSon")).id))

    getNumberOfRowsUpdated(metricsPrefix, "assethierarchy") shouldBe 4
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 1
    getNumberOfRowsDeleted(metricsPrefix, "assethierarchy") shouldBe 3
  }

  it should "ingest an asset tree, then successfully delete a subtree" in {
    cleanDB()

    ingest(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("son")),
      AssetCreate("daughter2", None, None, Some(testName), Some("daughter2"), None, Some("daughter")),
      AssetCreate("daughter3", None, None, Some(testName), Some("daughter3"), None, Some("daughter2")),
      AssetCreate("daughter4", None, None, Some(testName), Some("daughter4"), None, Some("daughter3"))
    ))

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some(""))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(updatedTree, deleteMissingAssets = true)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet
    )
    assert(result.map(r => r.getString(1)).toSet == updatedTree.map(_.name).toSet)
  }

  it should "insert a tree while ignoring assets that are not connected when setting the ignoreDisconnectedAssets option to true" in {
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)

    val assetTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("unusedZero", None, None, Some(testName),Some("unusedZero"), None, Some("nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds")),
      AssetCreate("unusedOne", None, None, Some(testName),Some("unusedOne"), None, Some("unusedZero")),
      AssetCreate("unusedTwo", None, None, Some(testName),Some("unusedTwo"), None, Some("unusedOne")),
      AssetCreate("unusedThree", None, None, Some(testName),Some("unusedThree"), None, Some("unusedTwo"))
    )

    val metricsPrefix = "insert.assetsHierarchy.ignored"
    ingest(assetTree, allowSubtreeIngestion = false, ignoreDisconnectedAssets = true, metricsPrefix = Some(metricsPrefix))

    val namesOfAssetsToInsert = assetTree.slice(0, 4).map(_.name).toSet

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != namesOfAssetsToInsert
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("son")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughterSon")).parentId.contains(extIdMap(Some("daughter")).id))

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 4
  }

  it should "insert a tree and then add subtree" in {
    cleanDB()

    val metricsPrefix = "insert.assetsHierarchy.ingest.subtree"

    ingest(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad"))
    ), metricsPrefix = Some(metricsPrefix))

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != Set("dad", "son")
    )

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 2

    ingest(Seq(
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("sonSon", None, None, Some(testName),Some("sonSon"), None, Some("son"))
    ), metricsPrefix = Some(metricsPrefix))

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != Set("dad", "son", "daughter", "daughterSon", "sonSon")
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("sonSon")).parentId.contains(extIdMap(Some("son")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughterSon")).parentId.contains(extIdMap(Some("daughter")).id))

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 5
  }

  it should "allow updating different subtrees" in {
    cleanDB()
    val metricsPrefix = "insert.assetsHierarchy.update.subtrees"

    //                    dad
    //                   /   \
    //                  /     \
    //               son       daughter
    //              /          |       \
    //          path0      daughterSon  daughterDaughter
    //            |
    //          path1
    //            |
    //          path2
    //            |
    //          path3

    val sourceTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("daughterDaughter", None, None, Some(testName),Some("daughterDaughter"), None, Some("daughter")),
      AssetCreate("path0", None, None, Some(testName),Some("path0"), None, Some("son")),
      AssetCreate("path1", None, None, Some(testName),Some("path1"), None, Some("path0")),
      AssetCreate("path2", None, None, Some(testName),Some("path2"), None, Some("path1")),
      AssetCreate("path3", None, None, Some(testName),Some("path3"), None, Some("path2"))
    )
    ingest(sourceTree, metricsPrefix = Some(metricsPrefix))
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe sourceTree.length

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != sourceTree.length
    )

    //                    dad
    //                   /   \
    //                  /     \
    //               son       daughter
    //                         |       \
    //                     daughterSon  daughterDaughter
    //                                         |
    //                                       path0
    //                                         |
    //                                       path1
    //                                         |
    //                                       path2*
    //                                         |
    //                                       path3

    ingest(Seq(
      AssetCreate("path0-updated", None, None, Some(testName), Some("path0"), None, Some("daughterDaughter")),
      AssetCreate("path2-updated", None, Some("desc"), Some(testName), Some("path2"), None, Some("path1")),
      AssetCreate("daughter-updated", None, Some("desc"), Some(testName),Some("daughter"), None, Some("dad")),
      AssetCreate("dad-updated", None, Some("desc"), Some(testName),Some("dad"), None, Some("")),
    ), metricsPrefix = Some(metricsPrefix))

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).count(_.endsWith("-updated")) != 4
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("path0")).parentId.contains(extIdMap(Some("daughterDaughter")).id))
    assert(extIdMap(Some("daughter")).description.contains("desc"))
    assert(extIdMap(Some("dad")).description.contains("desc"))
    assert(extIdMap(Some("path2")).description.contains("desc"))

    getNumberOfRowsUpdated(metricsPrefix, "assethierarchy") shouldBe 4
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe sourceTree.length
  }

  def getAssetsMap(assets: Seq[Row]): Map[Option[String], AssetsReadSchema] =
    assets.map(r => fromRow[AssetsReadSchema](r))
      .map(a => a.externalId -> a).toMap
}
