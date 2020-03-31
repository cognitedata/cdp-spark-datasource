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

  val testName = "assetHierarchyTest-"

  private def ingest(
      key: String,
      tree: Seq[AssetCreate],
      metricsPrefix: Option[String] = None,
      batchSize: Int = 2,
      subtrees: String = "ingest",
      deleteMissingAssets: Boolean = false): Unit = {
    def addKey(id: String) =
      if (id == "") {
        ""
      } else {
        s"$id$key"
      }
    val processedTree = tree.map(
      node =>
        node.copy(
          externalId = Some(addKey(node.externalId.get)),
          parentExternalId = Some(addKey(node.parentExternalId.get)),
          source = Some(s"$testName$key")
      ))
    spark.sparkContext
      .parallelize(processedTree)
      .toDF()
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assethierarchy")
      .option("collectMetrics", metricsPrefix.isDefined)
      .option("subtrees", subtrees)
      .option("deleteMissingAssets", deleteMissingAssets)
      .option("batchSize", batchSize)
      .option("metricsPrefix", metricsPrefix.getOrElse(""))
      .save
  }

  private def cleanDB(key: String) =
    writeClient.assets.deleteByExternalIds(
      Seq(s"dad$key", s"dad2$key", s"unusedZero$key", s"son$key"),
      true,
      true)

  it should "throw an error when everything is ignored" in {
    val key = shortRandomString()
    val tree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("someNode"))
    )
    val e = intercept[Exception] {
      ingest(key, tree, subtrees = "ignore")
    }
    e shouldBe an[NoRootException]
  }

  it should "throw an error if some nodes are disconnected from the root" in {
    val key = shortRandomString()
    val brokenTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", externalId = Some("daughterSon"), parentExternalId = Some("daughter")),
      AssetCreate("othertree", None, None, None, Some("other"), None, Some("otherDad"))
    )
    val e = intercept[Exception] {
      ingest(key, brokenTree, subtrees = "error")
    }
    e shouldBe an[InvalidTreeException]
  }

  it should "throw an error if there is a cycle" in {
    val key = shortRandomString()
    val cyclicHierarchy = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("daughter")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("daughterSon")),
      AssetCreate("daughterSon", None, None, None, Some("daughterSon"), None, Some("other")),
      AssetCreate("othertree", None, None, None, Some("other"), None, Some("son"))
    )
    val e = intercept[Exception] {
      ingest(key, cyclicHierarchy)
    }
    e shouldBe an[InvalidTreeException]
    val errorMessage = e.getMessage
    errorMessage should include(s"son$key")
    errorMessage should include(s"daughter$key")
    errorMessage should include(s"daughterSon$key")
    errorMessage should include(s"other$key")
  }

  it should "throw an error if one more externalIds are empty Strings" in {
    val key = shortRandomString()
    val e = intercept[Exception] {
      ingest(
        key,
        Seq(
          AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
          AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
          AssetCreate("daughter", None, None, None, Some(""), None, Some("dad")),
          AssetCreate(
            "daughterSon",
            externalId = Some("daughterSon"),
            parentExternalId = Some("daughter")),
          AssetCreate("othertree", None, None, None, Some(""), None, Some("otherDad"))
        )
      )
    }
    e shouldBe an[EmptyExternalIdException]
    val errorMessage = e.getMessage
    errorMessage should include(s"daughter")
    errorMessage should include(s"othertree")
    (errorMessage should not).include(s"daughterSon")
  }

  it should "fail reasonably when parent does not exist" in {
    val key = shortRandomString()
    val tree = Seq(
      AssetCreate(
        "testNode1",
        externalId = Some("testNode1"),
        parentExternalId = Some("nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds")),
      AssetCreate("testNode2", None, None, None, Some("testNode2"), None, Some(""))
    )

    val ex = intercept[Exception] { ingest(key, tree) }
    ex shouldBe an[InvalidNodeReferenceException]
    ex.getMessage shouldBe s"Parent 'nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds$key' referenced from 'testNode1$key' does not exist."
  }

  it should "fail when subtree is updated into being root" in {
    val key = shortRandomString()

    val assetTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, None, Some("daughterSon"), None, Some("daughter"))
    )

    ingest(key, assetTree)

    val ex = intercept[Exception] {
      ingest(
        key,
        Seq(
          AssetCreate("daughter", None, None, None, Some("daughter"), None, Some(""))
        ))
    }

    ex shouldBe an[CdfDoesNotSupportException]
    ex.getMessage should include("daughter")
    cleanDB(key)
  }

  // although these test basically only test what CDF does, we want
  // * to be sure that error message is not swallowed
  // * to be notified if this behavior changes, as then we should also support it
  //   or (at least) fail reasonably
  it should "fail when merging trees" in {
    val key = shortRandomString()

    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
        AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
        AssetCreate("dad2", None, None, None, Some("dad2"), None, Some("")),
        AssetCreate("son2", None, None, None, Some("son2"), None, Some("dad2"))
      )
    )

    val ex = intercept[CdpApiException] {
      ingest(
        key,
        Seq(
          AssetCreate("dad2-updated", None, None, None, Some("dad2"), None, Some("son"))
        ))
    }
    ex.getMessage should include("Changing from/to being root isn't allowed")
    cleanDB(key)
  }

  it should "fail when node moves between trees" in {
    val key = shortRandomString()

    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
        AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
        AssetCreate("dad2", None, None, None, Some("dad2"), None, Some(""))
      )
    )

    val ex = intercept[CdpApiException] {
      ingest(
        key,
        Seq(
          AssetCreate("son", None, None, None, Some("son"), None, Some("dad2"))
        ))
    }
    ex.getMessage should include("Asset must stay within same asset hierarchy")
    cleanDB(key)
  }

  it should "fail on duplicate externalId" in {
    val key = shortRandomString()

    val ex = intercept[NonUniqueAssetId] {
      ingest(
        key,
        Seq(
          AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
          AssetCreate("dad", None, None, None, Some("dad"), None, Some(""))
        )
      )
    }
    ex.id shouldBe s"dad$key"
    cleanDB(key)
  }

  it should "ingest an asset tree" in {
    val key = shortRandomString()

    val ds = Some(testDataSetId)
    val assetTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some(""), ds),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad"), ds),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad"), ds),
      AssetCreate(
        "daughterSon",
        externalId = Some("daughterSon"),
        parentExternalId = Some("daughter"),
        dataSetId = ds)
    )

    ingest(key, assetTree)

    val result = retryWhile[Array[Row]](
      spark
        .sql(s"select * from assets where source = '$testName$key' and dataSetId = $testDataSetId")
        .collect,
      rows => rows.map(r => r.getAs[String]("name")).toSet != assetTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"son$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughterSon$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))
    assert(extIdMap(Some(s"dad$key")).dataSetId == ds)
    cleanDB(key)
  }

  it should "ingest an asset tree, then update it" in {
    val key = shortRandomString()

    val ds = Some(testDataSetId)

    val originalTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad"), dataSetId = ds),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
      AssetCreate("sonDaughter", None, None, None, Some("sonDaughter"), None, Some("son")),
      AssetCreate("daughterSon", externalId = Some("daughterSon"), parentExternalId = Some("daughter")),
      AssetCreate(
        "secondDaughterToBeDeleted",
        externalId = Some("secondDaughter"),
        parentExternalId = Some("dad"))
    )

    ingest(key, originalTree, batchSize = 3)

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some(""), dataSetId = None),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad"), dataSetId = None),
      AssetCreate(
        "daughter",
        externalId = Some("daughter"),
        parentExternalId = Some("dad"),
        dataSetId = ds),
      AssetCreate("sonDaughter", externalId = Some("sonDaughter"), parentExternalId = Some("daughter")),
      AssetCreate("daughterSon", None, None, None, Some("daughterSon"), None, Some("daughter"))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(key, updatedTree, deleteMissingAssets = true)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getAs[String]("name")).toSet != updatedTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"sonDaughter$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))
    assert(extIdMap.get(Some(s"secondDaughterToBeDeleted$key")).isEmpty)
    assert(extIdMap(Some(s"daughter$key")).dataSetId == ds)
    assert(extIdMap(Some(s"son$key")).dataSetId == ds)
    assert(extIdMap(Some(s"dad$key")).dataSetId == None)

    cleanDB(key)
  }

  it should "ingest not update when nothing has changed" in {
    val key = shortRandomString()
    val metrics = "update.ignoreUnchanged"

    val tree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), Some(Map("Meta" -> "data")), Some("")),
      AssetCreate("son", None, Some("description"), Some("source"), Some("son"), None, Some("dad"), dataSetId = Some(testDataSetId)),
      AssetCreate("son2", None, None, None, Some("son2"), None, Some("dad"))
    )

    ingest(key, tree, metricsPrefix = Some(metrics))

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.length != 3)

    getNumberOfRowsCreated(metrics, "assethierarchy") shouldBe 3

    val updatedTree = tree.map({
      case x if x.name == "son2" => x.copy(parentExternalId = Some("son"))
      case x => x
    })

    ingest(key, updatedTree, metricsPrefix = Some(metrics))

    getNumberOfRowsCreated(metrics, "assethierarchy") shouldBe 3
    getNumberOfRowsUpdated(metrics, "assethierarchy") shouldBe 1

    cleanDB(key)
  }

  it should "move an asset to another asset that is being moved" in {
    val key = shortRandomString()

    val originalTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
      AssetCreate("sonChild", None, None, None, Some("sonChild"), None, Some("son"))
    )

    ingest(key, originalTree, batchSize = 5)

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.length != 4)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("son")),
      AssetCreate(
        "daughterChildUpdated",
        externalId = Some("sonChild"),
        parentExternalId = Some("daughter")),
      AssetCreate(
        "daughterChildTwo",
        externalId = Some("daughterChildTwo"),
        parentExternalId = Some("daughter"))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(key, updatedTree, batchSize = 1)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getAs[String]("name")).toSet != updatedTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"son$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"son$key")).id))
    assert(extIdMap(Some(s"sonChild$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))
    assert(extIdMap(Some(s"daughterChildTwo$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))

    cleanDB(key)
  }

  it should "avoid deleting assets when deleteMissingAssets is false" in {
    val key = shortRandomString()

    val originalTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad"))
    )

    ingest(key, originalTree, batchSize = 3)

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.length != 3)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("newSibling", None, None, None, Some("newSibling"), None, Some("dad"))
    )

    ingest(key, updatedTree, deleteMissingAssets = false)

    val allNames = updatedTree.map(_.name) ++ Seq("son", "daughter")

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getString(1)).toSet != allNames.toSet)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"son$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"newSibling$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))

    cleanDB(key)
  }

  it should "allow rearranging orders and depth of assets" in {
    val key = shortRandomString()

    val metricsPrefix = "update.assetsHierarchy.subtreeDepth"
    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
        AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
        AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
        AssetCreate(
          "daughterSon",
          externalId = Some("daughterSon"),
          parentExternalId = Some("daughter")),
        AssetCreate(
          "daughterDaughter",
          externalId = Some("daughterDaughter"),
          parentExternalId = Some("daughter")),
        AssetCreate("sonSon", None, None, None, Some("sonSon"), None, Some("son")),
        AssetCreate("sonDaughter", None, None, None, Some("sonDaughter"), None, Some("son"))
      )
    )

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.length != 7)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate(
        "daughter_ofDaughterSon",
        externalId = Some("daughter"),
        parentExternalId = Some("daughterSon")),
      AssetCreate(
        "daughterSon_ofDad",
        externalId = Some("daughterSon"),
        parentExternalId = Some("daughterDaughter")),
      AssetCreate(
        "daughterDaughter",
        externalId = Some("daughterDaughter"),
        parentExternalId = Some("dad")),
      AssetCreate("hen", None, None, None, Some("hen"), None, Some("dad"))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(
      key,
      updatedTree,
      deleteMissingAssets = true,
      batchSize = 3,
      metricsPrefix = Some(metricsPrefix))

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"daughterDaughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(
      extIdMap(Some(s"daughterSon$key")).parentId.contains(extIdMap(Some(s"daughterDaughter$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"daughterSon$key")).id))

    getNumberOfRowsUpdated(metricsPrefix, "assethierarchy") shouldBe 4
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 1
    getNumberOfRowsDeleted(metricsPrefix, "assethierarchy") shouldBe 3

    cleanDB(key)
  }

  it should "ingest an asset tree, then successfully delete a subtree" in {
    val key = shortRandomString()

    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
        AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
        AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("son")),
        AssetCreate("daughter2", None, None, None, Some("daughter2"), None, Some("daughter")),
        AssetCreate("daughter3", None, None, None, Some("daughter3"), None, Some("daughter2")),
        AssetCreate("daughter4", None, None, None, Some("daughter4"), None, Some("daughter3"))
      )
    )

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some(""))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(key, updatedTree, deleteMissingAssets = true)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet
    )
    assert(result.map(r => r.getString(1)).toSet == updatedTree.map(_.name).toSet)

    cleanDB(key)
  }

  it should "insert a tree while ignoring assets that are not connected when setting the ignoreDisconnectedAssets option to true" in {
    val key = shortRandomString()

    val assetTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", externalId = Some("daughterSon"), parentExternalId = Some("daughter")),
      AssetCreate(
        "unusedZero",
        externalId = Some("unusedZero"),
        parentExternalId = Some("nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds")),
      AssetCreate("unusedOne", None, None, None, Some("unusedOne"), None, Some("unusedZero")),
      AssetCreate("unusedTwo", None, None, None, Some("unusedTwo"), None, Some("unusedOne")),
      AssetCreate("unusedThree", externalId = Some("unusedThree"), parentExternalId = Some("unusedTwo"))
    )

    val metricsPrefix = "insert.assetsHierarchy.ignored"
    ingest(
      key,
      assetTree,
      subtrees = "ignore",
      metricsPrefix = Some(metricsPrefix))

    val namesOfAssetsToInsert = assetTree.slice(0, 4).map(_.name).toSet

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getAs[String]("name")).toSet != namesOfAssetsToInsert
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"son$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughterSon$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 4

    cleanDB(key)
  }

  it should "insert a tree and then add subtree" in {
    val key = shortRandomString()

    val metricsPrefix = "insert.assetsHierarchy.ingest.subtree"

    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
        AssetCreate("son", None, None, None, Some("son"), None, Some("dad"))
      ),
      metricsPrefix = Some(metricsPrefix)
    )

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getString(1)).toSet != Set("dad", "son")
    )

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 2

    ingest(
      key,
      Seq(
        AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
        AssetCreate(
          "daughterSon",
          externalId = Some("daughterSon"),
          parentExternalId = Some("daughter")),
        AssetCreate("sonSon", None, None, None, Some("sonSon"), None, Some("son"))
      ),
      metricsPrefix = Some(metricsPrefix)
    )

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows =>
        rows.map(r => r.getString(1)).toSet != Set("dad", "son", "daughter", "daughterSon", "sonSon")
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"sonSon$key")).parentId.contains(extIdMap(Some(s"son$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughterSon$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 5
  }

  it should "allow updating different subtrees" in {
    val key = shortRandomString()

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
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("son", None, None, None, Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, None, Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", externalId = Some("daughterSon"), parentExternalId = Some("daughter")),
      AssetCreate(
        "daughterDaughter",
        externalId = Some("daughterDaughter"),
        parentExternalId = Some("daughter")),
      AssetCreate("path0", None, None, None, Some("path0"), None, Some("son")),
      AssetCreate("path1", None, None, None, Some("path1"), None, Some("path0")),
      AssetCreate("path2", None, None, None, Some("path2"), None, Some("path1")),
      AssetCreate("path3", None, None, None, Some("path3"), None, Some("path2"))
    )
    ingest(key, sourceTree, metricsPrefix = Some(metricsPrefix))
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe sourceTree.length

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
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

    ingest(
      key,
      Seq(
        AssetCreate(
          "path0-updated",
          externalId = Some("path0"),
          parentExternalId = Some("daughterDaughter")),
        AssetCreate(
          "path2-updated",
          description = Some("desc"),
          externalId = Some("path2"),
          parentExternalId = Some("path1")),
        AssetCreate(
          "daughter-updated",
          description = Some("desc"),
          externalId = Some("daughter"),
          parentExternalId = Some("dad")),
        AssetCreate("dad-updated", None, Some("desc"), None, Some("dad"), None, Some(""))
      ),
      metricsPrefix = Some(metricsPrefix)
    )

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect,
      rows => rows.map(r => r.getString(1)).count(_.endsWith("-updated")) != 4
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"path0$key")).parentId.contains(extIdMap(Some(s"daughterDaughter$key")).id))
    assert(extIdMap(Some(s"daughter$key")).description.contains("desc"))
    assert(extIdMap(Some(s"dad$key")).description.contains("desc"))
    assert(extIdMap(Some(s"path2$key")).description.contains("desc"))

    getNumberOfRowsUpdated(metricsPrefix, "assethierarchy") shouldBe 4
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe sourceTree.length

    cleanDB(key)
  }

  def getAssetsMap(assets: Seq[Row]): Map[Option[String], AssetsReadSchema] =
    assets
      .map(r => fromRow[AssetsReadSchema](r))
      .map(a => a.externalId -> a)
      .toMap
}
