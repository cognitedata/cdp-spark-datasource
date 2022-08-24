package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{AssetCreate, CogniteExternalId}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers, OptionValues, ParallelTestExecution}

class AssetHierarchyBuilderTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with ParallelTestExecution
    with SparkTest {
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
      batchSize: Long = 100L,
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
      .save()
  }

  private def cleanDB(key: String) =
    writeClient.assets.deleteRecursive(
      Seq(s"dad$key", s"dad2$key", s"unusedZero$key", s"son$key").map(CogniteExternalId(_)),
      true,
      true)

  it should "throw an error when everything is ignored" in {
    val key = shortRandomString()
    val tree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("someNode"))
    )
    val e = sparkIntercept {
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
    val e = sparkIntercept {
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
    val e = sparkIntercept {
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
    val e = sparkIntercept {
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

    val ex = sparkIntercept { ingest(key, tree) }
    ex shouldBe an[InvalidNodeReferenceException]
    ex.getMessage shouldBe s"Parent 'nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds$key' referenced from 'testNode1$key' does not exist."
  }

  it should "fail reasonably when some subtree parents don't exist" in {
    val key = shortRandomString()

    ingest(key, Seq(
      AssetCreate(
        "testRootNode1",
        externalId = Some("testRootNode1"),
        parentExternalId = Some("")),
      AssetCreate(
        "testRootNode2",
        externalId = Some("testRootNode2"),
        parentExternalId = Some(""))
    ))

    val tree = Seq(
      AssetCreate(
        "testNode1",
        externalId = Some("testNode1"),
        parentExternalId = Some("testRootNode1")),
      AssetCreate(
        "testNode2",
        externalId = Some("testNode2"),
        parentExternalId = Some("nonExistentNode-aakdhdslfskgslfuwfvbnvwbqrvotfeds")),
      AssetCreate(
        "testNode3",
        externalId = Some("testNode3"),
        parentExternalId = Some("testRootNode1")),
      AssetCreate(
        "testNode4",
        externalId = Some("testNode4"),
        parentExternalId = Some("testRootNode2")),
      AssetCreate(
        "testNode5",
        externalId = Some("testNode5"),
        parentExternalId = Some("testNode4")),
      AssetCreate(
        "testNode6",
        externalId = Some("testNode6"),
        parentExternalId = Some("nonExistentNode-aakdhdslfskgslfuwfvbnvwbqrvotfeds")),
      AssetCreate(
        "testNode7",
        externalId = Some("testNode7"),
        parentExternalId = Some("nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds2"))
    )

    val ex = sparkIntercept { ingest(key, tree) }
    ex shouldBe an[InvalidNodeReferenceException]
    ex.getMessage shouldBe s"Parents 'nonExistentNode-aakdhdslfskgslfuwfvbnvwbqrvotfeds$key', 'nonExistentNode-jakdhdslfskgslfuwfvbnvwbqrvotfeds2$key' referenced from 'testNode2$key', 'testNode7$key' do not exist."
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


    val ex = sparkIntercept {
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

    val ex = sparkIntercept {
      ingest(
        key,
        Seq(
          AssetCreate("dad2-updated", None, None, None, Some("dad2"), None, Some("son"))
        ))
    }
    ex shouldBe an[CdpApiException]
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

    val ex = sparkIntercept {
      ingest(
        key,
        Seq(
          AssetCreate("son", None, None, None, Some("son"), None, Some("dad2"))
        ))
    }
    ex shouldBe an[CdpApiException]
    ex.getMessage should include("Asset must stay within same asset hierarchy")
    cleanDB(key)
  }

  it should "fail on duplicate externalId" in {
    val key = shortRandomString()

    val ex = sparkIntercept {
      ingest(
        key,
        Seq(
          AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
          AssetCreate("dad", None, None, None, Some("dad"), None, Some(""))
        )
      )
    }
    ex shouldBe NonUniqueAssetId(s"dad$key")
    cleanDB(key)
  }

  it should "fail reasonably on invalid source type" in {
    val exception = sparkIntercept {
      spark.sql(
        """
          |select "test-asset-rV2yGok98VNzWMb9yWGk" as externalId,
          |       "" as parentExternalId,
          |       1 as source,
          |       "my-test-asset" as name
          |""".stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assethierarchy")
        .save()
    }

    exception.getMessage shouldBe "Column 'source' was expected to have type String, but '1' of type Int was found (on row with externalId='test-asset-rV2yGok98VNzWMb9yWGk')."

  }

  it should "fail reasonably on NULLs" in {
    val exception = sparkIntercept {
      spark.sql(
        """
          |select "test-asset-rV2yGok98VNzWMb9yWGk" as externalId,
          |       "" as parentExternalId,
          |       1 as source,
          |       "my-test-asset" as name
          |""".stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assethierarchy")
        .save()
    }

    exception.getMessage shouldBe "Column 'source' was expected to have type String, but '1' of type Int was found (on row with externalId='test-asset-rV2yGok98VNzWMb9yWGk')."

  }

  it should "fail reasonably on invalid dataSetId type" in {
    val exception = sparkIntercept {
      spark.sql(
        """
          |select "test-asset-MNwWje501UZ83dFA3S" as externalId,
          |       "" as parentExternalId,
          |       "" as dataSetId,
          |       "my-test-asset" as name
          |""".stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assethierarchy")
        .save()
    }

    exception shouldBe an[CdfSparkIllegalArgumentException]
    exception.getMessage shouldBe "Column 'dataSetId' was expected to have type Long, but '' of type String was found (on row with externalId='test-asset-MNwWje501UZ83dFA3S')."
  }

  it should "fail with hint on parentExternalId=NULL" in {
    val exception = sparkIntercept {
      spark.sql(
        """
          |select "test-asset-55UbfFlTh2I95usWl7gnok" as externalId,
          |       NULL as parentExternalId,
          |       "my-test-asset" as name
          |""".stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assethierarchy")
        .save()
    }

    exception shouldBe an[CdfSparkIllegalArgumentException]
    exception.getMessage shouldBe "Column 'parentExternalId' was expected to have type String, but NULL was found (on row with externalId='test-asset-55UbfFlTh2I95usWl7gnok'). To mark the node as root, please use an empty string ('')."
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
        .collect(),
      rows => rows.map(r => r.getAs[String]("name")).toSet != assetTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"son$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughterSon$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))
    assert(extIdMap(Some(s"dad$key")).dataSetId == ds)
    cleanDB(key)
  }


  it should "ignore on nulls in metadata" in {
    val key = shortRandomString()

    val nullString: String = null
    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), Some(Map("foo" -> nullString, "bar" -> "a")), Some(""))
      )
    )

    val result = retryWhile[Array[Row]](
      spark
        .sql(s"select * from assets where source = '$testName$key'")
        .collect(),
      rows => rows.map(r => r.getAs[String]("externalId")).toSet != Set(s"dad$key")
    )

    val ingestedNode = fromRow[AssetsReadSchema](result.head)
    ingestedNode.metadata shouldBe Some(Map("bar" -> "a"))

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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
      rows => rows.length != 3)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
      AssetCreate("newSibling", None, None, None, Some("newSibling"), None, Some("dad"))
    )

    ingest(key, updatedTree, deleteMissingAssets = false)

    val allNames = updatedTree.map(_.name) ++ Seq("son", "daughter")

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, None, Some("dad"), None, Some(""))
    ).map(a => a.copy(name = a.name + "Updated"))

    ingest(key, updatedTree, deleteMissingAssets = true)

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
      rows =>
        rows.map(r => r.getString(1)).toSet != Set("dad", "son", "daughter", "daughterSon", "sonSon")
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"sonSon$key")).parentId.contains(extIdMap(Some(s"son$key")).id))
    assert(extIdMap(Some(s"daughter$key")).parentId.contains(extIdMap(Some(s"dad$key")).id))
    assert(extIdMap(Some(s"daughterSon$key")).parentId.contains(extIdMap(Some(s"daughter$key")).id))

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe 5
  }

  it should "delete including subtrees" in {
    val key = shortRandomString()

    ingest(
      key,
      Seq(
        AssetCreate("dad", None, None, None, Some("dad"), None, Some("")),
        AssetCreate("son", None, None, None, Some("son"), None, Some("dad"))
      )
    )

    retryWhile[Array[Row]](
      spark.sql(s"select name from assets where source = '$testName$key'").collect(),
      rows => rows.map(r => r.getString(0)).toSet != Set("dad", "son")
    )

    spark.sql(s"select id from assets where externalId = 'dad$key'")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assethierarchy")
      .option("onconflict", "delete")
      .option("collectMetrics", "true")
      .option("metricsPrefix", "assethierarchy-deletetest")
      .save()

    getNumberOfRowsDeleted("assethierarchy-deletetest", "assethierarchy") shouldBe 1 // counts the number of deleted hierarchies

    retryWhile[Array[Row]](
      spark.sql(s"select id from assets where source = '$testName$key'").collect(),
      rows => rows.length != 0
    )
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
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
    //                                     /   |
    //                              path2b   path2*
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
          "path2b-updated",
          description = Some("desc"),
          externalId = Some("path2b"),
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
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
      rows => rows.map(r => r.getString(1)).count(_.endsWith("-updated")) != 5
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some(s"path0$key")).parentId.contains(extIdMap(Some(s"daughterDaughter$key")).id))
    assert(extIdMap(Some(s"daughter$key")).description.contains("desc"))
    assert(extIdMap(Some(s"dad$key")).description.contains("desc"))
    assert(extIdMap(Some(s"path2$key")).description.contains("desc"))
    assert(extIdMap(Some(s"path2b$key")).description.contains("desc"))

    getNumberOfRowsUpdated(metricsPrefix, "assethierarchy") shouldBe 4
    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe (sourceTree.length + 1)

    cleanDB(key)
  }

  it should "allow moving an asset to become a child of its former grandparent" in {
    val key = shortRandomString()
    val initialStateMetricsPrefix = "insert.assetHierarchy.moveToGrandma.initialState"
    val afterMoveMetricsPrefix = "insert.assetHierarchy.moveToGrandma.afterMove"

    //    grandma
    //       |
    //      dad
    //       |
    //     child

    val sourceTree = Seq(
      AssetCreate("grandma", externalId = Some("grandma"), parentExternalId = Some("")),
      AssetCreate("dad", externalId = Some("dad"), parentExternalId = Some("grandma")),
      AssetCreate("child", externalId = Some("child"), parentExternalId = Some("dad"))
    )

    ingest(key, sourceTree, metricsPrefix = Some(initialStateMetricsPrefix))

    getNumberOfRowsCreated(initialStateMetricsPrefix, "assethierarchy") shouldBe sourceTree.length

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
      rows => rows.length != sourceTree.length
    )

    //      grandma
    //     /      \
    //   dad    child

    ingest(
      key,
      Seq(AssetCreate("child-updated", externalId = Some("child"), parentExternalId = Some("grandma"))),
      metricsPrefix = Some(afterMoveMetricsPrefix)
    )

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key' and name = 'child-updated'").collect(),
      rows => rows.length < 1
    )

    val row = result.head
    row.getAs[String]("parentExternalId") shouldBe s"grandma$key"

    getNumberOfRowsUpdated(afterMoveMetricsPrefix, "assethierarchy") shouldBe 1
    intercept[Exception](getNumberOfRowsCreated(afterMoveMetricsPrefix, "assethierarchy"))

    cleanDB(key)
  }

  it should "allow moving an asset to become a child of one of its former siblings" in {
    val key = shortRandomString()
    val initialStateMetricsPrefix = "insert.assetHierarchy.moveToSibling.initialState"
    val afterMoveMetricsPrefix = "insert.assetHierarchy.moveToSibling.afterMove"

    //          root
    //         /    \
    //     child1  child2

    val sourceTree = Seq(
      AssetCreate("root", externalId = Some("root"), parentExternalId = Some("")),
      AssetCreate("child1", externalId = Some("child1"), parentExternalId = Some("root")),
      AssetCreate("child2", externalId = Some("child2"), parentExternalId = Some("root"))
    )

    ingest(key, sourceTree, metricsPrefix = Some(initialStateMetricsPrefix))

    getNumberOfRowsCreated(initialStateMetricsPrefix, "assethierarchy") shouldBe sourceTree.length

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName$key'").collect(),
      rows => rows.length != sourceTree.length
    )

    //      root
    //       |
    //     child1
    //       |
    //     child2

    ingest(
      key,
      Seq(AssetCreate("child2-updated", externalId = Some("child2"), parentExternalId = Some("child1"))),
      metricsPrefix = Some(afterMoveMetricsPrefix)
    )

    val result = retryWhile[Array[Row]](
      spark
        .sql(s"select * from assets where source = '$testName$key' and name = 'child2-updated'")
        .collect(),
      rows => rows.length < 1
    )

    val row = result.head
    row.getAs[String]("parentExternalId") shouldBe s"child1$key"

    getNumberOfRowsUpdated(afterMoveMetricsPrefix, "assethierarchy") shouldBe 1
    intercept[Exception](getNumberOfRowsCreated(afterMoveMetricsPrefix, "assethierarchy"))

    cleanDB(key)
  }

  it should "throw a proper error when attempting to move an asset to a different root" in {
    val key = shortRandomString()
    val metricsPrefix = "insert.assetHierarchy.errorOnDifferentRoot"

    //     root1       root2
    //       |           |
    //    subtree1    subtree2
    //       |
    //     child

    val sourceTree = Seq(
      AssetCreate("root1", externalId = Some("root1"), parentExternalId = Some("")),
      AssetCreate("subtree1", externalId = Some("subtree1"), parentExternalId = Some("root1")),

      AssetCreate("root2", externalId = Some("root2"), parentExternalId = Some("")),
      AssetCreate("subtree2", externalId = Some("subtree2"), parentExternalId = Some("root2")),

      AssetCreate("child", externalId = Some("child"), parentExternalId = Some("subtree1"))
    )

    ingest(key, sourceTree, metricsPrefix = Some(metricsPrefix))

    getNumberOfRowsCreated(metricsPrefix, "assethierarchy") shouldBe sourceTree.length

    val assetRows = retryWhile[DataFrame](
      spark.sql(s"select * from assets where source = '$testName$key'"),
      rows => rows.count() != sourceTree.length
    )

    val apiException = sparkIntercept {
      // Attempting to move the subtree root to a different root asset will result in a proper error from CDF.
      // The "child" asset is the subtree root in this case.
      ingest(key, Seq(
        AssetCreate("child", externalId = Some("child"), parentExternalId = Some("subtree2"))
      ))
    }
    apiException shouldBe an[CdpApiException]

    apiException.asInstanceOf[CdpApiException].message should startWith("Asset must stay within same asset hierarchy root")

    val rootChangeException = sparkIntercept {
      // However, when attempting to move any of the subtree's children to a different root, we fail to find the asset,
      // and assume it doesn't exist. So we attempt to create it, which results in a duplicated error, which we catch
      // and convert to a more helpful error message.
      ingest(key, Seq(
        AssetCreate("subtree2", externalId = Some("subtree2"), parentExternalId = Some("root2")),
        AssetCreate("child", externalId = Some("child"), parentExternalId = Some("subtree2"))
      ))
    }

    val newRootAssetId = assetRows.where(s"externalId = 'root2$key'").head().getAs[Long]("id")

    rootChangeException shouldBe an[InvalidRootChangeException]
    rootChangeException.getMessage shouldBe (
      s"Attempted to move some assets to a different root asset in subtree subtree2$key under the rootId=$newRootAssetId. " +
        "If this is intended, the assets must be manually deleted and re-created under the new root asset. " +
        s"Assets with the following externalIds were attempted to be moved: child$key."
    )

    cleanDB(key)
  }

  it should "successfully do batching of subtree roots" in {
    val key = shortRandomString()

    val dad = AssetCreate("dad", None, None, None, Some("dad"), None, Some(""))

    val kids = (1 to 1100).flatMap(k => Seq(
      AssetCreate(s"kid$k", None, None, None, Some(s"kid$k"), None, Some("dad"))))
    val grandkids = (1 to 1100).map(k =>
      AssetCreate(s"grandkid$k", None, None, None, Some(s"grandkid$k"), None, Some(s"kid$k"))
    )

    // Ingest the first two levels of the tree
    ingest(key, kids :+ dad, batchSize = 1000)

    // Ingest the third level, 1100 subtrees
    ingest(key, grandkids, batchSize = 1000, metricsPrefix = Some(s"ingest.tree.grandkids.$key"))

    getNumberOfRowsCreated(s"ingest.tree.grandkids.$key", "assethierarchy") shouldBe 1100
    // 2 for fetching root parents (for validation)
    // 2 for fetching the roots
    // 2 for creating the roots
    getNumberOfRequests(s"ingest.tree.grandkids.$key") shouldBe 6

    val grandkidsUpdate = (1 to 1100).map(k =>
      AssetCreate(s"grandkid$k", None, Some("some description"), None, Some(s"grandkid$k"), None, Some(s"kid$k"))
    )
    // Update the third level, 1100 subtrees
    ingest(key, grandkidsUpdate, batchSize = 1000, metricsPrefix = Some(s"ingest.tree.grandkidsU.$key"))

    getNumberOfRowsUpdated(s"ingest.tree.grandkidsU.$key", "assethierarchy") shouldBe 1100
    // 2 for fetching root parents (for validation)
    // 2 for fetching the roots
    // 2 for updating the roots
    getNumberOfRequests(s"ingest.tree.grandkidsU.$key") shouldBe 6

    cleanDB(key)
  }

  // this runs for a long time, so we don't want to run it every time
  ignore should "successfully ingest million items" in {
    val key = shortRandomString()

    try {
      val dad = AssetCreate("dad", None, None, None, Some("dad"), None, Some(""))

      val kids = (0 to 200 * 1000).flatMap(k => Seq(
        AssetCreate(s"kid${k}_", None, None, None, Some(s"kid${k}_"), None, Some("dad"))))
      val grandkids = (0 to 1000 * 1000).map(k =>
        AssetCreate(s"grandkid${k}_", None, None, None, Some(s"grandkid${k}_"), None, Some(s"kid${k / 5}_"))
      )

      // Ingest the first two levels of the tree
      ingest(key, kids ++ grandkids :+ dad, batchSize = 1000)

      // Ingest the third level, 1100 subtrees
      ingest(key, grandkids, batchSize = 1000, metricsPrefix = Some(s"ingest.bigtree.$key"))

      getNumberOfRowsCreated(s"ingest.bigtree.$key", "assethierarchy") shouldBe 1 + 200 * 1000 + 1000 * 1000
    }
    finally {
      cleanDB(key)
    }
  }

  def getAssetsMap(assets: Array[Row]): Map[Option[String], AssetsReadSchema] =
    assets
      .map(r => fromRow[AssetsReadSchema](r))
      .map(a => a.externalId -> a)
      .toMap
}
