package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import com.cognite.sdk.scala.v1.AssetCreate
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class AssetsHierarchyBuilderTest extends FlatSpec with Matchers with SparkTest {
  import spark.implicits._

  private val assetsSourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "assets")
    .load()
  assetsSourceDf.createOrReplaceTempView("assets")

  val testName = "assetshierarchyTest"

  it should "throw an error on empty input" in {
    val e = intercept[Exception] {
      spark.sparkContext.parallelize(Seq[AssetCreate]()).toDF().write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assetshierarchy")
        .save
    }
    e shouldBe an[NoRootException]
  }

  it should "throw an error if some nodes are disconnected from the root" in {
    val e = intercept[Exception] {
      spark.sparkContext.parallelize(Seq(
        AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
        AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
        AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad")),
        AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
        AssetCreate("othertree", None, None, Some(testName),Some("other"), None, Some("otherDad"))
      )).toDF().write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assetshierarchy")
        .save
    }
    e shouldBe an[InvalidTreeException]
  }

  it should "throw an error if there are multiple roots" in {
    val e = intercept[Exception] {
      spark.sparkContext.parallelize(Seq(
        AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
        AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("")),
        AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("")),
        AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
        AssetCreate("othertree", None, None, Some(testName),Some("other"), None, Some("otherDad"))
      )).toDF().write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assetshierarchy")
        .save
    }
    e shouldBe an[MultipleRootsException]
  }

  it should "throw an error if there is a cycle" in {
    val e = intercept[Exception] {
      spark.sparkContext.parallelize(Seq(
        AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
        AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("daughter")),
        AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("daughterSon")),
        AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("other")),
        AssetCreate("othertree", None, None, Some(testName),Some("other"), None, Some("son"))
      )).toDF().write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assetshierarchy")
        .save
    }
    e shouldBe an[InvalidTreeException]
  }

  it should "throw an error if one more externalIds are empty Strings" in {
    val e = intercept[Exception] {
      spark.sparkContext.parallelize(Seq(
        AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
        AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
        AssetCreate("daughter", None, None, Some(testName),Some(""), None, Some("dad")),
        AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
        AssetCreate("othertree", None, None, Some(testName),Some(""), None, Some("otherDad"))
      )).toDF().write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assetshierarchy")
        .save
    }
    e shouldBe an[EmptyExternalIdException]
    val errorMessage = e.getMessage
    errorMessage should include("daughter")
    errorMessage should include("othertree")
    errorMessage should not include("daughterSon")
  }

  it should "ingest an asset tree" in {
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)

    spark.sparkContext.parallelize(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName),Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter"))
    )).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("batchSize", "2")
      .save

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 4)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("son")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughterSon")).parentId.contains(extIdMap(Some("daughter")).id))
  }

  it should "ingest an asset tree, then update it" in {
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)

    spark.sparkContext.parallelize(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("sonDaughter", None, None, Some(testName),Some("sonDaughter"), None, Some("son")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter")),
      AssetCreate("secondDaughterToBeDeleted", None, None, Some(testName),Some("secondDaughter"), None, Some("dad"))
    )).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("batchSize", "3")
      .save

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("sonDaughter", None, None, Some(testName), Some("sonDaughter"), None, Some("daughter")),
      AssetCreate("daughterSon", None, None, Some(testName),Some("daughterSon"), None, Some("daughter"))
    ).map(a => a.copy(name = a.name + "Updated"))

    spark.sparkContext.parallelize(updatedTree).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("deleteMissingAssets", "true")
      .option("batchSize", "2")
      .save

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet
    )

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("sonDaughter")).parentId.contains(extIdMap(Some("daughter")).id))
    assert(extIdMap.get(Some("secondDaughterToBeDeleted")).isEmpty)
  }

  it should "move an asset to another asset that is being moved" in {
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)

    spark.sparkContext.parallelize(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("sonChild", None, None, Some(testName),Some("sonChild"), None, Some("son"))
    )).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("batchSize", "5")
      .save

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

    spark.sparkContext.parallelize(updatedTree).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("", "true")
      .option("batchSize", "1")
      .save

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
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)

    spark.sparkContext.parallelize(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad"))
    )).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("batchSize", "3")
      .save

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 3)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("newSibling", None, None, Some(testName),Some("newSibling"), None, Some("dad"))
    )

      spark.sparkContext.parallelize(updatedTree).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("deleteMissingAssets", "false")
      .option("batchSize", "2")
      .save

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
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)
    writeClient.assets.deleteByExternalIds(Seq("theGrandFather"), true, true)

    spark.sparkContext.parallelize(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("dad")),
      AssetCreate("daughterSon", None, None, Some(testName), Some("daughterSon"), None, Some("daughter")),
      AssetCreate("daughterDaughter", None, None, Some(testName), Some("daughterDaughter"), None, Some("daughter")),
      AssetCreate("sonSon", None, None, Some(testName), Some("sonSon"), None, Some("son")),
      AssetCreate("sonDaughter", None, None, Some(testName), Some("sonDaughter"), None, Some("son"))
    )).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("batchSize", "2")
      .save

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

    spark.sparkContext.parallelize(updatedTree).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("deleteMissingAssets", "true")
      .option("batchSize", "3")
      .save

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet)

    val extIdMap = getAssetsMap(result)
    assert(extIdMap(Some("daughterDaughter")).parentId.contains(extIdMap(Some("dad")).id))
    assert(extIdMap(Some("daughterSon")).parentId.contains(extIdMap(Some("daughterDaughter")).id))
    assert(extIdMap(Some("daughter")).parentId.contains(extIdMap(Some("daughterSon")).id))
  }

  it should "ingest an asset tree, then successfully delete a subtree" in {
    writeClient.assets.deleteByExternalIds(Seq("dad"), true, true)

    spark.sparkContext.parallelize(Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some("")),
      AssetCreate("son", None, None, Some(testName),Some("son"), None, Some("dad")),
      AssetCreate("daughter", None, None, Some(testName), Some("daughter"), None, Some("son")),
      AssetCreate("daughter2", None, None, Some(testName), Some("daughter2"), None, Some("daughter")),
      AssetCreate("daughter3", None, None, Some(testName), Some("daughter3"), None, Some("daughter2")),
      AssetCreate("daughter4", None, None, Some(testName), Some("daughter4"), None, Some("daughter3"))
    )).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("batchSize", "3")
      .save

    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.length != 6)
    Thread.sleep(2000)

    val updatedTree = Seq(
      AssetCreate("dad", None, None, Some(testName),Some("dad"), None, Some(""))
    ).map(a => a.copy(name = a.name + "Updated"))

    spark.sparkContext.parallelize(updatedTree).toDF().write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assetshierarchy")
      .option("deleteMissingAssets", "true")
      .option("batchSize", "1")
      .save

    val result = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$testName'").collect,
      rows => rows.map(r => r.getString(1)).toSet != updatedTree.map(_.name).toSet
    )
    assert(result.map(r => r.getString(1)).toSet == updatedTree.map(_.name).toSet)
  }

  def getAssetsMap(assets: Seq[Row]): Map[Option[String], AssetsReadSchema] =
    assets.map(r => fromRow[AssetsReadSchema](r))
      .map(a => a.externalId -> a).toMap
}
