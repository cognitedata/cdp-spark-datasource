package com.cognite.spark.datasource

import com.softwaremill.sttp._
import org.apache.spark.sql.{Row}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions.col

class AssetsRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_READ"))
  val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  it should "read assets" taggedAs ReadTest in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .option("limit", "1000")
      .option("partitions", "1")
      .load()

    df.createTempView("assets")
    val res = spark
      .sql("select * from assets")
      .collect()
    assert(res.length == 1000)
  }

  it should "read assets with a small batchSize" taggedAs ReadTest in {
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .option("batchSize", "1")
      .option("limit", "10")
      .option("partitions", "1")
      .load()

    assert(df.count() == 10)
  }

  it should "be possible to create assets" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-create"
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
    df.createOrReplaceTempView("assets")
    cleanupAssets(assetsTestSource)
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length > 0)
    spark
      .sql(s"""
        |select null as id,
        |null as path,
        |null as depth,
        |'asset name' as name,
        |null as parentId,
        |'asset description' as description,
        |null as metadata,
        |'$assetsTestSource' as source,
        |null as sourceId,
        |null as createdTime,
        |null as lastUpdatedTime
      """.stripMargin)
      .write
      .insertInto("assets")
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)
  }

  it should "be possible to copy assets from one tenant to another" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-copy"
    val sourceDf = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .load()
    sourceDf.createOrReplaceTempView("source_assets")
    val df = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
    df.createOrReplaceTempView("assets")
    cleanupAssets(assetsTestSource)
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length > 0)
    spark
      .sql(s"""
         |select id,
         |path,
         |depth,
         |name,
         |null as parentId,
         |description,
         |metadata,
         |'$assetsTestSource' as source,
         |id as sourceId,
         |createdTime,
         |null as lastUpdatedTime
         |from source_assets where id = 2675073401706610
      """.stripMargin)
      .write
      .insertInto("assets")
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)
  }

  it should "allow partial updates" taggedAs WriteTest in {
    val sourceDf = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
      .where("name = 'upsertTestThree'")
    sourceDf.createTempView("update")
    val wdf = spark.sql("""
        |select 'upsertTestThree' as name, id from update
      """.stripMargin)

    wdf.write
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save()
  }

  it should "support some more partial updates" taggedAs WriteTest in {
    val source = "spark-assets-test"

    val sourceDf = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .load()
    sourceDf.createTempView("sourceAssets")

    val destinationDf = spark.read
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
    destinationDf.createTempView("destinationAssets")

    // Cleanup assets
    cleanupAssets(source)
    val oldAssetsFromTestDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where source = '$source'").collect,
      df => df.length > 0)
    assert(oldAssetsFromTestDf.length == 0)

    // Post new events
    spark
      .sql(s"""
                 |select id,
                 |path,
                 |depth,
                 |name,
                 |null as parentId,
                 |description,
                 |metadata,
                 |"$source" as source,
                 |sourceId,
                 |createdTime,
                 |lastUpdatedTime
                 |from sourceAssets
                 |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationAssets")

    // Check if post worked
    val assetsFromTestDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where source = '$source'").collect,
      df => df.length < 100)
    assert(assetsFromTestDf.length == 100)

    val description = "spark-testing-description"

    // Update assets
    spark
      .sql(s"""
                 |select '$description' as description,
                 |id from destinationAssets
                 |where source = '$source'
     """.stripMargin)
      .write
      .format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save()

    // Check if update worked
    val assetsWithNewNameDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where description = '$description'").collect,
      df => df.length < 100)
    assert(assetsFromTestDf.length == 100)
  }

  def cleanupAssets(source: String): Unit = {
    import io.circe.generic.auto._

    val project = getProject(writeApiKey, Constants.DefaultMaxRetries, Constants.DefaultBaseUrl)

    val assets = get[AssetsItem](
      writeApiKey,
      uri"https://api.cognitedata.com/api/0.6/projects/$project/assets?source=$source",
      batchSize = 1000,
      limit = None,
      maxRetries = 10)

    val assetIdsChunks = assets.map(_.id).grouped(1000)
    val AssetIdNotFound = "^(Asset ids not found).+".r
    for (assetIds <- assetIdsChunks) {
      try {
        post(
          writeApiKey,
          uri"https://api.cognitedata.com/api/0.6/projects/$project/assets/delete",
          assetIds,
          10
        ).unsafeRunSync()
      } catch {
        case CdpApiException(_, 400, AssetIdNotFound(_)) => // ignore exceptions about already deleted items
      }
    }
  }
}
