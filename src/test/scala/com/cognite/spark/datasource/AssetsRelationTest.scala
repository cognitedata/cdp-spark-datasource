package com.cognite.spark.datasource

import java.util.regex.Pattern

import com.softwaremill.sttp._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import cats.implicits._

class AssetsRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = System.getenv("TEST_API_KEY_READ")
  val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  it should "read assets" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limit", "1000")
      .option("partitions", "1")
      .load()

    df.createTempView("assets")
    val res = spark.sql("select * from assets")
      .collect()
    assert(res.length == 1000)
  }

  it should "read assets with a small batchSize" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("batchSize", "1")
      .option("limit", "10")
      .option("partitions", "1")
      .load()

    assert(df.count() == 10)
  }

  it should "be possible to create assets" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-create"
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .load()
    df.createOrReplaceTempView("assets")
    cleanupAssets(assetsTestSource)
    retryWhile[DataFrame](
      spark.sql(s"select * from assets where source = '$assetsTestSource'"),
      rows => rows.count > 0)
    spark.sql(
      s"""
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
    retryWhile[DataFrame](
      spark.sql(s"select * from assets where source = '$assetsTestSource'"),
      rows => rows.count < 1)
  }

  it should "be possible to copy assets from one tenant to another" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-copy"
    val sourceDf = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
    sourceDf.createOrReplaceTempView("source_assets")
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .load()
    df.createOrReplaceTempView("assets")
    cleanupAssets(assetsTestSource)
    retryWhile[DataFrame](
      spark.sql(s"select * from assets where source = '$assetsTestSource'"),
      rows => rows.count > 0)
    spark.sql(
      s"""
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
         |lastUpdatedTime
         |from source_assets where id = 2675073401706610
      """.stripMargin)
      .write
      .insertInto("assets")
    retryWhile[DataFrame](
      spark.sql(s"select * from assets where source = '$assetsTestSource'"),
      rows => rows.count < 1)
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

    val assetIdsChunks = assets.flatMap(_.id).grouped(1000)
    val AssetIdNotFound = "^Asset ids not found.+".r
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
