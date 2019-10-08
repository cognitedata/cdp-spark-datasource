package cognite.spark

import com.cognite.sdk.scala.common.ApiKeyAuth
import com.softwaremill.sttp._
import org.apache.spark.sql.Row
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}
import io.circe.generic.auto._

class AssetsRelationTest extends FlatSpec with Matchers with SparkTest {
  val readApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_READ"))
  val writeApiKey = ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE"))

  val sourceDf = spark.read
    .format("cognite.spark")
    .option("apiKey", readApiKey.apiKey)
    .option("type", "assets")
    .load()
  sourceDf.createTempView("sourceAssets")

  val destinationDf = spark.read
    .format("cognite.spark")
    .option("apiKey", writeApiKey.apiKey)
    .option("type", "assets")
    .load()
  destinationDf.createTempView("destinationAssets")

  it should "read assets" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark")
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
      .format("cognite.spark")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .option("batchSize", "1")
      .option("limit", "10")
      .option("partitions", "1")
      .load()

    assert(df.count() == 10)
  }

  it should "support pushdown filters on name" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assets.name"
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limit", "1000")
      .option("partitions", "1")
      .load()
      .where("name = '23-TT-92604B'")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "support pushdown filters on source" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assets.source"
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limit", "1000")
      .option("partitions", "1")
      .load()
      .where("source = 'some source'")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assets.duplicates"
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limit", "1000")
      .option("partitions", "5")
      .load()
      .where("source = 'some source' or name = '99-BB-99999'")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "be possible to create assets" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-create"
    val df = spark.read
      .format("cognite.spark")
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
        |select 1 as externalId,
        |'asset name' as name,
        |null as parentId,
        |'asset description' as description,
        |null as metadata,
        |'$assetsTestSource' as source,
        |10 as id,
        |0 as createdTime,
        |0 as lastUpdatedTime
      """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("assets")
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)
  }

  it should "handle null values in metadata when inserting in savemode" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-create"
    val df = spark.read
      .format("cognite.spark")
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
              |select "1" as externalId,
              |'asset name' as name,
              |null as parentId,
              |'asset description' as description,
              map("foo", null, "bar", "test") as metadata,
              |'$assetsTestSource' as source,
              |bigint(10) as id
      """.stripMargin)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "abort")
      .save
  }

  it should "be possible to copy assets from one tenant to another" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-copy"
    val sourceDf = spark.read
      .format("cognite.spark")
      .option("apiKey", readApiKey.apiKey)
      .option("type", "assets")
      .load()
    sourceDf.createOrReplaceTempView("source_assets")
    val df = spark.read
      .format("cognite.spark")
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
         |select externalId,
         |name,
         |null parentId,
         |description,
         |metadata,
         |'$assetsTestSource' as source,
         |id,
         |createdTime,
         |lastUpdatedTime
         |from source_assets where id = 2675073401706610
      """.stripMargin)
      .write
      .insertInto("assets")
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)
  }

  it should "support upserts when using insertInto()" taggedAs WriteTest in {
    val source = "spark-assets-upsert-testing"

    // Cleanup old assets
    cleanupAssets(source)
    retryWhile[Array[Row]](
      spark.sql(s"select * from sourceAssets where source = '$source'").collect,
      rows => rows.length > 0
    )

    // Post new assets
    spark
      .sql(s"""
              |select id as externalId,
              |name,
              |null as parentId,
              |'foo' as description,
              |map("bar", "test") as metadata,
              |'$source' as source,
              |id,
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
      spark.sql(s"select * from destinationAssets where source = '$source' and description = 'foo'").collect,
      df => df.length != 100)
    assert(assetsFromTestDf.length == 100)

    // Upsert assets
    spark
      .sql(s"""
              |select externalId,
              |name,
              |null parentId,
              |'bar' as description,
              |map("foo", null, "bar", "test") as metadata,
              |'$source' as source,
              |id,
              |createdTime,
              |lastUpdatedTime
              |from destinationAssets
              |where source = '$source'""".stripMargin)
      .union(spark
        .sql(s"""
              |select externalId,
              |name,
              |null parentId,
              |'bar' as description,
              |metadata,
              |'$source' as source,
              |id,
              |createdTime,
              |lastUpdatedTime
              |from sourceAssets
              |limit 100
     """.stripMargin))
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationAssets")

    // Check if upsert worked
    val descriptionsAfterUpsert = retryWhile[Array[Row]](
      spark
        .sql(
          s"select description from destinationAssets where source = '$source' and description = 'bar'")
        .collect,
      df => df.length != 200)
    assert(descriptionsAfterUpsert.length == 200)
  }

  it should "allow partial updates" taggedAs WriteTest in {
    val sourceDf = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
      .where("name = 'upsertTestThree'")
    sourceDf.createTempView("update")
    val wdf = spark.sql("""
        |select 'upsertTestThree' as name, id from update
      """.stripMargin)

    wdf.write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save
  }

  it should "throw proper exception on invalid onconflict options" taggedAs WriteTest in {
    val sourceDf = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
      .where("name = 'upsertTestThree'")
    sourceDf.createTempView("invalid")
    val wdf = spark.sql("""
        |select 'upsertTestThree' as name, id from invalid
      """.stripMargin)

    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console

    val e = assertThrows[IllegalArgumentException] {
      wdf.write
        .format("cognite.spark")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "assets")
        .option("onconflict", "does-not-exists")
        .save()
    }
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "support some more partial updates" taggedAs WriteTest in  {
    val source = "spark-assets-test"

    // Cleanup assets
    cleanupAssets(source)
    val oldAssetsFromTestDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where source = '$source'").collect,
      df => df.length > 0)
    assert(oldAssetsFromTestDf.length == 0)

    // Post new assets
    spark
      .sql(s"""
                 |select id as externalId,
                 |name,
                 |null as parentId,
                 |'foo' as description,
                 |map("bar", "test") as metadata,
                 |'$source' as source,
                 |id,
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
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save

    // Check if update worked
    val assetsWithNewNameDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where description = '$description'").collect,
      df => df.length < 100)
    assert(assetsFromTestDf.length == 100)
  }

  it should "check for null ids on asset update" taggedAs WriteTest in {
    val df = spark.read
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .load()
      .where("name = 'upsertTestThree'")
    val wdf = spark.sql("""
        |select 'upsertTestThree' as name, null as id
      """.stripMargin)

    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console

    val e = intercept[SparkException] {
      wdf.write
        .format("cognite.spark")
        .option("apiKey", writeApiKey.apiKey)
        .option("type", "assets")
        .option("onconflict", "update")
        .save()
    }
    e.getCause shouldBe a[IllegalArgumentException]
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "allow deletes in savemode" taggedAs WriteTest in {
    val source = "spark-savemode-asset-deletes-test"

    cleanupAssets(source)
    val dfWithDeletesAsSource = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where source = '$source'").collect,
      df => df.length > 0)
    assert(dfWithDeletesAsSource.length == 0)

    // Insert some test data
    spark
      .sql(s"""
              |select id as externalId,
              |name,
              |null as parentId,
              |'foo' as description,
              |map("bar", "test") as metadata,
              |'$source' as source,
              |id,
              |createdTime,
              |lastUpdatedTime
              |from sourceAssets
              |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationAssets")

    // Check if insert worked
    val idsAfterInsert =
      retryWhile[Array[Row]](
        spark
          .sql(s"select id from destinationAssets where source = '$source'")
          .collect,
        df => df.length < 100)
    assert(idsAfterInsert.length == 100)

    // Delete the data
    spark
      .sql(s"""
         |select id
         |from destinationAssets
         |where source = '$source'
       """.stripMargin)
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .save()

    // Check if delete worked
    val idsAfterDelete =
      retryWhile[Array[Row]](
        spark
          .sql(s"select id from destinationAssets where source = '$source'")
          .collect,
        df => df.length > 0)
    assert(idsAfterDelete.isEmpty)
  }

  def cleanupAssets(source: String): Unit = {
    spark.sql(s"""select * from destinationAssets where source = '$source'""")
      .write
      .format("cognite.spark")
      .option("apiKey", writeApiKey.apiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .save()
  }
}
