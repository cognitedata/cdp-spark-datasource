package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class AssetsRelationTest extends FlatSpec with Matchers with SparkTest {

  val sourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", readApiKey)
    .option("type", "assets")
    .load()
  sourceDf.createTempView("sourceAssets")

  val destinationDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "assets")
    .load()
  destinationDf.createTempView("destinationAssets")

  it should "read assets" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()

    df.createOrReplaceTempView("assets")
    val res = spark
      .sql("select * from assets")
      .collect()
    assert(res.length == 1000)
  }

  it should "read assets with a small batchSize" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("batchSize", "1")
      .option("limitPerPartition", "10")
      .option("partitions", "1")
      .load()

    assert(df.count() == 10)
  }

  it should "support pushdown filters on name" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assets.name"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
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
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("source = 'some source'")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "support pushdown filters on dataSetId" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assets.dataSetId"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("name = '23-TT-92604B' or dataSetId = 1 or dataSetId = 2 or dataSetId = 3 or dataSetId = 4")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "support filtering on null" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("dataSetId is null")
      .limit(5)

    assert(df.count() == 5)
  }

  it should "handle duplicates in a pushdown filter scenario" taggedAs ReadTest in {
    val metricsPrefix = "pushdown.filters.assets.duplicates"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "5")
      .load()
      .where("source = 'some source' or name = '99-BB-99999'")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "be possible to create assets" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-create"
    val metricsPrefix = "assets.test.create"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
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
        |null as parentExternalId,
        |'asset description' as description,
        |null as metadata,
        |'$assetsTestSource' as source,
        |10 as id,
        |0 as createdTime,
        |0 as lastUpdatedTime,
        |null as rootId,
        |null as aggregates,
        |$testDataSetId as dataSetId
      """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("assets")

    val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
    assert(assetsCreated == 1)

    val Array(createdAsset) = retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)

    createdAsset.getAs[String]("name") shouldBe "asset name"
    createdAsset.getAs[Long]("dataSetId") shouldBe testDataSetId
    createdAsset.getAs[String]("externalId") shouldBe "1"
  }

  it should "handle null values in metadata when inserting in savemode" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-create"
    val metricsPrefix = "assets.test.create.savemode"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
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
              |map("foo", null, "bar", "test") as metadata,
              |'$assetsTestSource' as source,
              |bigint(10) as id
      """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "abort")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save

    val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
    assert(assetsCreated == 1)
  }

  it should "be possible to copy assets from one tenant to another" taggedAs WriteTest in {
    val assetsTestSource = "assets-relation-test-copy"
    val sourceDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
    sourceDf.createOrReplaceTempView("source_assets")
    val destinationDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .load()
    destinationDf.createOrReplaceTempView("assets")
    cleanupAssets(assetsTestSource)
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length > 0)
    spark
      .sql(s"""
         |select externalId,
         |name,
         |null as parentId,
         |null as parentExternalId,
         |description,
         |metadata,
         |'$assetsTestSource' as source,
         |id,
         |createdTime,
         |lastUpdatedTime,
         |0 as rootId,
         |null as aggregates,
         |dataSetId
         |from source_assets where id = 2675073401706610
      """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("assets")
    retryWhile[Array[Row]](
      spark.sql(s"select * from assets where source = '$assetsTestSource'").collect,
      rows => rows.length < 1)
  }

  it should "support upserts when using insertInto()" taggedAs WriteTest in {
    val source = s"spark-assets-upsert-testing${shortRandomString()}"
    val metricsPrefix = "assets.upsert.test"

    // Cleanup old assets
    cleanupAssets(source)
    retryWhile[Array[Row]](
      spark.sql(s"select * from sourceAssets where source = '$source'").collect,
      rows => rows.length > 0
    )

    val destinationDf: DataFrame = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    destinationDf.createOrReplaceTempView("destinationAssetsUpsert")

    val randomSuffix = shortRandomString()
    // Post new assets
    spark
      .sql(s"""
              |select concat(string(id), '${randomSuffix}') as externalId,
              |name,
              |null as parentId,
              |null as parentExternalId,
              |'foo' as description,
              |map("bar", "test") as metadata,
              |'$source' as source,
              |id,
              |createdTime,
              |lastUpdatedTime,
              |0 as rootId,
              |null as aggregates,
              |dataSetId
              |from sourceAssets
              |limit 100
     """.stripMargin)
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationAssetsUpsert")

    // Check if post worked
    val assetsFromTestDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssetsUpsert where source = '$source' and description = 'foo'").collect,
      df => df.length != 100)
    assert(assetsFromTestDf.length == 100)

    val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
    assert(assetsCreated == 100)

    // Upsert assets
    spark
      .sql(s"""
              |select externalId,
              |name,
              |null as parentId,
              |null as parentExternalId,
              |'bar' as description,
              |map("foo", null, "bar", "test") as metadata,
              |'$source'as source,
              |id,
              |createdTime,
              |lastUpdatedTime,
              |0 as rootId,
              |null as aggregates,
              |dataSetId
              |from destinationAssetsUpsert
              |where source = '$source'""".stripMargin)
      .union(spark
        .sql(s"""
              |select concat(externalId, '${randomSuffix}_create') as externalId,
              |name,
              |null as parentId,
              |null as parentExternalId,
              |'bar' as description,
              |metadata,
              |'$source' as source,
              |null as id,
              |createdTime,
              |lastUpdatedTime,
              |0 as rootId,
              |null as aggregates,
              |dataSetId
              |from sourceAssets
              |limit 100
     """.stripMargin))
      .select(destinationDf.columns.map(col): _*)
      .write
      .insertInto("destinationAssetsUpsert")

    // Check if upsert worked
    val descriptionsAfterUpsert = retryWhile[Array[Row]](
      spark
        .sql(
          s"select description from destinationAssets where source = '$source' and description = 'bar'")
        .collect,
      df => df.length != 200)
    assert(descriptionsAfterUpsert.length == 200)

    val assetsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "assets")
    assert(assetsCreatedAfterUpsert == 200)
    val assetsUpdatedAfterUpsert = getNumberOfRowsUpdated(metricsPrefix, "assets")
    assert(assetsUpdatedAfterUpsert == 100)
  }

  it should "allow partial updates" taggedAs WriteTest in {
    val sourceDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .load()
      .where("name = 'upsertTestThree'")
    sourceDf.createTempView("update")
    val wdf = spark.sql("""
        |select 'upsertTestThree' as name, id from update
      """.stripMargin)

    wdf.write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save
  }

  it should "throw proper exception on invalid onconflict options" taggedAs WriteTest in {
    val sourceDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .load()
      .where("name = 'upsertTestThree'")
    sourceDf.createTempView("invalid")
    val wdf = spark.sql("""
        |select 'upsertTestThree' as name, id from invalid
      """.stripMargin)

    disableSparkLogging() // Removing expected Spark executor Errors from the console

    val e = assertThrows[IllegalArgumentException] {
      wdf.write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "does-not-exists")
        .save()
    }
    enableSparkLogging()
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
                 |null as parentExternalId,
                 |'foo' as description,
                 |map("bar", "test") as metadata,
                 |'$source' as source,
                 |id,
                 |createdTime,
                 |lastUpdatedTime,
                 |0 as rootId,
                 |null as aggregates,
                 |dataSetId
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
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save

    // Check if update worked
    val assetsWithNewNameDf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationAssets where description = '$description'").collect,
      df => df.length < 100)
    assert(assetsFromTestDf.length == 100)
  }

  it should "allow null ids on asset update" taggedAs WriteTest in {
    val source = "spark-assets-updateId-testing"
    // Cleanup old assets
    cleanupAssets(source)
    retryWhile[Array[Row]](
      spark.sql(s"select * from sourceAssets where source = '$source'").collect,
      rows => rows.length > 0
    )

    // Post new assets
    spark
      .sql(s"""
              |select string(id) as externalId,
              |name,
              |null as parentId,
              |null as parentExternalId,
              |'foo' as description,
              |map("bar", "test") as metadata,
              |'$source' as source,
              |null as id,
              |createdTime,
              |lastUpdatedTime,
              |0 as rootId,
              |null as aggregates,
              |dataSetId
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
              |'bar' as description
              |from destinationAssets
              |where source = '$source'
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save()

    // Check if update worked
    val descriptionsAfterUpsert = retryWhile[Array[Row]](
      spark
        .sql(
          s"select description from destinationAssets where source = '$source' and description = 'bar'")
        .collect,
      df => df.length != 100)
    assert(descriptionsAfterUpsert.length == 100)
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val assetInsert = AssetsInsertSchema(name = "test-asset")
    assetInsert.transformInto[AssetsReadSchema].copy(id = 1234L)

    val assetUpsert = AssetsUpsertSchema(name = Some("test-asset"), id = Some(1234L))
    assetUpsert.into[AssetsReadSchema]
      .withFieldComputed(_.id, aus => aus.id.getOrElse(0L))
      .withFieldComputed(_.name, aus => aus.name.getOrElse(""))
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
              |null as parentExternalId,
              |'foo' as description,
              |map("bar", "test") as metadata,
              |'$source' as source,
              |id,
              |createdTime,
              |lastUpdatedTime,
              |0 as rootId,
              |null as aggregates,
              |dataSetId
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

    val metricsPrefix = "assets.delete"
    // Delete the data
    spark
      .sql(s"""
         |select id
         |from destinationAssets
         |where source = '$source'
       """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .save()

    // Check if delete worked
    val idsAfterDelete =
      retryWhile[Array[Row]](
        spark
          .sql(s"select id from destinationAssets where source = '$source'")
          .collect,
        df => df.length > 0)
    assert(idsAfterDelete.isEmpty)
    getNumberOfRowsDeleted(metricsPrefix, "assets") shouldBe 100
  }

  it should "support ignoring unknown ids in deletes" in {
    val source = "ignore-unknown-id-test"
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
              |null as parentExternalId,
              |'foo' as description,
              |map("bar", "test") as metadata,
              |'$source' as source,
              |id,
              |createdTime,
              |lastUpdatedTime,
              |0 as rootId,
              |null as aggregates,
              |dataSetId
              |from sourceAssets
              |limit 1
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
        df => df.length < 1)
    assert(idsAfterInsert.length == 1)

    spark
      .sql(
        s"""
           |select 1574865177148 as id
           |from destinationAssets
           |where source = '$source'
        """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .option("ignoreUnknownIds", "true")
      .save()

    disableSparkLogging() // Removing expected Spark executor Errors from the console

    // Should throw error if ignoreUnknownIds is false
    val e = intercept[SparkException] {
      spark
        .sql(
          s"""
             |select 1574865177148 as id
             |from destinationAssets
             |where source = '$source'
        """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "delete")
        .option("ignoreUnknownIds", "false")
        .save()
    }
    enableSparkLogging()
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  def cleanupAssets(source: String): Unit = {
    spark.sql(s"""select * from destinationAssets where source = '$source'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .save()
  }
}
