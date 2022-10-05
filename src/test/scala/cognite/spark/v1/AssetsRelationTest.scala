package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.AssetCreate
import io.scalaland.chimney.dsl._
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

import java.util.UUID
import scala.util.control.NonFatal

class AssetsRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {

  val sourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", readApiKey)
    .option("type", "assets")
    .load()
  sourceDf.createOrReplaceTempView("sourceAssets")

  val destinationDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "assets")
    .load()
  destinationDf.createOrReplaceTempView("destinationAssets")

  "AssetsRelation" should "read assets" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()

    df.createOrReplaceTempView("assetsRead")
    val res = spark
      .sql("select * from assetsRead")
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
    val metricsPrefix = s"pushdown.assets.name.${shortRandomString()}"
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

  it should "support pushdown filters on labels" taggedAs WriteTest in {
    val assetsTestSource = s"assets-relation-test-create-${shortRandomString()}"
    writeClient.assets.deleteByExternalId("asset_with_label_spark_datasource", ignoreUnknownIds = true)
    spark
      .sql(s"""select 'asset_with_label_spark_datasource' as externalId,
           |'asset_with_label_spark_datasource' as name,
           |'${assetsTestSource}' as source,
           |$testDataSetId as dataSetId,
           |array('scala-sdk-relationships-test-label2') as labels""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("type", "assets")
      .option("apiKey", writeApiKey)
      .save()

    retryWhile[Array[Row]](
      spark.sql(s"""select * from destinationAssets
                   |where labels = array('scala-sdk-relationships-test-label2')
                   |and source='${assetsTestSource}'""".stripMargin).collect(),
      df => df.length != 1
    )

    retryWhile[Array[Row]](
      spark.sql(s"""select * from destinationAssets
                    |where labels in(array('scala-sdk-relationships-test-label2'), NULL)
                    |and source='${assetsTestSource}'""".stripMargin).collect(),
      df => df.length != 1
    )

    retryWhile[Array[Row]](
      spark.sql(s"""select * from destinationAssets
                |where labels in(array('nonExistingLabel'), NULL)
                |and source='${assetsTestSource}'""".stripMargin).collect(),
      df => df.length != 0
    )
  }

  it should "support pushdown filters with nulls" taggedAs ReadTest in {
    val metricsPrefix = s"pushdown.assets.name.null.${shortRandomString()}"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("name in ('23-TT-92604B', NULL)")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)

    // these just should not fail:

    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
      .where("name = NULL")
      .collect() shouldBe empty

    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
      .where("name = '23-TT-92604B' and name <> NULL")
      .collect() shouldBe empty

    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
      .where("createdTime > NULL")
      .collect() shouldBe empty

    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
      .where("name in (NULL)")
      .collect() shouldBe empty

    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .load()
      .where("name = '23-TT-92604B' or name <> NULL")
      .count() shouldBe 1
  }

  it should "support pushdown filters on source" taggedAs WriteTest in {
    val metricsPrefix = s"pushdown.assets.source.${shortRandomString()}"
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
    val metricsPrefix = s"pushdown.assets.dataSetId.${shortRandomString()}"
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

  it should "not fetch all items if filter on id" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.assets.id"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("id = 1150715783816357 and dataSetId = 1")

    assert(df.count() == 0)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(eventsRead == 1)
  }

  it should "not fetch all items if filter on externalId" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.assets.externalId"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("name = 'PWST201' or externalId = 'houston.Pass 2.PWST202'")

    assert(df.count() == 2)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(eventsRead == 2)
  }

  it should "not fetch all items if filter on externalIdPrefix" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.assets.externalIdPrefix"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .load()
      .where("externalId LIKE 'houston.Pass%'")

    assert(df.count() == 3)
    val eventsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(eventsRead == 3)
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
    val metricsPrefix = s"pushdown.assets.duplicates.${shortRandomString()}"
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

  it should "support option filter assetSubtreeIds" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .option("assetSubtreeIds", "2161493773812721")
      .load()

    assert(df.count() == 3)
  }

  it should "support option filter assetSubtreeIds with externalId" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .option("assetSubtreeIds", """["WMT:23-YT-96105-01","WMT:23-TE-96137-02"]""")
      .load()

    assert(df.count() == 6)
  }

  it should "support option filter assetSubtreeIds with internal and externalId" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .option("assetSubtreeIds", """[2161493773812721,"WMT:23-YT-96105-01"]""")
      .load()

    assert(df.count() == 6)
  }

  it should "support option filter assetSubtreeIds with pushdown filters" taggedAs ReadTest in {
    val metricsPrefix = s"pushdown.assets.name.${shortRandomString()}"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("limitPerPartition", "1000")
      .option("partitions", "1")
      .option("assetSubtreeIds", """["WMT:23-YT-96105-01","WMT:23-TE-96137-02"]""")
      .load()
      .where("name = '23-YAHH-96105-01'")

    assert(df.count() == 1)

    val assetsRead = getNumberOfRowsRead(metricsPrefix, "assets")
    assert(assetsRead == 1)
  }

  it should "be possible to create assets" taggedAs WriteTest in {
    val assetsTestSource = s"assets-relation-test-create-${shortRandomString()}"
    val externalId = s"assets-test-create-${shortRandomString()}"
    val metricsPrefix = s"assets.test.create.${shortRandomString()}"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    df.createOrReplaceTempView("createAssets")
    cleanupAssets(assetsTestSource)
    try {
      spark
        .sql(s"""
                |select '$externalId' as externalId,
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
                |array('scala-sdk-relationships-test-label1') as labels,
                |$testDataSetId as dataSetId
      """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("createAssets")

      val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
      assert(assetsCreated == 1)

      val Array(createdAsset) = retryWhile[Array[Row]](
        spark.sql(s"select * from createAssets where source = '$assetsTestSource'").collect(),
        rows => rows.length < 1)

      createdAsset.getAs[String]("name") shouldBe "asset name"
      createdAsset.getAs[Long]("dataSetId") shouldBe testDataSetId
      createdAsset.getAs[String]("externalId") shouldBe externalId
    } finally {
      try {
        writeClient.assets.deleteByExternalId(externalId)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "handle null values in metadata when inserting in savemode" taggedAs WriteTest in {
    val assetsTestSource = s"assets-relation-test-create-${shortRandomString()}"
    val metricsPrefix = s"assets.create.savemode.${shortRandomString()}"
    val externalId = shortRandomString()

    try {
      spark
        .sql(s"""
                |select '$externalId' as externalId,
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
        .save()

      val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
      assert(assetsCreated == 1)
    } finally {
      try {
        writeClient.assets.deleteByExternalId(externalId)
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }

  it should "be possible to copy assets from one tenant to another" taggedAs WriteTest in {
    val assetsTestSource = s"assets-relation-test-copy-${shortRandomString()}"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .load()
    df.createOrReplaceTempView("assets")
    try {
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
                |labels,
                |dataSetId
                |from sourceAssets
                |limit 1
      """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("assets")
      retryWhile[Array[Row]](
        spark.sql(s"select * from destinationAssets where source = '$assetsTestSource'").collect(),
        rows => rows.length < 1)
    } finally {
      try {
        cleanupAssets(assetsTestSource)
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }

  it should "support upserts when using insertInto()" taggedAs WriteTest in {
    val source = s"spark-assets-upsert-testing${shortRandomString()}"
    val metricsPrefix = s"assets.upsert.test.${shortRandomString()}"

    val destinationDf: DataFrame = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    destinationDf.createOrReplaceTempView("destinationAssetsUpsert")

    val randomSuffix = shortRandomString()
    try {
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
                |array(cast(null as string)) as labels,
                |dataSetId
                |from sourceAssets
                |limit 100
     """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationAssetsUpsert")

      val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
      assert(assetsCreated == 100)

      // Check if post worked
      val assetsFromTestDf = retryWhile[Array[Row]](
        spark
          .sql(s"select * from destinationAssetsUpsert where source = '$source' and description = 'foo'")
          .collect(),
        df => df.length != 100)
      assert(assetsFromTestDf.length == 100)

      // Upsert assets
      // To avoid issues with eventual consistency, we create a new view for the assets we just created an will update.
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(assetsFromTestDf.toIndexedSeq),
          destinationDf.schema)
        .createOrReplaceTempView("destinationAssetsUpsertCreated")
      val dfToUpdate = spark
        .sql(s"""
              |select externalId,
              |name,
              |parentId,
              |parentExternalId,
              |'bar' as description,
              |map("foo", null, "bar", "test") as metadata,
              |source,
              |id,
              |createdTime,
              |lastUpdatedTime,
              |rootId,
              |aggregates,
              |labels,
              |dataSetId
              |from destinationAssetsUpsertCreated
              |where source = '$source'""".stripMargin)

      val dfToInsert = spark
        .sql(s"""
                |select null as externalId,
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
                |null as labels,
                |dataSetId
                |from sourceAssets
                |limit 100
      """.stripMargin)

      dfToUpdate
        .union(dfToInsert)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationAssetsUpsert")

      val assetsUpdatedAfterUpsert = getNumberOfRowsUpdated(metricsPrefix, "assets")
      assert(assetsUpdatedAfterUpsert == 100)
      val assetsCreatedAfterUpsert = getNumberOfRowsCreated(metricsPrefix, "assets")
      assert(assetsCreatedAfterUpsert == 200)

      // Check if upsert worked
      val descriptionsAfterUpsert = retryWhile[Array[Row]](
        spark
          .sql(
            s"select description from destinationAssets where source = '$source' and description = 'bar'")
          .collect(),
        df => df.length < 200)
      assert(descriptionsAfterUpsert.length == 200)

    } finally {
      try {
        cleanupAssets(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
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
      .save()
  }

  it should "allow empty metadata updates" taggedAs WriteTest in {
    val externalId1 = UUID.randomUUID.toString

    writeClient.assets.create(
      Seq(
        AssetCreate(
          name = externalId1,
          externalId = Some(externalId1),
          metadata = Some(Map("test1" -> "test1")))))

    writeClient.assets.retrieveByExternalId(externalId1).metadata shouldBe Some(Map("test1" -> "test1"))
    val wdf = spark.sql(s"select '$externalId1' as externalId, map() as metadata")

    wdf.write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "update")
      .save()

    val updated = writeClient.assets.retrieveByExternalId(externalId1)

    writeClient.assets.deleteByExternalId(externalId1)

    updated.metadata shouldBe Some(Map())
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

    assertThrows[CdfSparkIllegalArgumentException] {
      wdf.write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "does-not-exists")
        .save()
    }
  }

  it should "support some more partial updates" taggedAs WriteTest in {
    val source = s"spark-assets-test-partial-${shortRandomString()}"
    val destinationDf: DataFrame = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("collectMetrics", "false")
      .load()
    destinationDf.createOrReplaceTempView("destinationAssetsMorePartial")

    val randomSuffix = shortRandomString()
    try {
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
                |null as labels,
                |dataSetId
                |from sourceAssets
                |limit 100
     """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationAssetsMorePartial")

      // Check if post worked
      val assetsFromTestDf = retryWhile[Array[Row]](
        spark.sql(s"select * from destinationAssetsMorePartial where source = '$source'").collect(),
        df => df.length < 100)
      assert(assetsFromTestDf.length == 100)

      val description = "spark-testing-description"
      // Update assets
      val assetsWithNewNameDf = retryWhile[Array[Row]](
        {
          spark
            .sql(s"""
               |select '$description' as description,
               |id from destinationAssetsMorePartial
               |where source = '$source'
     """.stripMargin)
            .write
            .format("cognite.spark.v1")
            .option("apiKey", writeApiKey)
            .option("type", "assets")
            .option("onconflict", "update")
            .save()
          spark
            .sql(
              s"select * from destinationAssetsMorePartial where source = '$source' and description = '$description'")
            .collect()
        },
        df => df.length < 100
      )
      assert(assetsWithNewNameDf.length == 100)
    } finally {
      try {
        cleanupAssets(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "allow null ids on asset update" taggedAs WriteTest in {
    val source = s"spark-assets-updateId-${shortRandomString()}"

    val randomSuffix = shortRandomString()
    try {
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
                |null as id,
                |createdTime,
                |lastUpdatedTime,
                |0 as rootId,
                |null as aggregates,
                |null as labels,
                |dataSetId
                |from sourceAssets
                |limit 100
     """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationAssets")

      // Check if post worked
      val assetsFromTestDf = retryWhile[Array[Row]](
        spark
          .sql(s"select * from destinationAssets where source = '$source' and description = 'foo'")
          .collect(),
        df => df.length < 100)
      assert(assetsFromTestDf.length == 100)

      // Upsert assets
      val descriptionsAfterUpsert = retryWhile[Array[Row]](
        {
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
          spark
            .sql(
              s"select description from destinationAssets where source = '$source' and description = 'bar'")
            .collect()
        },
        df => df.length != 100
      )
      assert(descriptionsAfterUpsert.length == 100)
    } finally {
      try {
        cleanupAssets(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "be possible to make an upsert on externalId without name" in {
    val source = s"spark-assets-no-name-${shortRandomString()}"
    val externalId = shortRandomString()

    try {
      // Post new assets
      spark
        .sql(s"""
                |select '$externalId' as externalId,
                |'test' as name,
                |null as parentId,
                |null as parentExternalId,
                |'foo' as description,
                |map("bar", "test") as metadata,
                |'$source' as source,
                |null as id,
                |null as createdTime,
                |null as lastUpdatedTime,
                |0 as rootId,
                |null as aggregates,
                |null dataSetId
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "upsert")
        .save()

      // Check if update worked
      val descriptionsAfterUpsert = retryWhile[Array[Row]](
        {
          spark
            .sql(s"""
               |select '$externalId' as externalId,
               |'bar' as description
     """.stripMargin)
            .write
            .format("cognite.spark.v1")
            .option("apiKey", writeApiKey)
            .option("type", "assets")
            .option("onconflict", "upsert")
            .save()
          spark
            .sql(
              s"select description from destinationAssets where source = '$source' and description = 'bar'")
            .collect()
        },
        df => df.length != 1
      )
      assert(descriptionsAfterUpsert.length == 1)
    } finally {
      try {
        writeClient.assets.deleteByExternalId(externalId)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "be possible to update name (using upsert) given only externalId" in {
    val source = s"spark-assets-update-extId-${shortRandomString()}"
    val externalId = shortRandomString()

    try {
      // Post new assets
      spark
        .sql(s"""
                |select '$externalId' as externalId,
                |'test' as name,
                |null as parentId,
                |null as parentExternalId,
                |'foo' as description,
                |map("bar", "test") as metadata,
                |'$source' as source,
                |null as id,
                |null as createdTime,
                |null as lastUpdatedTime,
                |0 as rootId,
                |null as aggregates,
                |null dataSetId
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "upsert")
        .save()

      // Check if update worked
      val descriptionsAfterUpsert = retryWhile[Array[Row]](
        {
          spark
            .sql(s"""
               |select '$externalId' as externalId,
               |'updated-name' as name
     """.stripMargin)
            .write
            .format("cognite.spark.v1")
            .option("apiKey", writeApiKey)
            .option("type", "assets")
            .option("onconflict", "upsert")
            .save()
          spark
            .sql(
              s"select name from destinationAssets where source = '$source' and name = 'updated-name'")
            .collect()
        },
        df => df.length != 1
      )
      assert(descriptionsAfterUpsert.length == 1)
    } finally {
      try {
        writeClient.assets.deleteByExternalId(externalId)
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }

  it should "be possible to update both name and externalId (using upsert) given id" in {
    val source = s"spark-assets-update-both-extId-${shortRandomString()}"
    val externalId = shortRandomString()
    val newExternalId = shortRandomString()

    try {
      // Post new assets
      spark
        .sql(s"""
                |select '$externalId' as externalId,
                |'test' as name,
                |null as parentId,
                |null as parentExternalId,
                |'foo' as description,
                |map("bar", "test") as metadata,
                |'$source' as source,
                |null as id,
                |null as createdTime,
                |null as lastUpdatedTime,
                |0 as rootId,
                |null as aggregates,
                |null dataSetId
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "upsert")
        .save()

      val id = writeClient.assets.retrieveByExternalId(externalId).id

      // Check if update worked
      val descriptionsAfterUpsert = retryWhile[Array[Row]](
        {
          spark
            .sql(s"""
               |select ${id.toString} as id,
               |'updated-name' as name,
               |'updated-$newExternalId' as externalId
     """.stripMargin)
            .write
            .format("cognite.spark.v1")
            .option("apiKey", writeApiKey)
            .option("type", "assets")
            .option("onconflict", "upsert")
            .save()
          spark
            .sql(
              s"select name from destinationAssets where source = '$source' and name = 'updated-name' and externalId ='updated-$newExternalId'")
            .collect()
        },
        df => df.length != 1
      )
      assert(descriptionsAfterUpsert.length == 1)
    } finally {
      try {
        writeClient.assets.deleteByExternalId(externalId)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val assetInsert = AssetsInsertSchema(name = "test-asset")
    assetInsert.transformInto[AssetsReadSchema].copy(id = 1234L)

    val assetUpsert = AssetsUpsertSchema(name = Some("test-asset"), id = Some(1234L))
    assetUpsert
      .into[AssetsReadSchema]
      .withFieldComputed(_.id, aus => aus.id.getOrElse(0L))
      .withFieldComputed(_.name, aus => aus.name.getOrElse(""))
  }

  it should "allow deletes in savemode" taggedAs WriteTest in {
    val source = s"spark-savemode-asset-delete-${shortRandomString()}"

    val randomSuffix = shortRandomString()
    try {
      // Insert some test data
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
                |array('scala-sdk-relationships-test-label1') as labels,
                |dataSetId
                |from sourceAssets
                |limit 100
     """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationAssets")

      // Check if insert worked
      val idsAfterInsert =
        retryWhile[Array[Row]](
          spark
            .sql(s"select id from destinationAssets where source = '$source'")
            .collect(),
          df => df.length < 100)
      assert(idsAfterInsert.length == 100)

      val metricsPrefix = s"assets.delete.${shortRandomString()}"
      // Delete the data
      val idsAfterDelete =
        retryWhile[Array[Row]](
          {
            spark
              .sql(s"select id from destinationAssets where source = '$source'")
              .write
              .format("cognite.spark.v1")
              .option("apiKey", writeApiKey)
              .option("type", "assets")
              .option("onconflict", "delete")
              .option("collectMetrics", "true")
              .option("metricsPrefix", metricsPrefix)
              .save()
            spark
              .sql(s"select id from destinationAssets where source = '$source'")
              .collect()
          },
          df => df.length > 0
        )
      assert(idsAfterDelete.isEmpty)
      // Due to retries, rows deleted may exceed 100
      getNumberOfRowsDeleted(metricsPrefix, "assets") should be >= 100L
    } finally {
      try {
        cleanupAssets(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support ignoring unknown ids in deletes" in {
    spark
      .sql("select 1234 as id")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .option("ignoreUnknownIds", "true")
      .save()

    // Should throw error if ignoreUnknownIds is false
    val e = intercept[SparkException] {
      spark
        .sql("select 1234 as id")
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "assets")
        .option("onconflict", "delete")
        .option("ignoreUnknownIds", "false")
        .save()
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  it should "support deletes by externalIds" in {
    val source = s"spark-externalIds-asset-delete-${shortRandomString()}"
    val randomSuffix = shortRandomString()
    try {
      // Insert some test data
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
                |array('scala-sdk-relationships-test-label1') as labels,
                |dataSetId
                |from sourceAssets
                |limit 100
     """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationAssets")

      // Check if insert worked
      val idsAfterInsert =
        retryWhile[Array[Row]](
          spark
            .sql(s"select externalId from destinationAssets where source = '$source'")
            .collect(),
          df => df.length < 100)
      assert(idsAfterInsert.length == 100)

      val metricsPrefix = s"assets.delete.${shortRandomString()}"
      // Delete the data
      val idsAfterDelete =
        retryWhile[Array[Row]](
          {
            spark
              .sql(s"select externalId from destinationAssets where source = '$source'")
              .write
              .format("cognite.spark.v1")
              .option("apiKey", writeApiKey)
              .option("type", "assets")
              .option("onconflict", "delete")
              .option("collectMetrics", "true")
              .option("metricsPrefix", metricsPrefix)
              .save()
            spark
              .sql(s"select externalId from destinationAssets where source = '$source'")
              .collect()
          },
          df => df.length > 0
        )
      assert(idsAfterDelete.isEmpty)
      // Due to retries, rows deleted may exceed 100
      getNumberOfRowsDeleted(metricsPrefix, "assets") should be >= 100L
    } finally {
      try {
        cleanupAssets(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  def cleanupAssets(source: String): Unit =
    spark
      .sql(s"""select id from destinationAssets where source = '$source'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "assets")
      .option("onconflict", "delete")
      .save()
}
