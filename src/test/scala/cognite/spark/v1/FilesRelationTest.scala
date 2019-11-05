package cognite.spark.v1

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

class FilesRelationTest extends FlatSpec with Matchers with SparkTest {
  "FilesRelation" should "read files" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .load()

    // as of 2019-02-19 there are 11 files, but more might be added in the future,
    // which should not be allowed to break this test.
    assert(df.count() >= 11)
  }

  it should "respect the limit option" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("limitPerPartition", "5")
      .load()

    assert(df.count() == 5)
  }

  it should "use cursors when necessary" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("batchSize", "2")
      .load()

    assert(df.count() >= 11)
  }

  it should "support updates" taggedAs WriteTest in {
    val originalSource = "spark datasource upsert test"
    val firstSource = "test directory"
    val secondSource = "dummy directory"

    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "files")
      .load()
    df.createOrReplaceTempView("filesSource")

    spark
      .sql(s"""
            |select id,
            |name,
            |source,
            |id as externalId,
            |mimetype,
            |metadata,
            |Array(1739842716040355) as assetIds,
            |uploaded,
            |uploadedTime,
            |createdTime,
            |lastUpdatedTime,
            |uploadUrl
            |from filesSource
            |where source = '$originalSource'
     """.stripMargin)
      .select(df.columns.map(col): _*)
      .write
      .insertInto("filesSource")

    val dfWithTestDirectory = retryWhile[Array[Row]](
      spark.sql(s"select * from filesSource where assetIds = Array(1739842716040355)").collect,
      rows => rows.length < 10)
    assert(dfWithTestDirectory.length == 10)

    spark
      .sql(s"""
            |select id,
            |name,
            |source,
            |id as externalId,
            |mimetype,
            |metadata,
            |Array(176631399357975) as assetIds,
            |uploaded,
            |uploadedTime,
            |createdTime,
            |lastUpdatedTime,
            |uploadUrl
            |from filesSource
            |where source = '$originalSource'
     """.stripMargin)
      .select(df.columns.map(col): _*)
      .write
      .insertInto("filesSource")

    val dfWithUpdatedSource = retryWhile[Array[Row]](
      spark.sql(s"select * from filesSource where assetIds = Array(176631399357975)").collect,
      rows => rows.length < 10)
    assert(dfWithUpdatedSource.length == 10)

    val emptyDfWithTestsource = retryWhile[Array[Row]](
      spark.sql(s"select * from filesSource where assetIds = Array(1739842716040355)").collect,
      rows => rows.length > 0)
    assert(emptyDfWithTestsource.length == 0)

  }
}
