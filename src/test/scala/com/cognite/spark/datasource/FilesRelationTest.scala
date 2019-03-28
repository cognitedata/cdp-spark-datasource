package com.cognite.spark.datasource

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

class FilesRelationTest extends FlatSpec with Matchers with SparkTest {
  private val readApiKey = System.getenv("TEST_API_KEY_READ")
  private val writeApiKey = System.getenv("TEST_API_KEY_WRITE")

  "FilesRelation" should "read files" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .load()

    // as of 2019-02-19 there are 11 files, but more might be added in the future,
    // which should not be allowed to break this test.
    assert(df.count() >= 11)
  }

  it should "respect the limit option" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("limit", "5")
      .load()

    assert(df.count() == 5)
  }

  it should "use cursors when necessary" taggedAs ReadTest in {
    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("batchSize", "2")
      .load()

    assert(df.count() >= 11)
  }

  it should "support updates" taggedAs WriteTest in {
    val source = "spark datasource upsert test"
    val firstDirectoryName = "test directory"
    val secondDirectoryName = "dummy directory"


    val df = spark.read.format("com.cognite.spark.datasource")
      .option("apiKey", writeApiKey)
      .option("type", "files")
      .load()
    df.createOrReplaceTempView("filesSource")

    spark.sql(s"""
                 |select id,
                 |fileName,
                 |'$firstDirectoryName' as directory,
                 |source,
                 |sourceId,
                 |fileType,
                 |metadata,
                 |assetIds,
                 |uploaded,
                 |uploadedAt,
                 |createdTime,
                 |lastUpdatedTime
                 |from filesSource
                 |where source = '$source'
     """.stripMargin)
      .select(df.columns.map(col): _*)
      .write
      .insertInto("filesSource")

    val dfWithTestDirectory = retryWhile[DataFrame](
      spark.sql(s"select * from filesSource where directory = '$firstDirectoryName'"),
      rows => rows.count < 10)
    assert(dfWithTestDirectory.count == 10)

    spark.sql(s"""
                 |select id,
                 |fileName,
                 |'$secondDirectoryName' as directory,
                 |source,
                 |sourceId,
                 |fileType,
                 |metadata,
                 |assetIds,
                 |uploaded,
                 |uploadedAt,
                 |createdTime,
                 |lastUpdatedTime
                 |from filesSource
                 |where source = '$source'
     """.stripMargin)
      .select(df.columns.map(col): _*)
      .write
      .insertInto("filesSource")

    val dfWithUpdatedSource = retryWhile[DataFrame](
      spark.sql(s"select * from filesSource where directory = '$secondDirectoryName'"),
      rows => rows.count < 10)
    assert(dfWithUpdatedSource.count == 10)

    val emptyDfWithTestDirectory = retryWhile[DataFrame](
      spark.sql(s"select * from filesSource where directory = '$firstDirectoryName'"),
      rows => rows.count > 0)
    assert(emptyDfWithTestDirectory.count == 0)

  }
}
