package cognite.spark.v1

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

import scala.util.control.NonFatal

class FilesRelationTest extends FlatSpec with Matchers with SparkTest {
  val sourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", readApiKey)
    .option("type", "files")
    .load()

  sourceDf.createOrReplaceTempView("sourceFiles")

  val destinationDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "files")
    .load()

  destinationDf.createOrReplaceTempView("destinationFiles")

  "FilesRelation" should "read files" taggedAs ReadTest in {
    val res = spark.sqlContext
      .sql("select * from sourceFiles")
      .collect()
    // as of 2019-02-19 there are 11 files, but more might be added in the future,
    // which should not be allowed to break this test.
    assert(res.length == 18)
  }

  it should "respect the limit option" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "files")
      .option("limitPerPartition", "5")
      .option("partitions", "1")
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

  it should "support creating files using insertInto" taggedAs WriteTest in {
    val source = s"create-using-insertInto-${shortRandomString()}"

    try {
      spark
        .sql(s"""
                |select "name-$source" as name,
                |null as id,
                |'$source' as source,
                |'externalId-$source' as externalId,
                |null as mimeType,
                |null as metadata,
                |null as assetIds,
                |null as datasetId,
                |null as sourceCreatedTime,
                |null as sourceModifiedTime,
                |null as securityCategories,
                |null as uploaded,
                |null as createdTime,
                |null as lastUpdatedTime,
                |null as uploadedTime,
                |null as uploadUrl
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationFiles")

      val rows = retryWhile[Array[Row]](
        spark.sql(s"select * from destinationFiles where source = '$source'").collect,
        rows => rows.length < 1)
      assert(rows.length == 1)
    } finally {
      try {
        cleanupFiles(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support updates using id and externalId" taggedAs WriteTest in {
    val source = s"update-${shortRandomString()}"
    val metricsPrefix = "updates.files"

    try {
      val destinationDf: DataFrame = spark.read
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "files")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .load()
      destinationDf.createOrReplaceTempView("destinationFilesUpdate")

      spark
        .sql(s"""
                |select "name-$source" as name,
                |null as id,
                |'$source' as source,
                |'externalId-$source' as externalId,
                |null as mimeType,
                |null as metadata,
                |null as assetIds,
                |null as datasetId,
                |null as sourceCreatedTime,
                |null as sourceModifiedTime,
                |null as securityCategories,
                |null as uploaded,
                |null as createdTime,
                |null as lastUpdatedTime,
                |null as uploadedTime,
                |null as uploadUrl
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationFilesUpdate")

      val rows = retryWhile[Array[Row]](
        spark.sql(s"""select * from destinationFilesUpdate where source = "$source"""").collect,
        rows => rows.length < 1)
      assert(rows.length == 1)

      val filesCreated = getNumberOfRowsCreated(metricsPrefix, "files")
      assert(filesCreated == 1)

      val id = writeClient.files.retrieveByExternalId(s"externalId-$source").id

      //Update using id
      spark
        .sql(s"""
                |select ${id.toString} as id,
                |'$source' as source,
                |'updatedById-externalId-$source' as externalId
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "files")
        .option("onconflict", "update")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      val filesCreated2 = getNumberOfRowsCreated(metricsPrefix, "files")
      assert(filesCreated2 == 1)
      var updatedFiles = getNumberOfRowsUpdated(metricsPrefix, "files")
      assert(updatedFiles == 1)

      val updatedById =
        retryWhile[Array[Row]](
          spark
            .sql(
              s"select * from destinationFilesUpdate where externalId = 'updatedById-externalId-$source'")
            .collect,
          df => df.length < 1)
      assert(updatedById.length == 1)

      //Update using externalId
      spark
        .sql(s"""
                |select 'updatedById-externalId-$source' as externalId,
                |'updatedByExternalId-$source' as source
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "files")
        .option("onconflict", "update")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      val filesCreated3 = getNumberOfRowsCreated(metricsPrefix, "files")
      assert(filesCreated3 == 1)
      updatedFiles = getNumberOfRowsUpdated(metricsPrefix, "files")
      assert(updatedFiles == 2)

      val updatedByExternalId =
        retryWhile[Array[Row]](
          spark
            .sql(s"select * from destinationFilesUpdate where source = 'updatedByExternalId-$source'")
            .collect,
          df => df.length < 1)
      assert(updatedByExternalId.length == 1)

    } finally {
      try {
        cleanupFiles(s"updatedByExternalId-$source")
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "support upserts" taggedAs WriteTest in {
    val source = s"upsert-${shortRandomString()}"

    try {
      //insert data
      spark
        .sql(s"""
             |select "upsert-example" as name,
             |'$source' as source,
             |'externalId-$source' as externalId,
             |null as id,
             |null as mimeType
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "files")
        .option("onconflict", "upsert")
        .save()

      val id = writeClient.files.retrieveByExternalId(s"externalId-$source").id

      val insertWithUpsertIds =
        retryWhile[Array[Row]](
          spark
            .sql(s"select * from destinationFiles where source = '$source'")
            .collect,
          df => df.length < 1)
      assert(insertWithUpsertIds.length == 1)

      //update data
      spark
        .sql(s"""
             |select ${id.toString} as id,
             |'text/plain-$source' as mimeType,
             |'upserted-$source' as source
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "files")
        .option("onconflict", "upsert")
        .save()

      //check updated data
      val updatedWithUpsert =
        retryWhile[Array[Row]](
          spark
            .sql(
              s"select * from destinationFiles where mimeType = 'text/plain-$source' and source = 'upserted-$source'")
            .collect,
          df => df.length < 1)
      assert(updatedWithUpsert.length == 1)

      //original data doesn't exist
      val updated = retryWhile[Array[Row]](
        spark
          .sql(s"select * from destinationFiles where source = '$source'")
          .collect,
        df => df.length > 0)
      assert(updated.isEmpty)
    } finally {
      try {
        cleanupFiles(s"upserted-$source")
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }

  it should "support deletes" taggedAs WriteTest in {
    val source = s"delete-${shortRandomString()}"

    try {
      spark
        .sql(s"""
                |select "name-$source" as name,
                |null as id,
                |'$source' as source,
                |'externalId-$source' as externalId,
                |null as mimeType,
                |null as metadata,
                |null as assetIds,
                |null as datasetId,
                |null as sourceCreatedTime,
                |null as sourceModifiedTime,
                |null as securityCategories,
                |null as uploaded,
                |null as createdTime,
                |null as lastUpdatedTime,
                |null as uploadedTime,
                |null as uploadUrl
     """.stripMargin)
        .select(sourceDf.columns.map(col): _*)
        .write
        .insertInto("destinationFiles")

      val rows = retryWhile[Array[Row]](
        spark.sql(s"select * from destinationFiles where source = '$source'").collect,
        rows => rows.length < 1)
      assert(rows.length == 1)

      //Delete using id
      spark
        .sql(s"""
                |select id
                |from destinationFiles
                |where source = '$source'
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "files")
        .option("onconflict", "delete")
        .save()

      val idsAfterDelete =
        retryWhile[Array[Row]](
          spark
            .sql(s"select id from destinationFiles where source = '$source'")
            .collect,
          df => df.length > 0)
      assert(idsAfterDelete.isEmpty)
    } finally {
      try {
        cleanupFiles(source)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  def cleanupFiles(source: String): Unit =
    spark
      .sql(s"""select id from files where source = '$source'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "files")
      .option("onconflict", "delete")
      .save()
}
