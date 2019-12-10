package cognite.spark.v1

import java.util.UUID

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import com.softwaremill.sttp._
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._

class TimeSeriesRelationTest extends FlatSpec with Matchers with SparkTest {
  val sourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", readApiKey)
    .option("type", "timeseries")
    .load()
  sourceDf.createOrReplaceTempView("sourceTimeSeries")

  val destinationDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
  destinationDf.createOrReplaceTempView("destinationTimeSeries")

  val destinationDfWithLegacyName = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "timeseries")
    .option("useLegacyName", true)
    .load()
  destinationDfWithLegacyName.createOrReplaceTempView("destinationTimeSeriesWithLegacyName")

  it should "read all data regardless of the number of partitions" taggedAs ReadTest in {
    val dfreader = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
    for (np <- Seq(1, 4, 8, 12)){
      val df = dfreader.option("partitions", np.toString).load()
      assert(df.count == 363)
    }
  }

  it should "handle pushdown filters on assetId with multiple assetIds" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .option("partitions", "1")
      .load()
      .where(s"assetId In(6191827428964450, 3424990723231138, 3047932288982463)")
    assert(df.count == 87)
    val timeSeriesRead = getNumberOfRowsRead(metricsPrefix, "timeseries")
    assert(timeSeriesRead == 87)
  }

  it should "handle pushdown filters on assetId on nonexisting assetId" taggedAs WriteTest in {
    val metricsPrefix = "pushdown.filters.assetIds.nonexisting"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"assetId = 99")
    assert(df.count == 0)

    // Metrics counter does not create a Map key until reading the first row
    assertThrows[NoSuchElementException](getNumberOfRowsRead(metricsPrefix, "timeseries"))
  }

  it should "insert a time series with no name" taggedAs WriteTest in {
    val insertNoNameUnit = "time-series-insert-no-name"
    cleanUpTimeSeriesTestDataByUnit(insertNoNameUnit)
    val description = "no name"
    // Insert new time series test data
    spark
      .sql(s"""
              |select '$description' as description,
              |null as name,
              |isString,
              |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
              |'$insertNoNameUnit' as unit,
              |null as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |'no-name-time-series' as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |limit 1
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val dfAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationTimeSeries where description = '$description'""")
        .collect,
      df => df.length != 1)
    assert(dfAfterPost.length == 1)
    assert(dfAfterPost.head.get(0) == null)
  }

  it should "create time series with legacyName if useLegacyName option is set" taggedAs WriteTest in {
    val legacyNameUnit = "time-series-legacy-name"
    implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()
    val project = writeClient.login.status.project
    val legacyName1 = UUID.randomUUID().toString.substring(0, 8)
    val legacyName2 = UUID.randomUUID().toString.substring(0, 8)
    val before = the[CdpApiException] thrownBy writeClient.timeSeries.retrieveByExternalId(legacyName1)
    before.code shouldBe 400
    val before05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${legacyName1}")
      .header("api-key", writeApiKey)
      .contentType("application/json")
      .acceptEncoding("application/json")
      .send()
    before05.code shouldEqual 404
    spark
      .sql(s"""
              |select null as description,
              |'$legacyName1' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$legacyName1' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyName")

    val after = writeClient.timeSeries.retrieveByExternalId(legacyName1)
    after.externalId.get shouldBe legacyName1
    val after05 = sttp.get(uri"https://api.cognitedata.com/api/0.5/projects/${project}/timeseries/latest/${legacyName1}")
      .header("api-key", writeApiKey)
      .contentType("application/json")
      .acceptEncoding("application/json")
      .send()
    after05.code shouldEqual 200

    // Attempting to insert the same name should be an error when legacyName is set.
    // Note that we use a different externalId.
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val exception = the[SparkException] thrownBy spark
      .sql(s"""
              |select null as description,
              |'$legacyName1' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$legacyName2' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeriesWithLegacyName")
    spark.sparkContext.setLogLevel("WARN")
    assert(exception.getCause.isInstanceOf[IllegalArgumentException])

    val before2 = the[CdpApiException] thrownBy writeClient.timeSeries.retrieveByExternalId(legacyName2)
    before2.code shouldBe 400

    // inserting without useLegacyName should work.
    spark
      .sql(s"""
              |select null as description,
              |'$legacyName1' as name,
              |false as isString,
              |null as metadata,
              |'$legacyNameUnit' as unit,
              |null as assetId,
              |false as isStep,
              |null as securityCategories,
              |0 as id,
              |'$legacyName2' as externalId,
              |now() as createdTime,
              |now() lastUpdatedTime
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    val after2 = writeClient.timeSeries.retrieveByExternalId(legacyName1)
    after2.externalId.get shouldBe legacyName1

    writeClient.timeSeries.deleteByExternalIds(Seq(legacyName1, legacyName2))
  }

  it should "successfully both update and insert time series" taggedAs WriteTest in {
    val initialDescription = "post testing"
    val updatedDescription = "upsert testing"
    val insertTestUnit = "time-series-insert-testing"

    cleanUpTimeSeriesTestDataByUnit(insertTestUnit)

    // Insert new time series test data
    spark
      .sql(s"""
         |select '$initialDescription' as description,
         |concat('TEST', name) as name,
         |isString,
         |map("foo", null, "bar", "test", "some more", "test data", "nullValue", null) as metadata,
         |'$insertTestUnit' as unit,
         |null as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |id,
         |concat(string(id), "_upsert") as externalId,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |limit 5
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationTimeSeries where description = '$initialDescription'""")
        .collect,
      df => df.length < 5)
    assert(initialDescriptionsAfterPost.length == 5)

    // Upsert time series data, but wait 5 seconds to make sure
    Thread.sleep(5000)
    spark
      .sql(s"""
            |select '$updatedDescription' as description,
            |concat('TEST', name) as name,
            |isString,
            |map("foo", null, "bar", "test") as metadata,
            |unit,
            |null as assetId,
            |isStep,
            |securityCategories,
            |id,
            |externalId,
            |createdTime,
            |lastUpdatedTime
            |from destinationTimeSeries
            |where description = '$initialDescription'
          """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if upsert worked
    val updatedDescriptionsAfterUpsert = retryWhile[Array[Row]](
        spark
        .sql(s"""select * from destinationTimeSeries where description = '$updatedDescription'""")
        .collect,
      df => df.length < 5
    )
    assert(updatedDescriptionsAfterUpsert.length == 5)

    val initialDescriptionsAfterUpsert = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationTimeSeries where description = '$initialDescription'""")
        .collect,
      df => df.length > 0)
    assert(initialDescriptionsAfterUpsert.length == 0)
  }

  it should "support abort in savemode" taggedAs WriteTest in {
    val abortDescription = "spark-test-abort"
    val abortUnit = "time-series-insert-savemode"
    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(abortUnit)

    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$abortUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
         |select '$abortDescription' as description,
         |isString,
         |concat('TEST_', name) as name,
         |map("foo", null, "bar", "test") as metadata,
         |'$abortUnit' as unit,
         |NULL as assetId,
         |isStep,
         |cast(array() as array<long>) as securityCategories,
         |id,
         |concat(string(id), "_abort") as externalId,
         |createdTime,
         |lastUpdatedTime
         |from sourceTimeSeries
         |limit 5
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$abortDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Trying to insert existing rows should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val insertError = intercept[SparkException] {
      spark
        .sql(s"""
           |select description,
           |isString,
           |name,
           |metadata,
           |unit,
           |assetId,
           |isStep,
           |securityCategories,
           |id,
           |externalId,
           |createdTime,
           |lastUpdatedTime
           |from destinationTimeSeries
           |where unit = '$abortUnit'
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .save()
    }
    insertError.getCause shouldBe a[CdpApiException]
    val insertCdpApiException = insertError.getCause.asInstanceOf[CdpApiException]
    assert(insertCdpApiException.code == 409)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "support partial update in savemode" taggedAs WriteTest in {
    val insertDescription = "spark-insert-test"
    val updateDescription = "spark-update-test"
    val partialUpdateUnit = "time-series-partial-update"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(partialUpdateUnit)
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$partialUpdateUnit'""").collect
    }, df => df.length > 0
    )
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
              |select '$insertDescription' as description,
              |isString,
              |concat('TEST_', name) as name,
              |map("foo", null, "bar", "test") as metadata,
              |'$partialUpdateUnit' as unit,
              |NULL as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |concat(string(id), "_partial") as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |limit 5
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$insertDescription' and unit = '$partialUpdateUnit'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Update data with a new description, but wait 5 seconds to make sure
    Thread.sleep(5000)
    spark
      .sql(s"""
              |select '$updateDescription' as description,
              |id,
              |map("bar", "test") as metadata,
              |name
              |from destinationTimeSeries
              |where unit = '$partialUpdateUnit'
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "update")
      .save()

    val dfWithDescriptionUpdateTest = retryWhile[Array[Row]](
        spark
          .sql(s"select * from destinationTimeSeries where description = '$updateDescription'")
          .collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionUpdateTest.length == 5)

    // Trying to update nonexisting Time Series should throw a CdpApiException
    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
    val updateError = intercept[SparkException] {
      spark
        .sql(s"""
                   |select '$updateDescription' as description,
                   |isString,
                   |name,
                   |metadata,
                   |unit,
                   |assetId,
                   |isStep,
                   |securityCategories,
                   |bigint(1) as id,
                   |createdTime,
                   |lastUpdatedTime
                   |from destinationTimeSeries
                   |where unit = '$partialUpdateUnit'
     """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "update")
        .save()
    }
    updateError.getCause shouldBe a[CdpApiException]
    val updateCdpApiException = updateError.getCause.asInstanceOf[CdpApiException]
    assert(updateCdpApiException.code == 400)
    spark.sparkContext.setLogLevel("WARN")
  }

  it should "support upsert in savemode" taggedAs WriteTest in {
    val insertDescription = "spark-insert-test-savemode"
    val upsertDescription = "spark-upsert-test-savemode"
    val upsertUnit = "time-series-upsert-savemode"

    // Clean up any old test data
    cleanUpTimeSeriesTestDataByUnit(upsertUnit)
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$upsertUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
              |select '$insertDescription' as description,
              |isString,
              |concat('TEST_', name) as name,
              |map("foo", null, "bar", "test") as metadata,
              |'$upsertUnit' as unit,
              |NULL as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |concat(string(id), "_upsert_savemode") as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |limit 5
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .save()

    val dfWithDescriptionInsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$insertDescription'").collect,
      df => df.length < 5
    )
    assert(dfWithDescriptionInsertTest.length == 5)

    // Test upserts
    val existingTimeSeriesDf =
      spark.sql(s"""
              |select '$upsertDescription' as description,
              |isString,
              |name,
              |metadata,
              |unit,
              |assetId,
              |isStep,
              |securityCategories,
              |id,
              |string(id)+"_upsert_savemode" as externalId,
              |createdTime,
              |lastUpdatedTime
              |from destinationTimeSeries
              |where unit = '$upsertUnit'
     """.stripMargin)

    val nonExistingTimeSeriesDf =
      spark.sql(s"""
             |select '$upsertDescription' as description,
             |isString,
             |'test-upserts' as name,
             |map("foo", null, "bar", "test") as metadata,
             |unit,
             |assetId,
             |isStep,
             |securityCategories,
             |id + 10,
             |concat(string(id), "_upsert_savemode") as externalId,
             |createdTime,
             |lastUpdatedTime
             |from destinationTimeSeries
             |where unit = '$upsertUnit'
     """.stripMargin)

    existingTimeSeriesDf
      .union(nonExistingTimeSeriesDf)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .save()

    val dfWithDescriptionUpsertTest = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where description = '$upsertDescription'").collect,
      df => {
        df.length < 10
      }
    )
    assert(dfWithDescriptionUpsertTest.length == 10)
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val timeSeriesInsert = TimeSeriesInsertSchema()
    timeSeriesInsert.transformInto[TimeSeriesReadSchema]

    val timeSeriesUpsert = TimeSeriesUpsertSchema()
    timeSeriesUpsert.into[TimeSeriesReadSchema].withFieldComputed(_.id, tsu => tsu.id.getOrElse(0L))
  }

  it should "allow null ids on time series update" taggedAs WriteTest in {
    val updateTestUnit = "ts-update-testing-null-id"
    cleanUpTimeSeriesTestDataByUnit(updateTestUnit)
    retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where unit = '$updateTestUnit'").collect,
      rows => rows.length > 0
    )

    spark.sql(s"""
                 |select 'foo' as description,
                 |isString,
                 |name,
                 |map() as metadata,
                 |'$updateTestUnit' as unit,
                 |assetId,
                 |isStep,
                 |securityCategories,
                 |null as id,
                 |string(id) as externalId,
                 |createdTime,
                 |lastUpdatedTime
                 |from destinationTimeSeries
                 |limit 5
     """.stripMargin)
      .select(sourceDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val timeSeriesFromTestdf = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationTimeSeries where unit = '$updateTestUnit'").collect,
      df => df.length < 5
      )
    assert(timeSeriesFromTestdf.length == 5)

    // Upsert time series
    spark
      .sql(s"""
              |select externalId,
              |'bar' as description
              |from destinationTimeSeries
              |where unit = '$updateTestUnit'
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "update")
      .save()


    // Check if upsert worked
    val descriptionsAfterUpdate = retryWhile[Array[Row]](
      spark
        .sql(
          s"select description from destinationTimeSeries where unit = '$updateTestUnit' and description = 'bar'")
        .collect,
      df => df.length < 5)
    assert(descriptionsAfterUpdate.length == 5)
  }

  def cleanUpTimeSeriesTestDataByUnit(unit: String): Unit = {
    spark.sql(s"""select * from destinationTimeSeries where unit = '$unit'""")
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "timeseries")
        .option("onconflict", "delete")
        .save()
  }
}
