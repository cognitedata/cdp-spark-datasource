package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, TimestampType}
import org.scalatest.{FlatSpec, Matchers}

class StringDataPointsRelationTest extends FlatSpec with Matchers with SparkTest {
  val valhallTimeSeries = "'VAL_23-PIC-96153:MODE'"
  val valhallTimeSeriesId = 6536948395539605L

  "StringDataPointsRelation" should "use our own schema for data points" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .load()
      .where(s"id = $valhallTimeSeriesId")

    assert(df.schema.fields.sameElements(Array(
      StructField("id", LongType, nullable = true),
      StructField("externalId", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("value", StringType, nullable = false))))
  }

  it should "fetch all data we expect" taggedAs ReadTest in {
    val metricsPrefix = "fetch.with.limit"

    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("limitPerPartition", "100")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1552604342) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)

    val df2 = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("limitPerPartition", "100")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"timestamp <= to_timestamp(1552604342) and id = $valhallTimeSeriesId")
    assert(df2.count() == 100)

    val pointsRead = getNumberOfRowsRead(metricsPrefix, "stringdatapoints")
    assert(pointsRead == 200)
  }

  it should "iterate over period longer than limit" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "40")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp > to_timestamp(0) and timestamp < to_timestamp(1790902000) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "handle initial data set below batch size" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(s"id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "apply limit to each partition" taggedAs ReadTest ignore {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limitPerPartition", "100")
      .option("partitions", "2")
      .load()
      .where(s"timestamp >= to_timestamp(1395666380607) and timestamp <= to_timestamp(1557485862500) and id = $valhallTimeSeriesId")
    assert(df.count() == 200)

    val df2 = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limitPerPartition", "100")
      .option("partitions", "3")
      .load()
      .where(s"timestamp >= to_timestamp(1395666380607) and timestamp <= to_timestamp(1425187835353) and id = $valhallTimeSeriesId")
    assert(df2.count() == 300)
  }

  it should "handle initial data set with the same size as the batch size" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "100")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp >= to_timestamp(0) and timestamp <= to_timestamp(1790902000) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "test that start/stop time are handled correctly for data points" taggedAs ReadTest in {
    val metricsPrefix = "string.start.stop"
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("partitions", "1")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1395892205) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
    val pointsRead = getNumberOfRowsRead(metricsPrefix, "stringdatapoints")
    assert(pointsRead == 9)
  }

  it should "handle start/stop time without duplicates when using multiple partitions" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("partitions", "7")
      .load()
      .where(s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1395892205) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
  }

  it should "fetch all string data points from a time series using paging" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "100")
      .load()
      .where(s"timestamp >= to_timestamp(0) and timestamp <= to_timestamp(1573081192) and id = $valhallTimeSeriesId")
    assert(df.count() == 2146)
  }

  it should "fetch all string data points from a time series using paging and respect limitPerPartition" taggedAs ReadTest in {
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("batchSize", "10000")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(s"timestamp >= to_timestamp(0) and timestamp <= to_timestamp(1573081192) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "be an error to specify an invalid (time series) name" taggedAs WriteTest in {
    val destinationDf = spark.read.format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "stringdatapoints")
      .load()
    destinationDf.createTempView("destinationStringDatapoints")

    disableSparkLogging() // Removing expected Spark executor Errors from the console
    val e = intercept[Exception] {
      spark.sql(s"""
                   |select 9999 as id,
                   |null as externalId,
                   |true as isString,
                   |false as isStep,
                   |'someunit' as unit,
                   |bigint(123456789) as timestamp,
                   |"somevalue" as value
      """.stripMargin)
        .select(destinationDf.columns.map(col): _*)
        .write
        .insertInto("destinationStringDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
    enableSparkLogging()
  }

  it should "be possible to write string data points" taggedAs WriteTest in {
    val metricsPrefix = "stringdatapoints.insert"
    val randomSuffix = shortRandomString()
    val testUnit = s"stringdatapoints${randomSuffix}"
    val tsName = s"stringdatapoints-${randomSuffix}"

    val sourceTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "timeseries")
      .load()
    sourceTimeSeriesDf.createOrReplaceTempView("sourceTimeSeries")

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    val destinationDataPointsDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "stringdatapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    destinationDataPointsDf.createOrReplaceTempView("destinationDatapoints")

    // Clean up old time series data
    spark.sql(s"""select * from destinationTimeSeries where unit = '$testUnit'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "delete")
      .save()

    // Check that it's gone
    val testTimeSeriesAfterCleanup = retryWhile[Array[Row]]({
      spark.sql(s"""select * from destinationTimeSeries where unit = '$testUnit'""").collect
    }, df => df.length > 0)
    assert(testTimeSeriesAfterCleanup.length == 0)

    // Insert new time series test data
    spark
      .sql(s"""
              |select 'stringdatapoints test' as description,
              |'$tsName' as name,
              |true as isString,
              |metadata,
              |'$testUnit' as unit,
              |null as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |'stringdatapoints-testing${randomSuffix}' as externalId,
              |createdTime,
              |lastUpdatedTime
              |from sourceTimeSeries
              |limit 1
     """.stripMargin)
      .select(sourceTimeSeriesDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select id from destinationTimeSeries where name = '$tsName'""")
        .collect,
      df => df.length < 1)
    assert(initialDescriptionsAfterPost.length == 1)

    val id = initialDescriptionsAfterPost.head.getLong(0)

    // Insert some string data points to the new time series
    spark
      .sql(s"""
              |select $id as id,
              |'insert-test-data' as externalId,
              |to_timestamp(1509490001) as timestamp,
              |'testing' as value
      """.stripMargin)
      .write
      .insertInto("destinationDatapoints")
    val pointsCreated = getNumberOfRowsCreated(metricsPrefix, "stringdatapoints")
    assert(pointsCreated == 1)

    // Check if post worked
    val dataPointsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where id = '$id'""")
        .collect,
      df => df.length < 1)
    assert(dataPointsAfterPost.length == 1)

    // Insert some data points to the new time series by external id
    spark
      .sql(s"""
              |select null as id,
              |'stringdatapoints-testing${randomSuffix}' as externalId,
              |to_timestamp(1509500001) as timestamp,
              |'externalId' as value
      """.stripMargin)
      .write
      .insertInto("destinationDatapoints")

    val pointsAfterInsertByExternalId = getNumberOfRowsCreated(metricsPrefix, "stringdatapoints")
    assert(pointsAfterInsertByExternalId == 2)

    // Check if post worked
    val dataPointsAfterPostByExternalId = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where id = $id""")
        .collect,
      df => df.length < 2)
    assert(dataPointsAfterPostByExternalId.length == 2)
  }

  it should "be an error to specify an invalid (time series) id" taggedAs (WriteTest) in {
    val destinationDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "datapoints")
      .load()
    destinationDf.createOrReplaceTempView("destinationDatapoints")

    disableSparkLogging() // Removing expected Spark executor Errors from the console
    val e = intercept[SparkException] {
      spark
        .sql(s"""
                |select 9999 as id,
                |"" as externalId,
                |bigint(123456789) as timestamp,
                |double(1) as value,
                |null as aggregation,
                |null as granularity
      """.stripMargin)
        .select(destinationDf.columns.map(col): _*)
        .write
        .insertInto("destinationDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
    enableSparkLogging()
  }
}
