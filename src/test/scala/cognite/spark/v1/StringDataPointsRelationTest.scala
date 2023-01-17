package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, TimestampType}
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class StringDataPointsRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest {
  val valhallTimeSeries = "'pi:160671'"
  val valhallTimeSeriesId = 1470524308850282L
  val valhallNumericTimeSeriesId = 3278479880462408L

  val sourceTimeSeriesDf = dataFrameReaderUsingOidc
    .option("type", "timeseries")
    .load()
  sourceTimeSeriesDf.createOrReplaceTempView("sourceTimeSeries")

  val destinationTimeSeriesDf = spark.read
    .format("cognite.spark.v1")
    .option("tokenUri", OIDCWrite.tokenUri)
    .option("clientId", OIDCWrite.clientId)
    .option("clientSecret", OIDCWrite.clientSecret)
    .option("project", OIDCWrite.project)
    .option("scopes", OIDCWrite.scopes)
    .option("type", "timeseries")
    .load()
  destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

  val destinationStringDataPointsDf = spark.read
    .format("cognite.spark.v1")
    .option("tokenUri", OIDCWrite.tokenUri)
    .option("clientId", OIDCWrite.clientId)
    .option("clientSecret", OIDCWrite.clientSecret)
    .option("project", OIDCWrite.project)
    .option("scopes", OIDCWrite.scopes)
    .option("type", "stringdatapoints")
    .option("collectMetrics", "true")
    .load()
  destinationStringDataPointsDf.createOrReplaceTempView("destinationStringDatapoints")

  "StringDataPointsRelation" should "use our own schema for data points" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .load()
      .where(s"id = $valhallTimeSeriesId")

    assert(
      df.schema.fields.sameElements(Array(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", StringType, nullable = false)
      )))
  }

  it should "fetch all data we expect" taggedAs ReadTest in {
    val metricsPrefix = "fetch.with.limit"

    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("limitPerPartition", "100")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1552604342) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)

    val df2 = dataFrameReaderUsingOidc
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
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "40")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(
        s"timestamp > to_timestamp(0) and timestamp < to_timestamp(1790902000) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "handle initial data set below batch size" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(s"id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  // Ignored as we currently attempt to read all string data points in a single partition.
  (it should "apply limit to each partition" taggedAs ReadTest).ignore {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limitPerPartition", "100")
      .option("partitions", "2")
      .load()
      .where(s"timestamp >= to_timestamp(1395666380) and id = $valhallTimeSeriesId")
    assert(df.count() == 200)

    val df2 = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "2000")
      .option("limitPerPartition", "100")
      .option("partitions", "3")
      .load()
      .where(
        s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1425187835) and id = $valhallTimeSeriesId")
    assert(df2.count() == 300)
  }

  it should "handle initial data set with the same size as the batch size" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "100")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(
        s"timestamp >= to_timestamp(0) and timestamp <= to_timestamp(1790902000) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "test that start/stop time are handled correctly for data points" taggedAs ReadTest in {
    val metricsPrefix = "string.start.stop"
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("partitions", "1")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1395892205) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
    val pointsRead = getNumberOfRowsRead(metricsPrefix, "stringdatapoints")
    assert(pointsRead == 9)
  }

  it should "handle start/stop time without duplicates when using multiple partitions" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("partitions", "7")
      .load()
      .where(
        s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1395892205) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
  }

  it should "fetch all string data points from a time series using paging" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "100")
      .load()
      .where(
        s"timestamp >= to_timestamp(0) and timestamp <= to_timestamp(1573081192) and id = $valhallTimeSeriesId")
    assert(df.count() == 2368)
  }

  it should "fetch all string data points from a time series using paging and respect limitPerPartition" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .option("batchSize", "10000")
      .option("limitPerPartition", "100")
      .option("partitions", "1")
      .load()
      .where(
        s"timestamp >= to_timestamp(0) and timestamp <= to_timestamp(1573081192) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)
  }

  it should "be an error to specify an invalid (time series) name" taggedAs WriteTest in {
    val e = intercept[Exception] {
      spark
        .sql(s"""
                   |select 9999 as id,
                   |null as externalId,
                   |true as isString,
                   |false as isStep,
                   |'someunit' as unit,
                   |bigint(123456789) as timestamp,
                   |"somevalue" as value
      """.stripMargin)
        .select(destinationStringDataPointsDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationStringDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  it should "be possible to write string data points" taggedAs WriteTest in {
    val metricsPrefix = "stringdatapoints.insert"
    val randomSuffix = shortRandomString()
    val testUnit = s"stringdatapoints${randomSuffix}"
    val tsName = s"stringdatapoints-${randomSuffix}"

    val stringDataPointsInsertDf = spark.read
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "stringdatapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    stringDataPointsInsertDf.createOrReplaceTempView("destinationStringDatapointsInsert")

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
              |'string-test${randomSuffix}' as externalId,
              |createdTime,
              |lastUpdatedTime,
              |dataSetId
              |from sourceTimeSeries
              |limit 1
     """.stripMargin)
      .select(sourceTimeSeriesDf.columns.map(col).toIndexedSeq: _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select id from destinationTimeSeries where name = '$tsName'""")
        .collect(),
      df => df.length < 1)
    assert(initialDescriptionsAfterPost.length == 1)

    val id = initialDescriptionsAfterPost.head.getLong(0)

    // Insert some string data points to the new time series
    spark
      .sql(s"""
              |select $id as id,
              |'use-id-if-given' as externalId,
              |to_timestamp(1509490001) as timestamp,
              |'testing' as value
      """.stripMargin)
      .write
      .insertInto("destinationStringDatapointsInsert")
    val pointsCreated = getNumberOfRowsCreated(metricsPrefix, "stringdatapoints")
    assert(pointsCreated == 1)

    // Check if post worked
    val dataPointsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationStringDatapointsInsert where id = '$id'""")
        .collect(),
      df => df.length < 1)
    assert(dataPointsAfterPost.length == 1)

    // Insert some data points to the new time series by external id
    spark
      .sql(s"""
              |select null as id,
              |'string-test${randomSuffix}' as externalId,
              |to_timestamp(1509500001) as timestamp,
              |'externalId' as value
      """.stripMargin)
      .write
      .insertInto("destinationStringDatapointsInsert")

    val pointsAfterInsertByExternalId = getNumberOfRowsCreated(metricsPrefix, "stringdatapoints")
    assert(pointsAfterInsertByExternalId == 2)

    // Check if post worked
    val dataPointsAfterPostByExternalId = retryWhile[Array[Row]](
      spark
        .sql(
          s"""select * from destinationStringDatapointsInsert where externalId = 'string-test${randomSuffix}'""")
        .collect(),
      df => df.length < 2)
    assert(dataPointsAfterPostByExternalId.length == 2)
  }

  it should "be an error to specify an invalid (time series) id" taggedAs (WriteTest) in {
    val e = intercept[SparkException] {
      spark
        .sql(s"""
                |select 9999 as id,
                |"ignore-external-id-if-id-given" as externalId,
                |bigint(123456789) as timestamp,
                |double(1) as value,
                |null as aggregation,
                |null as granularity
      """.stripMargin)
        .select(destinationStringDataPointsDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationStringDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  it should "be possible to delete string data points" taggedAs WriteTest in {
    val destinationDataPointsDf = spark.read
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "stringdatapoints")
      .load()
    destinationDataPointsDf.createOrReplaceTempView("destinationDatapoints")

    val tsName = s"dps-delete1-${shortRandomString()}"

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    spark
      .sql(s"""
           |select '$tsName' as name,
           |'$tsName' as externalId,
           |false as isStep,
           |true as isString
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .save()

    val Array(tsIdRow) = retryWhile[Array[Row]](
      spark
        .sql(s"""select id from destinationTimeSeries where externalId = '$tsName'""")
        .collect(),
      df => df.isEmpty)
    val tsId = tsIdRow.getAs[Long]("id")

    spark
      .sql(s"""
           |select $tsId as id,
           |to_timestamp(1509500001) as timestamp,
           |'a' as value
           |
           |union all
           |
           |select $tsId as id,
           |to_timestamp(1509500002) as timestamp,
           |'b' as value
           |
           |union all
           |
           |select $tsId as id,
           |to_timestamp(1509500003) as timestamp,
           |'c' as value
        """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "stringdatapoints")
      .option("onconflict", "upsert")
      .save()

    // Check if post worked
    retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where id = $tsId""")
        .collect(),
      df => { println(df.mkString(",")); df.length < 3 })

    spark
      .sql(s"""
           |select
           |$tsId as id,
           |NULL as externalId,
           |to_timestamp(1509500001) as inclusiveBegin,
           |NULL as exclusiveBegin,
           |to_timestamp(1509500001) as inclusiveEnd,
           |NULL as exclusiveEnd
           |
           |union all
           |
           |select NULL as id,
           |'$tsName' as externalId,
           |NULL as inclusiveBegin,
           |to_timestamp(1509500002) as exclusiveBegin,
           |NULL as inclusiveEnd,
           |to_timestamp(1509500060) as exclusiveEnd
         """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "stringdatapoints")
      .option("onconflict", "delete")
      .save()

    val dataPointsAfterDelete = retryWhile[Array[Row]](
      spark
        .sql(s"""select value from destinationDatapoints where id = $tsId""")
        .collect(),
      df => { println(df.mkString(",")); df.length != 1 })
    dataPointsAfterDelete.head.getAs[String]("value") shouldBe "b"
  }

  it should "fail reasonably when TS is numeric" in {
    val destinationDf = dataFrameReaderUsingOidc
      .option("type", "stringdatapoints")
      .load()
    destinationDf.createOrReplaceTempView("destinationNumericDatapoints")
    val error = sparkIntercept {
      spark
        .sql(s"select * from destinationNumericDatapoints where id = $valhallNumericTimeSeriesId")
        .collect()
    }

    assert(error.getMessage.contains(s"Cannot read numeric data points as string datapoints"))
    assert(error.getMessage.contains(s"The timeseries id=$valhallNumericTimeSeriesId"))
  }
}
