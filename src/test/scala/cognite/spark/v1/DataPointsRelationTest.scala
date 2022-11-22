package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import com.cognite.sdk.scala.common.{CdpApiException, DataPoint}
import com.cognite.sdk.scala.v1.{CogniteExternalId, TimeSeriesCreate}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, TimestampType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, ParallelTestExecution}
import org.apache.spark.SparkException
import org.apache.spark.sql.Row

import java.time.Instant
import java.time.temporal.ChronoUnit

class DataPointsRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with BeforeAndAfterAll {

  val valhallTimeSeries = "'pi:195975'"

  val valhallTimeSeriesId = 3278479880462408L
  val valhallStringTimeSeriesId = 1470524308850282L
  // VAL_23-TT-92533:X.Value has some null aggregate values
  val withMissingAggregatesId = 5662453767080168L

  val destinationDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "datapoints")
    .load()
  destinationDf.createOrReplaceTempView("destinationDatapoints")

  override def beforeAll(): Unit = {
    val bluefieldClient = getBlufieldClient()
    bluefieldClient.timeSeries.deleteByExternalId("emel", true).unsafeRunSync()
    bluefieldClient.timeSeries.deleteByExternalId("emel2", true).unsafeRunSync()
    bluefieldClient.timeSeries
      .createOne(
        TimeSeriesCreate(
          externalId = Some("emel"),
          name = Some("emel"),
          isString = false,
          isStep = false))
      .unsafeRunSync()
    bluefieldClient.dataPoints
      .insert(
        id = CogniteExternalId("emel"),
        Seq(
          DataPoint(timestamp = Instant.ofEpochMilli(1661990400000L).minusMillis(1), value = 0.1),
          DataPoint(timestamp = Instant.ofEpochMilli(1661990400000L), value = 0.2),
          DataPoint(timestamp = Instant.ofEpochMilli(1661990400000L).plusMillis(1), value = 0.3),
          DataPoint(timestamp = Instant.ofEpochMilli(1662076800000L).minusMillis(1), value = 0.4),
          DataPoint(timestamp = Instant.ofEpochMilli(1662076800000L), value = 0.5),
          DataPoint(timestamp = Instant.ofEpochMilli(1662076800000L).plusMillis(1), value = 0.6)
        )
      )
      .unsafeRunSync()

    bluefieldClient.timeSeries
      .createOne(
        TimeSeriesCreate(
          externalId = Some("emel2"),
          name = Some("emel2"),
          isString = false,
          isStep = false))
      .unsafeRunSync()
    bluefieldClient.dataPoints
      .insert(
        id = CogniteExternalId("emel2"),
        Seq(
          DataPoint(timestamp = Instant.ofEpochMilli(1661990400000L).minusMillis(1), value = 0.7),
          DataPoint(timestamp = Instant.ofEpochMilli(1661990400000L), value = 0.8),
          DataPoint(timestamp = Instant.ofEpochMilli(1662076800000L), value = 0.9),
          DataPoint(timestamp = Instant.ofEpochMilli(1662076800000L).plusMillis(1), value = 1.0)
        )
      )
      .unsafeRunSync()
  }

  private val bluefieldDestinationDf = spark.read
    .format("cognite.spark.v1")
    .option("baseUrl", "https://bluefield.cognitedata.com")
    .option("tokenUri", bluefieldTokenUriStr)
    .option("clientId", bluefieldClientId)
    .option("clientSecret", bluefieldClientSecret)
    .option("project", "extractor-bluefield-testing")
    .option("scopes", "https://bluefield.cognitedata.com/.default")
    .option("type", "datapoints")
    .load()
  bluefieldDestinationDf.createOrReplaceTempView("destinationDatapointsBluefield")

  "DataPointsRelation" should "use our own schema for data points" taggedAs (ReadTest) in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(s"id = $valhallTimeSeriesId")

    assert(
      df.schema.fields.sameElements(Array(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("aggregation", StringType, nullable = true),
        StructField("granularity", StringType, nullable = true)
      )))
  }

  it should "throw an error when no id/externalId filter is provided" taggedAs ReadTest in {
    val thrown = the[CdfSparkIllegalArgumentException] thrownBy {
      dataFrameReaderUsingOidc
        .option("type", "datapoints")
        .load()
        .show()
    }
    assert(
      thrown.getMessage.contains(
        "Please filter by one or more ids or externalIds when reading data points."))
  }

  it should "test that start/stop time are handled correctly for data points" taggedAs (ReadTest) in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp > to_timestamp(1509528850) and timestamp < to_timestamp(1509528860) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
  }

  it should "support aggregations" taggedAs (ReadTest) in {
    val df1 = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(s"aggregation = 'stepInterpolation' and granularity = '1d' and id = $valhallTimeSeriesId")

    assert(df1.count() > 10)
    val df1Partitions = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1509490000) and timestamp <= to_timestamp(1510358400) and aggregation = 'max' and granularity = '1d' and id = $valhallTimeSeriesId")
    assert(df1Partitions.count() == 11)
    val df2 = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1520035200) and timestamp <= to_timestamp(1561507200) and aggregation = 'average' and granularity = '60d' and id = $valhallTimeSeriesId")
    assert(df2.count() == 8)
    val df3 = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .sort("timestamp")
      .where(
        s"timestamp >= to_timestamp(1514592000) and timestamp <= to_timestamp(1540512000) and aggregation = 'average' and granularity = '60d' and id = $valhallTimeSeriesId")
    val result = df3.collect()
    assert(result.length == 5)
    assert(result(0).getTimestamp(2).getTime == 1518912000000L)
  }

  it should "shift non-aligned aggregates to correct timestamps" taggedAs ReadTest in {
    val df1 = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .sort("timestamp")
      .where(
        s"timestamp >= to_timestamp(1509490001) and timestamp <= to_timestamp(1510358400) and aggregation = 'max' and granularity = '1d' and id = $valhallTimeSeriesId")
    val results1 = df1.collect()
    assert(results1.length == 11)
    assert(results1(0).getTimestamp(2).getTime == 1509494400000L)
    val df2 = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .sort("timestamp")
      .where(
        s"timestamp >= to_timestamp(1509490001) and timestamp <= to_timestamp(1510358400) and aggregation = 'max' and granularity = '1d' and id = $valhallTimeSeriesId")
    val results2 = df2.collect()
    assert(results2.length == 11)
    assert(results2(0).getTimestamp(2).getTime == 1509494400000L)
  }

  it should "be possible to specify multiple aggregation types in one query" taggedAs (ReadTest) in {
    val metricsPrefix = s"multi.aggregation.${shortRandomString()}"
    val results = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"timestamp >= to_timestamp(1508544000) and timestamp < to_timestamp(1511135998) and aggregation in ('sum', 'average', 'max') and granularity = '30d' and id = $valhallTimeSeriesId")
      .orderBy(col("aggregation").asc)
      .collect()
    assert(results.length == 3)
    val pointsRead = getNumberOfRowsRead(metricsPrefix, "datapoints")
    assert(pointsRead == 6)
    val Array(avg, max, sum) = results
    val timeSeriesId = valhallTimeSeriesId
    assert(sum.getLong(0) == timeSeriesId)
    assert(avg.getLong(0) == timeSeriesId)
    assert(max.getLong(0) == timeSeriesId)
    assert(sum.getString(4) == "sum")
    assert(avg.getString(4) == "average")
    assert(max.getString(4) == "max")
    assert(sum.getDouble(3) > avg.getDouble(3))
    assert(avg.getDouble(3) < max.getDouble(3))
  }

  it should "be an error to specify an aggregation without specifying a granularity" taggedAs (ReadTest) in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1508889600) and timestamp <= to_timestamp(1511481600) and aggregation in ('min') and id = $valhallTimeSeriesId")
    val e = intercept[Exception] {
      df.count()
    }
    e shouldBe an[CdfSparkIllegalArgumentException]
  }

  it should "be an error to specify a granularity without specifying an aggregation" taggedAs (ReadTest) in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1508889600) and timestamp <= to_timestamp(1511481600) and granularity = '30d' and id = $valhallTimeSeriesId")
    val e = intercept[Exception] {
      df.count()
    }
    e shouldBe an[CdfSparkIllegalArgumentException]
  }

  it should "be an error to specify an invalid granularity" taggedAs (ReadTest) in {
    for (granularity <- Seq("30", "dd", "d30", "1", "0", "1.2d", "1.4y", "1.4seconds")) {
      val df = dataFrameReaderUsingOidc
        .option("type", "datapoints")
        .load()
        .where(
          s"timestamp >= to_timestamp(1508889600) and timestamp <= to_timestamp(1511481600) and aggregation in ('min') and granularity = '$granularity' and id = $valhallTimeSeriesId")
      val e = intercept[Exception] {
        df.count()
      }
      e shouldBe an[CdfSparkIllegalArgumentException]
    }
  }

  it should "accept valid granularity specifications" taggedAs (ReadTest) in {
    for (granularity <- Seq(
        "1d",
        "day",
        "h",
        "hour",
        "m",
        "minute",
        "1hour",
        "2h",
        "20d",
        "13day",
        "7m",
        "7minute")) {
      val df = dataFrameReaderUsingOidc
        .option("type", "datapoints")
        .load()
        .where(
          s"timestamp > to_timestamp(0) and timestamp <= to_timestamp(1510358400) and aggregation in ('max') and granularity = '$granularity' and id = $valhallTimeSeriesId")
      assert(df.count() >= 1)
    }
  }

  it should "accept all aggregation options" in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(s"timestamp > to_timestamp(0) and timestamp <= to_timestamp(1510358400) and " +
        s"aggregation in ('average', 'max', 'min', 'count', 'sum', 'interpolation', 'stepInterpolation', 'totalVariation', 'continuousVariance', 'discreteVariance') " +
        s"and granularity = '1m' and id = $valhallTimeSeriesId")
    assert(df.select("aggregation").distinct().count() == 10)
  }

  it should "read data points without duplicates" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510150000) and timestamp <= to_timestamp(1510358401) and id = $valhallTimeSeriesId")
      .cache()
    assert(df.count() > 200000)
    assert(df.count() == df.distinct().count())
  }
  it should "read datapoint with value > 100" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510150000) and timestamp <= to_timestamp(1510358401) and value > 100 and id = $valhallTimeSeriesId")
      .cache()
    assert(df.count() > 200000)
    assert(df.count() == df.distinct().count())
  }

  it should "read data points while respecting limitPerPartition" taggedAs ReadTest in {
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .option("limitPerPartition", "1000")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510150000) and timestamp <= to_timestamp(1510358401) and id = $valhallTimeSeriesId")
      .cache()
    assert(df.count() < 10000)
  }

  it should "read very many aggregates correctly and without duplicates" taggedAs ReadTest in {
    val emptyDf = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510358400) and timestamp <= to_timestamp(1510358400) and aggregation in ('max') and granularity = '1s' and id = $valhallTimeSeriesId")
    assert(emptyDf.count() == 0)
    val oneDf = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510358400) and timestamp < to_timestamp(1510358401) and aggregation in ('max') and granularity = '1s' and id = $valhallTimeSeriesId")
    assert(oneDf.count() == 1)

    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510338400) and timestamp <= to_timestamp(1510358400) and aggregation in ('max') and granularity = '1s' and id = $valhallTimeSeriesId")
    assert(df.count() > 10000)
    assert(df.distinct().count() == df.count())
  }

  it should "handle missing aggregates" taggedAs ReadTest in {
    val metricsPrefix = s"missing.aggregates.${shortRandomString()}"
    val df = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"timestamp >= to_timestamp(1349732220) and timestamp <= to_timestamp(1572931920) and aggregation = 'average' and granularity = '5m' and id = $withMissingAggregatesId")
    assert(df.count() == 723073)
    val pointsRead = getNumberOfRowsRead(metricsPrefix, "datapoints")
    // We read one more than strictly necessary, but it's filtered out by Spark.
    assert(pointsRead == 723074)
  }

  it should "be possible to write datapoints to CDF using the Spark Data Source " taggedAs WriteTest in {
    val metricsPrefix = s"datapoints.insert.${shortRandomString()}"
    val testUnit = s"test ${shortRandomString()}"
    val tsName = s"datapoints-insert-${shortRandomString()}"

    val sourceTimeSeriesDf = dataFrameReaderUsingOidc
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
      .option("type", "datapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
    destinationDataPointsDf.createOrReplaceTempView("destinationDatapointsInsert")

    // Insert new time series test data
    spark
      .sql(s"""
              |select '' as description,
              |'$tsName' as name,
              |isString,
              |metadata,
              |'$testUnit' as unit,
              |null as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |'$tsName' as externalId,
              |createdTime,
              |lastUpdatedTime,
              |$testDataSetId as dataSetId
              |from sourceTimeSeries
              |limit 1
     """.stripMargin)
      .select(sourceTimeSeriesDf.columns.map(col).toIndexedSeq: _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(
          s"""select id from destinationTimeSeries where name = '$tsName' and dataSetId = $testDataSetId""")
        .collect(),
      df => df.length < 1)
    assert(initialDescriptionsAfterPost.length == 1)

    val id = initialDescriptionsAfterPost.head.getLong(0)

    // Insert some data points to the new time series
    spark
      .sql(s"""
              |select $id as id,
              |'this-should-be-ignored' as externalId,
              |to_timestamp(1509490001) as timestamp,
              |double(1.5) as value,
              |null as aggregation,
              |null as granularity
      """.stripMargin)
      .write
      .insertInto("destinationDatapointsInsert")
    val pointsCreated = getNumberOfRowsCreated(metricsPrefix, "datapoints")
    assert(pointsCreated == 1)

    // Check if post worked
    val dataPointsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapointsInsert where id = '$id'""")
        .collect(),
      df => df.length < 1)
    assert(dataPointsAfterPost.length == 1)

    // Insert some data points to the new time series by external id
    spark
      .sql(s"""
              |select null as id,
              |'$tsName' as externalId,
              |to_timestamp(1509500001) as timestamp,
              |double(1.5) as value,
              |null as aggregation,
              |null as granularity
      """.stripMargin)
      .write
      .insertInto("destinationDatapointsInsert")

    val pointsAfterInsertByExternalId = getNumberOfRowsCreated(metricsPrefix, "datapoints")
    assert(pointsAfterInsertByExternalId == 2)

    // Check if post worked
    val dataPointsAfterPostByExternalId = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapointsInsert where id = '$id'""")
        .collect(),
      df => df.length < 2)
    assert(dataPointsAfterPostByExternalId.length == 2)
  }

  it should "read all the datapoints in a time series with infrequent datapoints" taggedAs WriteTest in {

    val testUnit = s"last testing ${shortRandomString()}"
    val tsName = s"lastpoint${shortRandomString()}"

    val sourceTimeSeriesDf = dataFrameReaderUsingOidc
      .option("type", "timeseries")
      .load()
    sourceTimeSeriesDf.createOrReplaceTempView("sourceTimeSeries")

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    // Insert new time series test data
    spark
      .sql(s"""
              |select '' as description,
              |'$tsName' as name,
              |isString,
              |metadata,
              |'$testUnit' as unit,
              |null as assetId,
              |isStep,
              |cast(array() as array<long>) as securityCategories,
              |id,
              |'$tsName' as externalId,
              |createdTime,
              |lastUpdatedTime,
              |$testDataSetId as dataSetId
              |from sourceTimeSeries
              |limit 1
     """.stripMargin)
      .select(sourceTimeSeriesDf.columns.map(col).toIndexedSeq: _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(
          s"""select id from destinationTimeSeries where name = '$tsName' and dataSetId = $testDataSetId""")
        .collect(),
      df => df.length < 1)
    assert(initialDescriptionsAfterPost.length == 1)

    // Insert data points that are few and far apart
    val timestamps = Seq(1422713600, 1522713600, 1575000000, 1575000001)

    for (i <- timestamps) {
      spark
        .sql(s"""
                |select null as id,
                |'$tsName' as externalId,
                |to_timestamp($i) as timestamp,
                |double(1.5) as value,
                |null as aggregation,
                |null as granularity
      """.stripMargin)
        .write
        .insertInto("destinationDatapoints")
    }

    // Check if post worked
    val dataPointsAfterPostByExternalId = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where externalId = '$tsName'""")
        .collect(),
      df => df.length < timestamps.length)
    assert(dataPointsAfterPostByExternalId.length == timestamps.length)
  }

  it should "fail reasonably when datapoint values are invalid" taggedAs WriteTest in {
    val tsName = s"dps-insert1-${shortRandomString()}"

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    val destinationDataPointsDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "datapoints")
      .load()
    destinationDataPointsDf.createOrReplaceTempView("destinationDatapoints")

    spark
      .sql(s"""
           |select '$tsName' as name,
           |'$tsName' as externalId,
           |false as isStep,
           |false as isString
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .save()

    val Array(tsId) = retryWhile[Array[Row]](
      spark
        .sql(s"""select id from destinationTimeSeries where externalId = '$tsName'""")
        .collect(),
      df => df.isEmpty)
      .map(r => r.getLong(0).toString)

    val exception = intercept[SparkException] {
      spark
        .sql(s"""
             |select $tsId as id,
             |'this-should-be-ignored' as externalId,
             |to_timestamp(1509500001) as timestamp,
             |'non-numeric value' as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin)
        .write
        .insertInto("destinationDatapoints")
    }
    exception.getMessage should include("Column 'value' was expected to have type Double")
  }

  it should "fail reasonably when datapoint externalId has invalid type (save)" taggedAs WriteTest in {
    val exception = intercept[SparkException] {
      spark
        .sql(s"""
             |select cast(1 as long) as id,
             |1 as externalId,
             |to_timestamp(1509500001) as timestamp,
             |cast(1 as double) as value,
             |null as aggregation,
             |null as granularity
        """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "upsert")
        .save()
    }
    exception.getMessage should include(
      "Column 'externalId' was expected to have type String, but '1' of type Int was found (on row with externalId='1')")
  }

  it should "fail reasonably when datapoint value has invalid type (save)" taggedAs WriteTest in {
    val exception = intercept[SparkException] {
      spark
        .sql(s"""
             |select 1 as id,
             |to_timestamp(1509500001) as timestamp,
             |'non-numeric value' as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "upsert")
        .save()
    }
    exception.getMessage should include(
      "Column 'value' was expected to have type Double, but 'non-numeric value' of type String was found (on row with id='1')")
  }

  it should "fail reasonably when datapoint timestamp has invalid type (save)" taggedAs WriteTest in {
    val exception = intercept[SparkException] {
      spark
        .sql(s"""
             |select 1 as id,
             |1509500001 as timestamp,
             |1 as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "upsert")
        .save()
    }
    exception.getMessage should include(
      "Column 'timestamp' was expected to have type Timestamp, but '1509500001' of type Int was found (on row with id='1')")
  }

  it should "be possible to create data points for several time series at the same time" taggedAs WriteTest in {
    val tsName1 = s"dps-insert1-${shortRandomString()}"
    val tsName2 = s"dps-insert2-${shortRandomString()}"

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    spark
      .sql(s"""
           |select '$tsName1' as name,
           |'$tsName1' as externalId,
           |false as isStep,
           |false as isString
     """.stripMargin)
      .union(spark
        .sql(s"""
             |select '$tsName2' as name,
             |'$tsName2' as externalId,
             |false as isStep,
             |false as isString
     """.stripMargin))
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "upsert")
      .save()

    val idsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(
          s"""select id from destinationTimeSeries where externalId in ('$tsName1', '$tsName2') order by externalId asc""")
        .collect(),
      df => df.length < 2)
    assert(idsAfterPost.length == 2)

    val Array(id1, id2) = idsAfterPost.map(r => r.getLong(0).toString)
    spark
      .sql(s"""
           |select $id1 as id,
           |'this-should-be-ignored' as externalId,
           |to_timestamp(1509500001) as timestamp,
           |1.0 as value,
           |null as aggregation,
           |null as granularity
      """.stripMargin)
      .union(spark.sql(s"""
             |select $id2 as id,
             |'this-should-be-ignored' as externalId,
             |to_timestamp(1509500001) as timestamp,
             |9.0 as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin))
      .union(spark.sql(s"""
             |select null as id,
             |'$tsName1' as externalId,
             |to_timestamp(1509900001) as timestamp,
             |9.0 as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin))
      .union(spark.sql(s"""
             |select null as id,
             |'$tsName2' as externalId,
             |to_timestamp(1509900001) as timestamp,
             |1.0 as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin))
      .write
      .insertInto("destinationDatapoints")

    // Check if post worked
    val dataPointsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where id in ($id1, $id2)""")
        .collect(),
      df => df.length < 4)
    assert(dataPointsAfterPost.length == 4)
  }

  it should "be an error to specify an invalid (time series) id" taggedAs (WriteTest) in {

    val e = intercept[SparkException] {
      spark
        .sql(s"""
                   |select 9999 as id,
                   |"" as externalId,
                   |bigint(123456789) as timestamp,
                   |1 as value,
                   |null as aggregation,
                   |null as granularity
      """.stripMargin)
        .select(destinationDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto("destinationDatapoints")
    }
    e.getCause shouldBe a[CdpApiException]
    val cdpApiException = e.getCause.asInstanceOf[CdpApiException]
    assert(cdpApiException.code == 400)
  }

  it should "fail reasonably on invalid delete (empty range)" in {
    val e = intercept[SparkException] {
      spark
        .sql(s"""select
                |9999 as id,
                |to_timestamp(1509900000) as exclusiveEnd,
                |to_timestamp(1509900000) as inclusiveBegin
      """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "delete")
        .save()
    }
    e.getCause shouldBe a[CdfSparkIllegalArgumentException]
    e.getCause.getMessage should startWith("Delete range [1509900000000, 1509900000000) is invalid")
  }

  it should "fail reasonably on invalid delete (inclusiveEnd and exclusiveEnd)" in {
    val e = intercept[SparkException] {
      spark
        .sql(s"""select
                |"some id" as externalId,
                |to_timestamp(1509900000) as inclusiveEnd,
                |to_timestamp(1509900000) as exclusiveEnd,
                |to_timestamp(1509900000) as inclusiveBegin
      """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "delete")
        .save()
    }
    e.getCause shouldBe a[CdfSparkIllegalArgumentException]
    e.getCause.getMessage should startWith(
      "Delete row for data points can not contain both inclusiveEnd and exclusiveEnd ")
  }

  it should "fail reasonably on invalid delete (no Begin)" in {
    val e = intercept[SparkException] {
      spark
        .sql(s"""select
                |"some id" as externalId,
                |to_timestamp(1509900000) as inclusiveEnd
      """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "delete")
        .save()
    }
    e.getCause shouldBe a[CdfSparkIllegalArgumentException]
    e.getCause.getMessage should startWith(
      "Delete row for data points must contain inclusiveBegin or exclusiveBegin ")
  }

  it should "fail reasonably on invalid delete (no id)" in {
    val e = intercept[SparkException] {
      spark
        .sql(s"""select
                |to_timestamp(1509900000) as inclusiveEnd,
                |to_timestamp(1509900000) as inclusiveBegin
      """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("apiKey", writeApiKey)
        .option("type", "datapoints")
        .option("onconflict", "delete")
        .save()
    }
    e.getCause shouldBe a[CdfSparkIllegalArgumentException]
    e.getCause.getMessage should startWith("Delete row for data points must contain id or externalId ")
  }

  it should "be possible to delete data points" taggedAs WriteTest in {

    val tsName = s"dps-delete1-${shortRandomString()}"

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    spark
      .sql(s"""
           |select '$tsName' as name,
           |'$tsName' as externalId,
           |false as isStep,
           |false as isString
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
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
           |1.0 as value
           |
           |union all
           |
           |select $tsId as id,
           |to_timestamp(1509500002) as timestamp,
           |2.0 as value
           |
           |union all
           |
           |select $tsId as id,
           |to_timestamp(1509500003) as timestamp,
           |3.0 as value
        """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "datapoints")
      .option("onconflict", "upsert")
      .save()

    // Check if post worked
    retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where id = $tsId""")
        .collect(),
      df => df.length < 3)

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
      .option("apiKey", writeApiKey)
      .option("type", "datapoints")
      .option("onconflict", "delete")
      .save()

    val dataPointsAfterDelete = retryWhile[Array[Row]](
      spark
        .sql(s"""select value from destinationDatapoints where id = $tsId""")
        .collect(),
      df => df.length != 1)
    dataPointsAfterDelete.head.getAs[Double]("value") shouldBe 2.0
  }

  it should "be empty set when id does not exist" in {
    val idDoesNotExist = spark
      .sql(
        s"select * from destinationDatapoints where externalId = '2QEuQHKxStrhMG83wFgg9Rxd3NjZe8Y9ubyRXWciP'")

    idDoesNotExist shouldBe empty
  }

  it should "fail reasonably when TS is string" in {
    val destinationDf = dataFrameReaderUsingOidc
      .option("type", "datapoints")
      .load()
    destinationDf.createOrReplaceTempView("destinationStringDatapoints")
    val error = sparkIntercept {
      spark
        .sql(s"select * from destinationStringDatapoints where id = $valhallStringTimeSeriesId")
        .collect()
    }

    assert(error.getMessage.contains(s"Cannot read string data points as numeric datapoints"))
    assert(error.getMessage.contains(s"The timeseries id=$valhallStringTimeSeriesId"))
  }

  it should "read and write datapoints in the future" taggedAs WriteTest in {
    val testUnit = s"future ${shortRandomString()}"
    val tsName = s"future${shortRandomString()}"

    val destinationTimeSeriesDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .load()
    destinationTimeSeriesDf.createOrReplaceTempView("destinationTimeSeries")

    // Insert new time series test data
    spark
      .sql(s"""
              |select 'test serie with points in future' as description,
              |'$tsName' as name,
              |false as isString,
              |false as isStep,
              |'$testUnit' as unit,
              |'$tsName' as externalId,
              |$testDataSetId as dataSetId
     """.stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .save()

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(
          s"""select id from destinationTimeSeries where name = '$tsName' and dataSetId = $testDataSetId""")
        .collect(),
      df => df.length < 1)
    assert(initialDescriptionsAfterPost.length == 1)

    import spark.implicits._

    val now = Instant.now()

    val points = Seq(
      (tsName, now.minus(3, ChronoUnit.DAYS), -2),
      (tsName, now.minus(10, ChronoUnit.MINUTES), -1),
      (tsName, now.plus(10, ChronoUnit.MINUTES), 1),
      (tsName, now.plus(30, ChronoUnit.DAYS), 3),
      (tsName, now.plus(3, ChronoUnit.DAYS), 2),
      (tsName, now.plus(300, ChronoUnit.DAYS), 4)
    ).map { case (tsName, instant, value) => (tsName, java.sql.Timestamp.from(instant), value) }

    points
      .toDF("externalId", "timestamp", "value")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "datapoints")
      .option("onconflict", "upsert")
      .save()

    // Check if post worked and we can read all of them
    val dataPointsAfterPostByExternalId = retryWhile[Array[Row]](
      spark
        .sql(s"""select value from destinationDatapoints where externalId = '$tsName'""")
        .collect(),
      df => df.length < points.length)
    assert(dataPointsAfterPostByExternalId.length == points.length)
    val pointsInFuture =
      spark
        .sql(
          s"""select value from destinationDatapoints where externalId = '$tsName' and timestamp > now()""")
        .as[Double]
        .collect()
    assert(
      pointsInFuture.toList.sorted == points
        .filter(_._2.after(java.sql.Timestamp.from(now)))
        .map(_._3)
        .sorted)

    //Delete the timeSeries to avoid random timeSeries
    spark
      .sql(s"""select * from destinationTimeSeries where unit = '$testUnit'""")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "timeseries")
      .option("onconflict", "delete")
      .save()

    val emptyPointInFuture =
      spark
        .sql(
          s"""select value from destinationDatapoints where externalId = '$tsName' and timestamp > now()""")
        .as[Double]
        .collect()
    assert(emptyPointInFuture.length == 0)
  }

  // Ignored because it runs for an hour (on not so good computer and not so good internet)
  ignore should "just read all ~1B datapoints and not give up on 61mil ¯\\_(ツ)_/¯" in {
    import spark.implicits._
    val metricsPrefix = "datapoints.bigtable.dont.give.up.please"

    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", jetfiretest2ApiKey)
      .option("type", "datapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(col("id").equalTo(6518187534741535L))

    val realCount =
      df.where(expr("aggregation = 'count' and granularity = '1000d'"))
        .select("value")
        .as[Double]
        .collect()
        .sum
    assert(realCount > 1000 * 1000 * 800)
    val loadedCount = df.count() // this should actually load the datapoints from CDF
    assert(loadedCount == realCount)
    assert(loadedCount == getNumberOfRowsRead(metricsPrefix, "datapoints") - 1)
  }

  it should "list datapoints in a day with inclusive start and exclusive end limits" taggedAs (ReadTest) in {
    val res = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp >= TO_TIMESTAMP('2022-09-01T00:00:00Z') AND
        |dp.timestamp < TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res.length shouldBe 3
    res.map(row => row.getDouble(1)).toSet shouldBe Set(0.2, 0.3, 0.4)

    val res2 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp >= TO_TIMESTAMP('2022-09-01T00:00:00Z') AND
        |dp.timestamp < TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res2.length shouldBe 1
    res2.map(row => row.getDouble(1)).toSet shouldBe Set(0.8)
  }

  it should "list datapoints in a day with exclusive start and inclusive end limits" taggedAs (ReadTest) in {
    val res3 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp > TO_TIMESTAMP('2022-09-01T00:00:00Z') AND
        |dp.timestamp <= TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res3.length shouldBe 3
    res3.map(row => row.getDouble(1)).toSet shouldBe Set(0.3, 0.4, 0.5)

    val res4 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp > TO_TIMESTAMP('2022-09-01T00:00:00Z') AND
        |dp.timestamp <= TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res4.length shouldBe 1
    res4.map(row => row.getDouble(1)).toSet shouldBe Set(0.9)
  }

  it should "list datapoints in a day with exclusive start and exclusive end limits" taggedAs (ReadTest) in {
    val res5 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp > TO_TIMESTAMP('2022-09-01T00:00:00Z') AND
        |dp.timestamp < TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res5.length shouldBe 2
    res5.map(row => row.getDouble(1)).toSet shouldBe Set(0.3, 0.4)

    val res6 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp > TO_TIMESTAMP('2022-09-01T00:00:00Z') AND
        |dp.timestamp < TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res6.length shouldBe 0
  }

  it should "list datapoints only with start limit" taggedAs (ReadTest) in {
    val res7 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp > TO_TIMESTAMP('2022-09-01T00:00:00Z')""".stripMargin).collect()
    res7.length shouldBe 4
    res7.map(row => row.getDouble(1)).toSet shouldBe Set(0.3, 0.4, 0.5, 0.6)

    val res8 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp > TO_TIMESTAMP('2022-09-01T00:00:00Z')""".stripMargin).collect()
    res8.length shouldBe 2

    val res9 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp >= TO_TIMESTAMP('2022-09-01T00:00:00Z')""".stripMargin).collect()
    res9.length shouldBe 5
    res9.map(row => row.getDouble(1)).toSet shouldBe Set(0.2, 0.3, 0.4, 0.5, 0.6)

    val res10 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp >= TO_TIMESTAMP('2022-09-01T00:00:00Z')""".stripMargin).collect()
    res10.length shouldBe 3
  }

  it should "list datapoints only with end limit" taggedAs (ReadTest) in {
    val res11 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp < TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res11.length shouldBe 4
    res11.map(row => row.getDouble(1)).toSet shouldBe Set(0.1, 0.2, 0.3, 0.4)

    val res12 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp < TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res12.length shouldBe 2

    val res13 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel' AND
        |dp.timestamp <= TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res13.length shouldBe 5
    res13.map(row => row.getDouble(1)).toSet shouldBe Set(0.1, 0.2, 0.3, 0.4, 0.5)

    val res14 = spark.sql("""SELECT dp.timestamp, dp.value FROM destinationDatapointsBluefield dp
        |WHERE dp.externalId == 'emel2' AND
        |dp.timestamp <= TO_TIMESTAMP('2022-09-02T00:00:00Z')""".stripMargin).collect()
    res14.length shouldBe 3
  }
}
