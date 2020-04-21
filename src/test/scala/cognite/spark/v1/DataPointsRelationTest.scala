package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, TimestampType}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.SparkException
import org.apache.spark.sql.Row

class DataPointsRelationTest extends FlatSpec with Matchers with SparkTest {
  val valhallTimeSeries = "'VAL_23-FT-92537-04:X.Value'"

  val valhallTimeSeriesId = 3385857257491234L
  // VAL_23-TT-92533:X.Value has some null aggregate values
  val withMissingAggregatesId = 3644806523397779L

  "DataPointsRelation" should "use our own schema for data points" taggedAs (ReadTest) in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
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
    val thrown = the[IllegalArgumentException] thrownBy {
      spark.read
        .format("cognite.spark.v1")
        .option("apiKey", readApiKey)
        .option("type", "datapoints")
        .load()
        .show()
    }
    assert(thrown.getMessage.contains("Please filter by one or more ids or externalIds when reading data points."))
  }

  it should "test that start/stop time are handled correctly for data points" taggedAs (ReadTest) in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp > to_timestamp(1509528850) and timestamp < to_timestamp(1509528860) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
  }

  it should "support aggregations" taggedAs (ReadTest) in {
    val df1 = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"aggregation = 'stepInterpolation' and granularity = '1d' and id = $valhallTimeSeriesId")

    assert(df1.count() > 10)
    val df1Partitions = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1509490000) and timestamp <= to_timestamp(1510358400) and aggregation = 'max' and granularity = '1d' and id = $valhallTimeSeriesId")
    assert(df1Partitions.count() == 11)
    val df2 = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1520035200) and timestamp <= to_timestamp(1561507200) and aggregation = 'average' and granularity = '60d' and id = $valhallTimeSeriesId")
    assert(df2.count() == 8)
    val df3 = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
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
    val df1 = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .sort("timestamp")
      .where(
        s"timestamp >= to_timestamp(1509490001) and timestamp <= to_timestamp(1510358400) and aggregation = 'max' and granularity = '1d' and id = $valhallTimeSeriesId")
    val results1 = df1.collect()
    assert(results1.length == 11)
    assert(results1(0).getTimestamp(2).getTime == 1509494400000L)
    val df2 = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
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
    val metricsPrefix = "multi.aggregation"
    val results = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"timestamp >= to_timestamp(1508544000) and timestamp < to_timestamp(1511135998) and aggregation in ('sum', 'average', 'max') and granularity = '30d' and id = $valhallTimeSeriesId")
      .orderBy(col("aggregation").asc).collect()
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
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(s"timestamp >= to_timestamp(1508889600) and timestamp <= to_timestamp(1511481600) and aggregation in ('min') and id = $valhallTimeSeriesId")
    val e = intercept[Exception] {
      df.count()
    }
    e shouldBe an[IllegalArgumentException]
  }

  it should "be an error to specify a granularity without specifying an aggregation" taggedAs (ReadTest) in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(s"timestamp >= to_timestamp(1508889600) and timestamp <= to_timestamp(1511481600) and granularity = '30d' and id = $valhallTimeSeriesId")
    val e = intercept[Exception] {
      df.count()
    }
    e shouldBe an[IllegalArgumentException]
  }

  it should "be an error to specify an invalid granularity" taggedAs (ReadTest) in {
    disableSparkLogging() // Removing expected Spark executor Errors from the console
    for (granularity <- Seq("30", "dd", "d30", "1", "0", "1.2d", "1.4y", "1.4seconds")) {
      val df = spark.read
        .format("cognite.spark.v1")
        .option("apiKey", readApiKey)
        .option("type", "datapoints")
        .load()
        .where(
          s"timestamp >= to_timestamp(1508889600) and timestamp <= to_timestamp(1511481600) and aggregation in ('min') and granularity = '$granularity' and id = $valhallTimeSeriesId")
      val e = intercept[Exception] {
        df.count()
      }
      e shouldBe an[IllegalArgumentException]
    }
    enableSparkLogging()
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
      val df = spark.read
        .format("cognite.spark.v1")
        .option("apiKey", readApiKey)
        .option("type", "datapoints")
        .load()
        .where(
          s"timestamp > to_timestamp(0) and timestamp <= to_timestamp(1510358400) and aggregation in ('max') and granularity = '$granularity' and id = $valhallTimeSeriesId")
      assert(df.count() >= 1)
    }
  }

  it should "accept all aggregation options" in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp > to_timestamp(0) and timestamp <= to_timestamp(1510358400) and " +
          s"aggregation in ('average', 'max', 'min', 'count', 'sum', 'interpolation', 'stepInterpolation', 'totalVariation', 'continuousVariance', 'discreteVariance') " +
          s"and granularity = '1m' and id = $valhallTimeSeriesId")
    assert(df.select("aggregation").distinct.count == 10)
  }

  it should "read data points without duplicates" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510150000) and timestamp <= to_timestamp(1510358401) and id = $valhallTimeSeriesId")
      .cache()
    assert(df.count() > 200000)
    assert(df.count() == df.distinct().count())
  }

  it should "read data points while respecting limitPerPartition" taggedAs ReadTest in {
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("limitPerPartition", "1000")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510150000) and timestamp <= to_timestamp(1510358401) and id = $valhallTimeSeriesId")
      .cache()
    assert(df.count() < 10000)
  }

  it should "read very many aggregates correctly and without duplicates" taggedAs ReadTest in {
    val emptyDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510358400) and timestamp <= to_timestamp(1510358400) and aggregation in ('max') and granularity = '1s' and id = $valhallTimeSeriesId")
    assert(emptyDf.count() == 0)
    val oneDf = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510358400) and timestamp < to_timestamp(1510358401) and aggregation in ('max') and granularity = '1s' and id = $valhallTimeSeriesId")
    assert(oneDf.count() == 1)

    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .load()
      .where(
        s"timestamp >= to_timestamp(1510338400) and timestamp <= to_timestamp(1510358400) and aggregation in ('max') and granularity = '1s' and id = $valhallTimeSeriesId")
    assert(df.count() > 10000)
    assert(df.distinct().count() == df.count())
  }

  it should "handle missing aggregates" taggedAs ReadTest in {
    val metricsPrefix = "missing.aggregates"
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "datapoints")
      .option("collectMetrics", "true")
      .option("metricsPrefix", metricsPrefix)
      .load()
      .where(
        s"timestamp >= to_timestamp(1349732220) and timestamp <= to_timestamp(1572931920) and aggregation = 'average' and granularity = '5m' and id = $withMissingAggregatesId")
    // TODO: Check if this is the correct number.
    assert(df.count() == 723073)
    val pointsRead = getNumberOfRowsRead(metricsPrefix, "datapoints")
    // We read one more than strictly necessary, but it's filtered out by Spark.
    assert(pointsRead == 723074)
  }

  // TODO: Reenable this when the issue with deleting the old time series has been resolved
  it should "be possible to write datapoints to CDF using the Spark Data Source " taggedAs WriteTest ignore {
    val metricsPrefix = "datapoints.insert"
    val testUnit = "datapoints testing"
    val tsName = s"datapoints-insert-${shortRandomString()}"

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
      .option("type", "datapoints")
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
      .select(sourceTimeSeriesDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select id from destinationTimeSeries where name = '$tsName' and dataSetId = $testDataSetId""")
        .collect,
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
      .insertInto("destinationDatapoints")
    val pointsCreated = getNumberOfRowsCreated(metricsPrefix, "datapoints")
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
              |'$tsName' as externalId,
              |to_timestamp(1509500001) as timestamp,
              |double(1.5) as value,
              |null as aggregation,
              |null as granularity
      """.stripMargin)
      .write
      .insertInto("destinationDatapoints")

    val pointsAfterInsertByExternalId = getNumberOfRowsCreated(metricsPrefix, "datapoints")
    assert(pointsAfterInsertByExternalId == 2)

    // Check if post worked
    val dataPointsAfterPostByExternalId = retryWhile[Array[Row]](
      spark
        .sql(s"""select * from destinationDatapoints where id = '$id'""")
        .collect,
      df => df.length < 2)
    assert(dataPointsAfterPostByExternalId.length == 2)
  }

  it should "read all the datapoints in a time series with infrequent datapoints" taggedAs WriteTest in {

    val testUnit = "last datapoint testing"
    val tsName = "lastpointtester"

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
      .option("type", "datapoints")
      .option("collectMetrics", "true")
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
      .select(sourceTimeSeriesDf.columns.map(col): _*)
      .write
      .insertInto("destinationTimeSeries")

    // Check if post worked
    val initialDescriptionsAfterPost = retryWhile[Array[Row]](
      spark
        .sql(s"""select id from destinationTimeSeries where name = '$tsName' and dataSetId = $testDataSetId""")
        .collect,
      df => df.length < 1)
    assert(initialDescriptionsAfterPost.length == 1)

    // Insert data points that are few and far apart
    val timestamps = Seq(1422713600, 1522713600,  1575000000, 1575000001)

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
        .collect,
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
      .sql(
        s"""
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
        .collect,
      df => df.isEmpty)
      .map(r => r.getLong(0).toString)

    disableSparkLogging()
    val exception = intercept[SparkException] {
      spark
        .sql(
          s"""
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
    exception.getMessage should include ("Column 'value' was expected to have type Double")
    enableSparkLogging()
  }

  it should "fail reasonably when datapoint externalId has invalid type (save)" taggedAs WriteTest in {
    disableSparkLogging()
    val exception = intercept[SparkException] {
      spark
        .sql(
          s"""
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
        .save
    }
    exception.getMessage should include ("Column 'externalId' was expected to have type String, but value '1' of type Int was found (on row with externalId='1')")
    enableSparkLogging()
  }

  it should "fail reasonably when datapoint value has invalid type (save)" taggedAs WriteTest in {
    disableSparkLogging()
    val exception = intercept[SparkException] {
      spark
        .sql(
          s"""
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
        .save
    }
    exception.getMessage should include ("Column 'value' was expected to have type Double, but value 'non-numeric value' of type String was found (on row with id='1')")
    enableSparkLogging()
  }

  it should "fail reasonably when datapoint timestamp has invalid type (save)" taggedAs WriteTest in {
    disableSparkLogging()
    val exception = intercept[SparkException] {
      spark
        .sql(
          s"""
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
        .save
    }
    exception.getMessage should include ("Column 'timestamp' was expected to have type Timestamp, but value '1509500001' of type Int was found (on row with id='1')")
    enableSparkLogging()
  }

  //  it should "fail reasonably "

  it should "be possible to create data points for several time series at the same time" taggedAs WriteTest in {
    val tsName1 = s"dps-insert1-${shortRandomString()}"
    val tsName2 = s"dps-insert2-${shortRandomString()}"

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
      .sql(
        s"""
           |select '$tsName1' as name,
           |'$tsName1' as externalId,
           |false as isStep,
           |false as isString
     """.stripMargin)
      .union(spark
        .sql(
          s"""
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
        .sql(s"""select id from destinationTimeSeries where externalId in ('$tsName1', '$tsName2') order by externalId asc""")
        .collect,
      df => df.length < 2)
    assert(idsAfterPost.length == 2)

    val Array(id1, id2) = idsAfterPost.map(r => r.getLong(0).toString)
    spark
      .sql(
        s"""
           |select $id1 as id,
           |'this-should-be-ignored' as externalId,
           |to_timestamp(1509500001) as timestamp,
           |1.0 as value,
           |null as aggregation,
           |null as granularity
      """.stripMargin)
      .union(
        spark.sql(
          s"""
             |select $id2 as id,
             |'this-should-be-ignored' as externalId,
             |to_timestamp(1509500001) as timestamp,
             |9.0 as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin))
      .union(
        spark.sql(
          s"""
             |select null as id,
             |'$tsName1' as externalId,
             |to_timestamp(1509900001) as timestamp,
             |9.0 as value,
             |null as aggregation,
             |null as granularity
      """.stripMargin))
      .union(
        spark.sql(
          s"""
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
        .collect,
      df => df.length < 4)
    assert(dataPointsAfterPost.length == 4)
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
                   |1 as value,
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
