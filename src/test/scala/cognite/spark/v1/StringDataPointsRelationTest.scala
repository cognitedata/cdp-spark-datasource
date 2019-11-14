package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
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
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("limitPerPartition", "100")
      .load()
      .where(s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1552604342) and id = $valhallTimeSeriesId")
    assert(df.count() == 100)

    val df2 = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("limitPerPartition", "100")
      .load()
      .where(s"timestamp <= to_timestamp(1552604342) and id = $valhallTimeSeriesId")
    assert(df2.count() == 100)
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
    val df = spark.read.format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "stringdatapoints")
      .option("partitions", "1")
      .load()
      .where(s"timestamp >= to_timestamp(1395666380) and timestamp <= to_timestamp(1395892205) and id = $valhallTimeSeriesId")
    assert(df.count() == 9)
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

    spark.sparkContext.setLogLevel("OFF") // Removing expected Spark executor Errors from the console
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
    spark.sparkContext.setLogLevel("WARN")
  }
}
