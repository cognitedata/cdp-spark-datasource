package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, SparkTest}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLRDDCursorLimitTest
    extends FlatSpec
    with SparkTest
    with Inspectors
    with DataFrameMatcher
    with BeforeAndAfter {

  import RowEquality._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  val testClient = new TestWdlClient(writeClient)

  private val testNptIngestionsDF = spark.read
    .schema(testClient.getSchema("NptIngestion"))
    .json("src/test/resources/wdl-test-ntp-ingestions.jsonl")

  private val testNptsDF = spark.read
    .schema(testClient.getSchema("Npt"))
    .json("src/test/resources/wdl-test-ntps.jsonl")

  before {
    SQLConf.get.setConfString("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
    testClient.deleteAll()
    testClient.miniSetup()
  }

  it should "read 0 NTP events with limit 1" in {
    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "1")
      .load()

    NptsDF.isEmpty shouldBe true
  }

  it should "read 1 NTP event with limit 1" in {
    testNptIngestionsDF
      .limit(1)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "1")
      .load()

    (testNptsDF.limit(1).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 1 NTP event with limit 2" in {
    testNptIngestionsDF
      .limit(1)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "2")
      .load()

    (testNptsDF.limit(1).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 1 NTP event without limit" in {
    testNptIngestionsDF
      .limit(1)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .load()

    (testNptsDF.limit(1).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 2 NTP events with limit 1" in {
    testNptIngestionsDF
      .limit(2)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "1")
      .load()

    (testNptsDF.limit(2).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 2 NTP events with limit 2" in {
    testNptIngestionsDF
      .limit(2)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "2")
      .load()

    (testNptsDF.limit(2).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 2 NTP events with limit 3" in {
    testNptIngestionsDF
      .limit(2)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "3")
      .load()

    (testNptsDF.limit(2).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 2 NTP events without limit" in {
    testNptIngestionsDF
      .limit(2)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .load()

    (testNptsDF.limit(2).collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 3 NTP events with limit 1" in {
    testNptIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "1")
      .load()

    (testNptsDF.collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 3 NTP events with limit 2" in {
    testNptIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "2")
      .load()

    (testNptsDF.collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 3 NTP events with limit 3" in {
    testNptIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "3")
      .load()

    (testNptsDF.collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 3 NTP events with limit 4" in {
    testNptIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .option("batchSize", "4")
      .load()

    (testNptsDF.collect() should contain).theSameElementsAs(NptsDF.collect())
  }

  it should "read 3 NTP events without limit" in {
    testNptIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .load()

    (testNptsDF.collect() should contain).theSameElementsAs(NptsDF.collect())
  }
}
