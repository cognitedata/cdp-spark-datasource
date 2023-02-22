package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, SparkTest}
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLNptsTest
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

  before {
    testClient.deleteAll()
    testClient.miniSetup()
  }

  it should "ingest and read NTP events" in {
    val testNptIngestionsDF = spark.read
      .schema(testClient.getSchema("NptIngestion"))
      .json("src/test/resources/wdl-test-ntp-ingestions.jsonl")

    testNptIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "NptIngestion")
      .useOIDCWrite
      .save()

    val testNptsDF = spark.read
      .schema(testClient.getSchema("Npt"))
      .json("src/test/resources/wdl-test-ntps.jsonl")

    val NptsDF = sparkReader
      .option("wdlDataType", "Npt")
      .load()

    (testNptsDF.collect() should contain).theSameElementsAs(NptsDF.collect())
  }
}
