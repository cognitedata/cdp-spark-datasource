package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, SparkTest}
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLSourcesTest
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

  ignore should "ingest and read Sources" in {
    val testSourcesDF = spark.read
      .schema(testClient.getSchema("Source"))
      .json("src/test/resources/wdl-test-sources.jsonl")

    testSourcesDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "Source")
      .useOIDCWrite
      .save()

    val sourcesDF = sparkReader
      .option("wdlDataType", "Source")
      .load()
      .select("name", "description")

    val expectedSources = spark.read
      .schema(testClient.getSchema("Source"))
      .json("src/test/resources/wdl-test-expected-sources.jsonl")
      .select("name", "description")

    (expectedSources.collect() should contain).theSameElementsAs(sourcesDF.collect())
  }
}
