package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfSparkAuth, DataFrameMatcher, WDLSparkTest}
import com.cognite.sdk.scala.common.ApiKeyAuth
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLSourcesTest
    extends FlatSpec
    with WDLSparkTest
    with Inspectors
    with DataFrameMatcher
    with BeforeAndAfter {

  import RowEquality._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("apiKey", writeApiKey)
    .option("type", "welldatalayer")

  private val config = getDefaultConfig(CdfSparkAuth.Static(ApiKeyAuth(writeApiKey)))
  private val client = new TestWdlClient(WdlClient.fromConfig(config))

  before {
    SQLConf.get.setConfString("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
    client.deleteAll()
    client.miniSetup()
  }

  it should "ingest and read Sources" in {
    val testSourcesDF = spark.read
      .schema(client.getSchema("Source"))
      .json("src/test/resources/wdl-test-sources.jsonl")

    testSourcesDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "Source")
      .option("apiKey", writeApiKey)
      .save()

    val sourcesDF = sparkReader
      .option("wdlDataType", "Source")
      .load()

    val expectedSources = spark.read
      .schema(client.getSchema("Source"))
      .json("src/test/resources/wdl-test-expected-sources.jsonl")
    (expectedSources.collect() should contain).theSameElementsAs(sourcesDF.collect())
  }
}
