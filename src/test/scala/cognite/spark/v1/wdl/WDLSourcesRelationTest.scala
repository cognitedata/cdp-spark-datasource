package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfSparkAuth, DataFrameMatcher, WDLSparkTest}
import com.cognite.sdk.scala.common.ApiKeyAuth
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLSourcesRelationTest
    extends FlatSpec
    with WDLSparkTest
    with Inspectors
    with DataFrameMatcher
      with BeforeAndAfter {

  import RowEquality._
  import spark.implicits._

  val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("apiKey", writeApiKey)
    .option("type", "wdl")

  val sourcesDF = sparkReader
    .option("wdlDataType", "Source")
    .load()
  sourcesDF.createOrReplaceTempView("wdl_test_sources")

  private val config = getDefaultConfig(CdfSparkAuth.Static(ApiKeyAuth(writeApiKey)))
  private val client = new TestWdlClient(config)

  before {
    SQLConf.get.setConfString("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
    client.deleteAll()
  }

  it should "setup wells and wellbores with miniSetup" in {
    val miniSetup: client.MiniSetup = client.miniSetup()

    val ingestedWell = miniSetup.well.copy(wellbores = Some(Seq(miniSetup.wellbore)))
    val wellJsonString = ingestedWell.asJson.spaces2SortKeys
    val testWellDS = spark.createDataset(Seq(wellJsonString))
    val testWellDF = spark
      .read
      .option("multiline", value = true)
      .schema(client.getSchema("Well"))
      .json(testWellDS)

    client.getSchema("Well").printTreeString()

    testWellDF.printSchema()
    testWellDF.show(truncate = false)

    val wellsDF = sparkReader
      .option("wdlDataType", "Well")
      .load()

    wellsDF.show(truncate = false)
    wellsDF.printSchema()

    testWellDF.collect() should contain theSameElementsAs wellsDF.collect()
  }

  it should "read JSONL and write as DataFrame" in {
    val testSourcesJSONL = Seq(
      """{"name": "EDM", "description": null}""",
      """{"name": "VOLVE", "description": "VOLVE SOURCE"}""",
      """{"name": "test_source", "description": "For testing merging with 3 sources"}""",
    )

    val testSourcesDS = spark.createDataset(testSourcesJSONL)
    val testSourcesDF = spark.read.json(testSourcesDS)
    testSourcesDF.show()
    testSourcesDF.printSchema()

    testSourcesDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "wdl")
      .option("wdlDataType", "Source")
      .option("apiKey", writeApiKey)
      .save()

    sourcesDF.show()

    testSourcesDF.collect() should contain theSameElementsAs sourcesDF.collect()

//    testSourcesDF should containTheSameRowsAs(destinationDf)
  }
}
