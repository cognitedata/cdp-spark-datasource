package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, WDLSparkTest}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Inspectors}

class WDLSourcesRelationTest
    extends FlatSpec
    with WDLSparkTest
    with Inspectors
    with DataFrameMatcher {

  import spark.implicits._

  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("apiKey", writeApiKey)
    .option("type", "wdl")
    .option("wdlDataType", "Source")
    .load()
  destinationDf.createOrReplaceTempView("wdl_test")

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

    testSourcesDF should containTheSameRowsAs(destinationDf)
  }
}
