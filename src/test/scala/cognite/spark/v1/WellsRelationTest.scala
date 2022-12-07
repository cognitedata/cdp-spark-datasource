package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import cognite.spark.v1.wdl.{AssetSource, Well}
import org.apache.spark.sql.{DataFrame}
import org.scalatest.{FlatSpec, Inspectors, Matchers, ParallelTestExecution}

class WellsRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with Inspectors {
  val datasetWells: String = "spark-ds-wells-test"
  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "wells")
    .load()
  destinationDf.createOrReplaceTempView("destinationWells")

  ignore should "be able to write a well" taggedAs (WriteTest) in {
    val externalId = s"sparktest-${shortRandomString()}"
    val name = "test-insert"
    val description = "Created by test for spark data source"

    spark
      .sql(
        s"""select '$externalId' as externalId,
           |'$name' as name, '$description' as description""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("type", "wells")
      .option("apiKey", writeApiKey)
      .save()
  }

  it should "be able to read a well" taggedAs (ReadTest) in {
    val name = "my name"
    val description = None
    val matchingId = "my matching id"

    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .load()
//      .where(s"externalId = '$externalId'")
      .collect()

    assert(rows.length == 1)
    val well = fromRow[Well](rows.head)
    assert(well.name == name)
    assert(well.sources == Seq(AssetSource("EDM:well-1", "EDM")))
    assert(well.description == description)
    assert(well.matchingId == matchingId)
  }

  it should "be able to delete a well" taggedAs (WriteTest) in {
    val externalId = s"sparktest-${shortRandomString()}"

    spark
      .sql(s"select matchingId from destinationWells where matchingId = '$externalId'")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .option("onconflict", "delete")
      .save()
  }
}
