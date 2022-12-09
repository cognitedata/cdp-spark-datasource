package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import cognite.spark.v1.wdl.{AssetSource, Well, Wellhead}
import org.apache.spark.sql.DataFrame
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

  private val testWells = Vector[Well](
    Well(
      matchingId = "my matching id",
      name = "my name",
      wellhead = Wellhead(0.1, 10.1, "CRS"),
      sources = Seq(AssetSource("EDM:well-1", "EDM")),
    ),
  )

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

  it should "be able to read all wells" taggedAs (ReadTest) in {
    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .load()
      .collect()

    val wells = rows.map(r => fromRow[Well](r))
    wells should contain theSameElementsAs testWells
  }

  it should "be able to query well by matchingId" taggedAs (ReadTest) in {
    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .load()
      .where(s"matchingId = '${testWells.head.matchingId}'")
      .collect()

    val wells = rows.map(r => fromRow[Well](r))
    wells should contain theSameElementsAs testWells
  }

  it should "be able to query non-existent well by matchingId" taggedAs (ReadTest) in {
    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .load()
      .where(s"matchingId = '123'")
      .collect()

    rows shouldBe empty
  }

  it should "be able to query columns from all wells" taggedAs (ReadTest) in {
    spark
      .sql(s"select inline(sources) from destinationWells")
      .show

//    val rows = spark
//      .sql(s"select explode(sources) from destinationWells")
//      .collect()
//
//    val sources = rows.map(r => fromRow[AssetSource](r))
//    sources should contain theSameElementsAs testWells
  }

  it should "be able to delete a well" taggedAs (WriteTest) in {
    spark
      .sql(s"select inline(sources) from destinationWells")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .option("onconflict", "delete")
      .save()

    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .load()
      .collect()

    rows shouldBe empty
  }
}
