package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import cognite.spark.v1.wdl.{AssetSource, Well, WellIngestion, Wellhead}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class WellsRelationTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with Inspectors {

  import spark.implicits._

  val datasetWells: String = "spark-ds-wells-test"
  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "wells")
    .load()
  destinationDf.createOrReplaceTempView("destinationWells")

  private var testWells = Vector[Well](
    Well(
      matchingId = "my matching id",
      name = "my name",
      wellhead = Wellhead(0.1, 10.1, "CRS"),
      sources = Seq(AssetSource("EDM:well-1", "EDM")),
    ),
  )

  it should "be able to insert a well" taggedAs (WriteTest) in {
    val oldWell = testWells.head
    val newWell = oldWell.copy(
      matchingId = "new matchingId",
      sources = Seq(AssetSource("Petrel:well-2", "Petrel")),
    )
    val newWellIngestion = WellIngestion(
      matchingId = Some(newWell.matchingId),
      name = newWell.name,
      wellhead = Some(newWell.wellhead),
      source = newWell.sources.head,
    )
    val newWellsDF = Seq(newWellIngestion).toDF()

    newWellsDF
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "wells")
      .option("onconflict", "upsert")
      .save()

    testWells = testWells ++ Seq(newWell)
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
    wells should contain theSameElementsAs testWells.take(1)
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
    val sources = spark
      .sql(s"select inline(sources) from destinationWells")
      .as[AssetSource]
      .collect()

    val testSources = testWells.flatMap(well => well.sources)
    sources should contain theSameElementsAs testSources
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
