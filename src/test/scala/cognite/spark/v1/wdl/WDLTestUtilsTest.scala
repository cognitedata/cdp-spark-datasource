package cognite.spark.v1.wdl

import cognite.spark.v1.{WDLSparkTest}
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors, Matchers}

class WDLTestUtilsTest
    extends FlatSpec
    with Matchers
    with WDLSparkTest
    with Inspectors
    with BeforeAndAfter {

  import RowEquality._
  import spark.implicits._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  before {
    client.deleteAll()
  }

  it should "setup wells and wellbores with miniSetup" in {
    val miniSetup: client.MiniSetup = client.miniSetup()

    val ingestedWell = miniSetup.well.copy(wellbores = Some(miniSetup.wellbores))
    val wellJsonString = ingestedWell.asJson.spaces2SortKeys
    val testWellDS = spark.createDataset(Seq(wellJsonString))
    val testWellDF = spark.read
      .option("multiline", value = true)
      .schema(client.getSchema("Well"))
      .json(testWellDS)

    val wellsDF = sparkReader
      .option("wdlDataType", "Well")
      .load()

    (testWellDF.collect() should contain).theSameElementsAs(wellsDF.collect())
  }

  it should "ingest and delete sources" in {
    // Create new source
    client.ingestSources(Seq(Source(name = "my-test")))
    val sources2 = client.getSources
    assert(sources2.map(_.name) == Seq("my-test"))

    client.setMergeRules(Seq("my-test"))

    // Delete the source
    client.deleteSources(Seq("my-test"))
    val sources3 = client.getSources
    assert(sources3 == Seq())
  }
}
