package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, SparkTest}
import com.cognite.sdk.scala.playground._
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLWellSourcesTest
    extends FlatSpec
    with SparkTest
    with Inspectors
    with DataFrameMatcher
    with BeforeAndAfter {

  import cognite.spark.v1.CdpConnector._

  val testClient = new TestWdlClient(writeClient)
  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  before {
    testClient.deleteAll()
  }

  it should "ingest and read well sources" in {
    {
      for {
        _ <- writeClient.wdl.sources.create(Seq(Source("EDM")))
        _ <- writeClient.wdl.wells.setMergeRules(WellMergeRules(Seq("EDM")))
        _ <- writeClient.wdl.wellbores.setMergeRules(WellboreMergeRules(Seq("EDM")))
      } yield ()
    }.unsafeRunSync()

    val df = spark.read
      .schema(testClient.getSchema("WellSource"))
      .json("src/test/resources/wdl-test-well-sources.jsonl")

    df.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "WellSource")
      .useOIDCWrite
      .save()

    val columns = Seq("name", "wellhead", "waterDepth", "field", "source")

    val dfExpected = df.selectExpr(columns: _*)

    val wellSources = sparkReader
      .option("wdlDataType", "WellSource")
      .load()
      // Only check the columns we are interested in.
      .selectExpr(columns: _*)

    (wellSources.collect() should contain).theSameElementsAs(dfExpected.collect())
  }
}
