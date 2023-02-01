package cognite.spark.v1.wdl

import cognite.spark.v1.SparkTest
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors, Matchers}

class WDLTestUtilsTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with Inspectors
    with BeforeAndAfter {

  import RowEquality._
  import spark.implicits._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  val testClient: TestWdlClient = new TestWdlClient(writeClient)

  before {
    testClient.deleteAll()
  }

  it should "setup wells and wellbores with miniSetup" in {
    val miniSetup: testClient.MiniSetup = testClient.miniSetup()

    val ingestedWell = miniSetup.well.copy(wellbores = Some(miniSetup.wellbores))
    val wellJsonString = ingestedWell.asJson.spaces2SortKeys
    val testWellDS = spark.createDataset(Seq(wellJsonString))
    val testWellDF = spark.read
      .option("multiline", value = true)
      .schema(testClient.getSchema("Well"))
      .json(testWellDS)

    val wellsDF = sparkReader
      .option("wdlDataType", "Well")
      .load()

    (testWellDF.collect() should contain).theSameElementsAs(wellsDF.collect())
  }
}
