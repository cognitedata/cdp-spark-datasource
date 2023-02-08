package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, SparkTest}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfterAll, FunSpec, Inspectors}

class WDLRDDCursorLimitTest
    extends FunSpec
    with SparkTest
    with Inspectors
    with DataFrameMatcher
    with BeforeAndAfterAll {

  import RowEquality._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  val testClient = new TestWdlClient(writeClient)

  private val testNptIngestionsDF = spark.read
    .schema(testClient.getSchema("NptIngestion"))
    .json("src/test/resources/wdl-test-ntp-ingestions.jsonl")

  private val testNptsDF = spark.read
    .schema(testClient.getSchema("Npt"))
    .json("src/test/resources/wdl-test-ntps.jsonl")

  override protected def beforeAll(): Unit = {
    SQLConf.get.setConfString("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
    testClient.deleteAll()
    testClient.miniSetup()
    ()
  }

  for (numberOfEvents <- 0 to 3) {
    describe(s"Should write $numberOfEvents NTP event(s)") {
      it("successfully") {
        testNptIngestionsDF
          .limit(numberOfEvents)
          .write
          .format("cognite.spark.v1")
          .option("project", "jetfiretest2")
          .option("type", "welldatalayer")
          .option("wdlDataType", "NptIngestion")
          .useOIDCWrite
          .save()
      }

      for (limit <- 1 to numberOfEvents + 1) {
        it(s"and read them back with limit $limit") {
          val NptsDF = sparkReader
            .option("wdlDataType", "Npt")
            .option("batchSize", s"$limit")
            .load()

          (testNptsDF.limit(numberOfEvents).collect() should contain)
            .theSameElementsAs(NptsDF.collect())
        }
      }

      it(s"without limit") {
        val NptsDF = sparkReader
          .option("wdlDataType", "Npt")
          .load()

        (testNptsDF.limit(numberOfEvents).collect() should contain)
          .theSameElementsAs(NptsDF.collect())
      }
    }
  }
}
