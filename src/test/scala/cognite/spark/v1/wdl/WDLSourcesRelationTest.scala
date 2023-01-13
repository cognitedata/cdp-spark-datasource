package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfSparkAuth, DataFrameMatcher, WDLSparkTest}
import com.cognite.sdk.scala.common.ApiKeyAuth
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import org.apache.spark.sql.Row
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

private[wdl] object rowEquality { // scalastyle:ignore object.name
  implicit val rowEq: Equality[Row] =
    new Equality[Row] {
      def areEqual(a: Row, b: Any): Boolean =
        b match {
          case p: Row =>
            val fieldNames = a.schema.fieldNames
            if (fieldNames.toSet == p.schema.fieldNames.toSet) {
              a.getValuesMap(fieldNames) == p.getValuesMap(fieldNames)
            } else {
              false
            }
          case _ => false
        }
    }
}

class WDLSourcesRelationTest
    extends FlatSpec
    with WDLSparkTest
    with Inspectors
    with DataFrameMatcher
      with BeforeAndAfter {

  import rowEquality._
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
    client.deleteAll()
  }

  it should "setup wells and wellbores with miniSetup" in {
    val miniSetup: client.MiniSetup = client.miniSetup()

    val ingestedWell = miniSetup.well.copy(wellbores = Some(Seq(miniSetup.wellbore)))
    val wellJsonString = ingestedWell.asJson.spaces2SortKeys
    val testWellDS = spark.createDataset(Seq(wellJsonString))
    val testWellDF = spark
      .read
      .option("multiline", "true")
      .schema(client.getSchema("Well"))
      .json(testWellDS)

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
