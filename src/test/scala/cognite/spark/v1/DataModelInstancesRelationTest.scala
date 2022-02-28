package cognite.spark.v1

import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1.{DataModel, DataModelInstanceQuery, DataModelProperty, DataModelPropertyIndex}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.duration.DurationInt

class DataModelInstancesRelationTest extends FlatSpec with Matchers with SparkTest {
  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  import CdpConnector.ioRuntime

  private def getExternalIdList(modelExternalId: String): Seq[String] =
    bluefieldAlphaClient.dataModelInstances.query(DataModelInstanceQuery(modelExternalId = modelExternalId,
      filter = None, sort = None, limit = None))
      .unsafeRunTimed(5.minutes).get.items
      .flatMap(_.properties.flatMap(_.get("externalId")).toList).flatMap(_.asString.toList)

  def insertRows(modelExternalId: String, df: DataFrame, onconflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", "modelinstances")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("modelExternalId", modelExternalId)
      .option("onconflict", onconflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", modelExternalId)
      .save

  it should "ingest data" in {
    val modelExternalId = "Equipment-0de0774f"
    insertRows(
      modelExternalId,
      spark
        .sql(
          s"""select 1.0 as prop_float,
             |true as prop_bool,
             |'abc' as prop_string,
             |'first_test2' as externalId""".stripMargin))
    getExternalIdList(modelExternalId) should contain("first_test")
    getNumberOfRowsUpserted(modelExternalId, "modelinstances") shouldBe 1
  }

  it should "ingest multi valued data" in {
    val modelExternalId = "Equipment_sparkDS4"

    val props = Map(
      "arr_int"-> DataModelProperty(`type`="int[]", nullable = Some(false)),
      "arr_boolean"-> DataModelProperty(`type`="boolean[]", nullable = Some(true)),
      "arr_str"-> DataModelProperty(`type`="text[]", nullable = Some(true)),
      "str_prop" -> DataModelProperty(`type`="text", nullable = Some(true))
    )
   bluefieldAlphaClient.dataModels.createItems(
     Items(Seq(DataModel(externalId = modelExternalId, properties = Some(props)))))
     .unsafeRunTimed(5.minutes).get

    val tsDf = spark.read
      .format("cognite.spark.v1")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("type", "timeseries")
      .load()
    tsDf.createOrReplaceTempView("timeSeries")

    insertRows(
      modelExternalId,
      spark
        .sql(
          s"""select array(int(t.id)) as arr_int,
             |array(true, false) as arr_boolean,
             |null as arr_str,
             |null as str_prop,
             |'test_arr3' as externalId
             |from timeSeries t
             |limit 1""".stripMargin))
    getExternalIdList(modelExternalId) should contain("test_arr3")
    getNumberOfRowsUpserted(modelExternalId, "modelinstances") shouldBe 1
  }

}