package cognite.spark.v1

import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1.{DMIEqualsFilter, DataModel, DataModelInstanceQuery, DataModelProperty, DataModelPropertyIndex}
import io.circe.Json
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

  private def byExternalId(modelExternalId: String, externalId: String): String =
    bluefieldAlphaClient.dataModelInstances.query(DataModelInstanceQuery(modelExternalId = modelExternalId,
      filter = Some(DMIEqualsFilter(Seq("instance", "externalId"), Json.fromString(externalId))), sort = None, limit = None))
      .unsafeRunTimed(5.minutes).get.items
      .flatMap(_.properties.flatMap(_.get("externalId")).toList).flatMap(_.asString.toList).head


  private def readRows(modelExternalId: String) = spark.read
      .format("cognite.spark.v1")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("modelExternalId", modelExternalId)
      .option("collectMetrics", true)
      .option("metricsPrefix", modelExternalId)
      .option("type", "modelinstances")
      .load()

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
          s"""select 2.0 as prop_float,
             |true as prop_bool,
             |'abc' as prop_string,
             |'first_test2' as externalId""".stripMargin))
    byExternalId(modelExternalId, "first_test2") shouldBe "first_test2"
    getNumberOfRowsUpserted(modelExternalId, "modelinstances") shouldBe 1
  }

  it should "ingest multi valued data" in {
    val modelExternalId = "Equipment_sparkDS4"

    val props = Map(
      "arr_int"-> DataModelProperty(`type`="int[]", nullable = false),
      "arr_boolean"-> DataModelProperty(`type`="boolean[]", nullable = true),
      "arr_str"-> DataModelProperty(`type`="text[]", nullable = true),
      "str_prop" -> DataModelProperty(`type`="text", nullable = true)
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
          s"""select array(1) as arr_int,
             |array(true, false) as arr_boolean,
             |NULL as arr_str,
             |NULL as str_prop,
             |'test_arr3' as externalId
             |
             |union all
             |
             |select array(1,2) as arr_int,
             |NULL as arr_boolean,
             |array('hehe') as arr_str,
             |'hehe' as str_prop,
             |'test_arr4' as externalId""".stripMargin))
    getExternalIdList(modelExternalId) should contain allOf("test_arr3", "test_arr4")
    getNumberOfRowsUpserted(modelExternalId, "modelinstances") shouldBe 2
  }

  ignore should "read instances" in {
    val modelExternalId = "Equipment-0de0774f"
    val df = readRows(modelExternalId)
    df.count() shouldBe 2
    getNumberOfRowsRead(modelExternalId, "modelinstances") shouldBe 2
  }

}