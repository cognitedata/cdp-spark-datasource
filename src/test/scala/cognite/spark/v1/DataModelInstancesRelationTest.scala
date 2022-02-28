package cognite.spark.v1

import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1.{DataModel, DataModelInstanceQuery, DataModelProperty, DataModelPropertyIndex}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

import scala.concurrent.duration.DurationInt

class DataModelInstancesRelationTest extends FlatSpec with Matchers with SparkTest {
  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  import CdpConnector.ioRuntime

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
             |'first_test' as externalId""".stripMargin))
    val resp = bluefieldAlphaClient.dataModelInstances.query(DataModelInstanceQuery(modelExternalId = modelExternalId,
      filter = None, sort = None, limit = None)).unsafeRunTimed(5.minutes).get.items
    resp.size should be >= 1
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
     Items(Seq(DataModel(externalId = modelExternalId, properties = Some(props))))).unsafeRunTimed(5.minutes).get

    insertRows(
      modelExternalId,
      spark
        .sql(
          s"""select array(1,2) as arr_int,
             |array('emel', 'test') as arr_str,
             |array(true) as arr_boolean,
             |'test' as str_prop,
             |'test_arr1' as externalId""".stripMargin))

//    insertRows(
//      modelExternalId,
//      spark
//        .sql(
//          s"""select array(1,2) as arr_int,
//             |array(true) as arr_boolean,
//             |array('emel', 'test') as arr_str,
//             |'test' as str_prop,
//             |'test_arr2' as externalId
//             |union
//             |select array(1,4) as arr_int,
//             |array(true, false) as arr_boolean,
//             |null as arr_str,
//             |null as str_prop,
//             |'test_arr3' as externalId""".stripMargin))
    val resp = bluefieldAlphaClient.dataModelInstances.query(DataModelInstanceQuery(modelExternalId = modelExternalId,
      filter = None, sort = None, limit = None)).unsafeRunTimed(5.minutes).get.items
    resp.size should be >= 1
    getNumberOfRowsUpserted(modelExternalId, "modelinstances") shouldBe 1
  }

}