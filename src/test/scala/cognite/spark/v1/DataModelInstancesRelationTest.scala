package cognite.spark.v1

import com.cognite.sdk.scala.v1.DataModelInstanceQuery
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class DataModelInstancesRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"

  def insertRows(modelExternalId: String, df: DataFrame, onconflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", "mappinginstances")
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
      filter = None, sort = None, limit = None)).unsafeRunSync().items
    resp.size shouldBe 1
    getNumberOfRowsCreated(modelExternalId, "mappinginstances") shouldBe 1
  }

}