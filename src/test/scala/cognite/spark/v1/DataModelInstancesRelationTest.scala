package cognite.spark.v1

import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1._
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

  private def listInstances(modelExternalId: String, filter: Option[DataModelInstanceFilter] = None): Seq[DataModelInstanceQueryResponse] =
    bluefieldAlphaClient.dataModelInstances.query(DataModelInstanceQuery(modelExternalId = modelExternalId,
      filter = filter, sort = None, limit = None))
      .unsafeRunTimed(5.minutes).get.items

  private def getExternalIdList(modelExternalId: String): Seq[String] =
    listInstances(modelExternalId).flatMap(_.properties.flatMap(_.get("externalId")).toList).flatMap(_.asString.toList)

  private def byExternalId(modelExternalId: String, externalId: String): String =
    listInstances(modelExternalId, filter = Some(DMIEqualsFilter(Seq("instance", "externalId"), Json.fromString(externalId))))
      .flatMap(_.properties.flatMap(_.get("externalId")).toList).flatMap(_.asString.toList).head

  private val multiValuedExtId = "Equipment_sparkDS5"
  private val primitiveExtId = "Equipment_sparkDS6"

  private val props = Map(
    "arr_int"-> DataModelProperty(`type`="int[]", nullable = false),
    "arr_boolean"-> DataModelProperty(`type`="boolean[]", nullable = true),
    "arr_str"-> DataModelProperty(`type`="text[]", nullable = true),
    "str_prop" -> DataModelProperty(`type`="text", nullable = true)
  )
  private val props2 = Map(
    "prop_float"-> DataModelProperty(`type`="float64", nullable = true),
    "prop_bool"-> DataModelProperty(`type`="boolean", nullable = true),
    "prop_string"-> DataModelProperty(`type`="text", nullable = true)
  )

  bluefieldAlphaClient.dataModels.createItems(
    Items(Seq(
      DataModel(externalId = multiValuedExtId, properties = Some(props)),
      DataModel(externalId = primitiveExtId, properties = Some(props2))
    )))
    .unsafeRunTimed(5.minutes).get

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
      .option("type", "datamodelinstances")
      .load()

  def insertRows(modelExternalId: String, df: DataFrame, onconflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", "datamodelinstances")
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
    insertRows(
      primitiveExtId,
      spark
        .sql(
          s"""select 2.0 as prop_float,
             |true as prop_bool,
             |'abc' as prop_string,
             |'prim_test' as externalId""".stripMargin))
    byExternalId(primitiveExtId, "prim_test") shouldBe "prim_test"
    getNumberOfRowsUpserted(primitiveExtId, "datamodelinstances") shouldBe 1
  }

  it should "ingest multi valued data" in {


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
      multiValuedExtId,
      spark
        .sql(
          s"""select array(1) as arr_int,
             |array(true, false) as arr_boolean,
             |NULL as arr_str,
             |NULL as str_prop,
             |'test_multi' as externalId
             |
             |union all
             |
             |select array(1,2) as arr_int,
             |NULL as arr_boolean,
             |array('hehe') as arr_str,
             |'hehe' as str_prop,
             |'test_multi2' as externalId""".stripMargin))
    getExternalIdList(multiValuedExtId) should contain allOf("test_multi", "test_multi2")
    getNumberOfRowsUpserted(multiValuedExtId, "datamodelinstances") shouldBe 2
  }

  it should "read instances" in {
    val df = readRows(primitiveExtId)
    df.limit(1).count() shouldBe 1
    getNumberOfRowsRead(primitiveExtId, "datamodelinstances") shouldBe 1
  }

  it should "read multi valued instances" in {
    val df = readRows(multiValuedExtId)
    df.limit(2).count() shouldBe 2
    getNumberOfRowsRead(multiValuedExtId, "datamodelinstances") shouldBe 2
  }

  ignore should "query instances by externalId" in {
    val df = readRows(primitiveExtId)
    df.where("externalId = 'prim_test'").count() shouldBe 1
    getNumberOfRowsRead(primitiveExtId, "datamodelinstances") shouldBe 1
  }
}
