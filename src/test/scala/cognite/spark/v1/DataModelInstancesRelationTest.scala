package cognite.spark.v1

import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1._
import io.circe.Json
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.DurationInt

class DataModelInstancesRelationTest extends FlatSpec with Matchers with SparkTest {
  import CdpConnector.ioRuntime

  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val bluefieldAlphaClient = getBlufieldClient(Some("alpha"))

  private def listInstances(modelExternalId: String,
      filter: Option[DataModelInstanceFilter] = None): Seq[DataModelInstanceQueryResponse] =
    bluefieldAlphaClient.dataModelInstances.query(DataModelInstanceQuery(modelExternalId = modelExternalId,
      filter = filter, sort = None, limit = None))
      .unsafeRunTimed(30.seconds).get.items

  private def getExternalIdList(modelExternalId: String): Seq[String] =
    listInstances(modelExternalId).flatMap(_.properties.flatMap(_.get("externalId")).toList).flatMap(_.asString.toList)

  private def byExternalId(modelExternalId: String, externalId: String): String =
    listInstances(modelExternalId, filter = Some(DMIEqualsFilter(Seq("instance", "externalId"),
      Json.fromString(externalId))))
      .flatMap(_.properties.flatMap(_.get("externalId")).toList).flatMap(_.asString.toList).head

  private val multiValuedExtId = "Equipment_sparkDS5"
  private val primitiveExtId = "Equipment_sparkDS6"
  private val multiValuedExtId2 = "Equipment_sparkDS7"

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
  private val props3 = Map(
    "prop_int32"-> DataModelProperty(`type`="int32", nullable = false),
    "prop_int64"-> DataModelProperty(`type`="int64", nullable = false),
    "prop_float32"-> DataModelProperty(`type`="float32", nullable = true),
    "prop_float64" -> DataModelProperty(`type`="float64", nullable = true),
    "prop_numeric" -> DataModelProperty(`type`="numeric", nullable = true),
    "arr_int32"-> DataModelProperty(`type`="int32[]", nullable = false),
    "arr_int64"-> DataModelProperty(`type`="int64[]", nullable = false),
    "arr_float32"-> DataModelProperty(`type`="float32[]", nullable = true),
    "arr_float64" -> DataModelProperty(`type`="float64[]", nullable = true),
    "arr_numeric" -> DataModelProperty(`type`="numeric[]", nullable = true)
  )

  bluefieldAlphaClient.dataModels.createItems(
    Items(Seq(
      DataModel(externalId = multiValuedExtId, properties = Some(props)),
      DataModel(externalId = primitiveExtId, properties = Some(props2)),
      DataModel(externalId = multiValuedExtId2, properties = Some(props3))
    )))
    .unsafeRunTimed(30.seconds).get

  private def collectExternalIds(df: DataFrame): List[String] = df.select("externalId")
    .collect().map(_.getAs[String]("externalId")).toList

  private def readRows(modelExternalId: String, metricPrefix: String) = spark.read
      .format("cognite.spark.v1")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("modelExternalId", modelExternalId)
      .option("collectMetrics", true)
      .option("metricsPrefix", metricPrefix)
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
    bluefieldAlphaClient.dataModelInstances.deleteByExternalId("prim_test").unsafeRunSync()
  }

  it should "ingest multi valued data" in {
    insertRows(
      multiValuedExtId,
      spark
        .sql(
          s"""select array() as arr_int,
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
    bluefieldAlphaClient.dataModelInstances.deleteByExternalIds(Seq("test_multi2", "test_multi")).unsafeRunSync()
  }

  it should "read instances" in {
    insertRows(
      primitiveExtId,
      spark
        .sql(
          s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'prim_test2' as externalId""".stripMargin))

    val metricPrefix = shortRandomString()
    val df = readRows(primitiveExtId, metricPrefix)
    df.limit(1).count() shouldBe 1
    getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
    bluefieldAlphaClient.dataModelInstances.deleteByExternalId("prim_test2").unsafeRunSync()
  }

  it should "read multi valued instances" in {
    insertRows(
      multiValuedExtId2,
      spark
        .sql(
          s"""select 1234 as prop_int32,
             |4398046511104 as prop_int64,
             |0.424242 as prop_float32,
             |0.424242 as prop_float64,
             |1.00000000001 as prop_numeric,
             |array(1,2,3) as arr_int32,
             |array(1,2,3) as arr_int64,
             |array(0.618, 1.618) as arr_float32,
             |array(0.618, 1.618) as arr_float64,
             |array(1.00000000001) as arr_numeric,
             |'numeric_test' as externalId
             |
             |union all
             |
             |select 1234 as prop_int32,
             |4398046511104 as prop_int64,
             |NULL as prop_float32,
             |NULL as prop_float64,
             |1.00000000001 as prop_numeric,
             |array(1,2,3) as arr_int32,
             |array(1,2,3) as arr_int64,
             |array(0.618, 1.618) as arr_float32,
             |NULL as arr_float64,
             |array(1.00000000001) as arr_numeric,
             |'numeric_test2' as externalId""".stripMargin
        )
    )

    val metricPrefix = shortRandomString()
    val df = readRows(multiValuedExtId2, metricPrefix)
    df.limit(1).count() shouldBe 1
    getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
    df.select("externalId").collect()
      .map(_.getAs[String]("externalId")).toList should contain allOf("numeric_test", "numeric_test2")
    getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 3
    bluefieldAlphaClient.dataModelInstances.deleteByExternalIds(Seq("numeric_test", "numeric_test2")).unsafeRunSync()
  }

  it should "fail when writing null to a non nullable property" in {
    val ex = sparkIntercept {
      insertRows(
      multiValuedExtId,
      spark
        .sql(
          s"""select NULL as arr_int,
             |array(true, false) as arr_boolean,
             |NULL as arr_str,
             |NULL as str_prop,
             |'test_multi' as externalId""".stripMargin))
    }
    ex shouldBe an[CdfSparkException]
    ex.getMessage shouldBe s"Property of int[] type is not nullable."
  }

  it should "filter instances by externalId" in {
    insertRows(
      primitiveExtId,
      spark
        .sql(
          s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'prim_test' as externalId""".stripMargin))
    val metricPrefix = shortRandomString()
    val df = readRows(primitiveExtId, metricPrefix)
    df.where("externalId = 'prim_test'").count() shouldBe 1
     getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
    bluefieldAlphaClient.dataModelInstances.deleteByExternalId("prim_test").unsafeRunSync()
  }

  it should "filter instances" in {
    insertRows(
      multiValuedExtId2,
      spark
        .sql(
          s"""select 1234 as prop_int32,
             |4398046511104 as prop_int64,
             |0.424242 as prop_float32,
             |0.8 as prop_float64,
             |2.0 as prop_numeric,
             |array(1,2,3) as arr_int32,
             |array(1,2,3) as arr_int64,
             |array(0.618, 1.618) as arr_float32,
             |array(0.618, 1.618) as arr_float64,
             |array(1.00000000001) as arr_numeric,
             |'numeric_test' as externalId
             |
             |union all
             |
             |select 1234 as prop_int32,
             |4398046511104 as prop_int64,
             |NULL as prop_float32,
             |NULL as prop_float64,
             |1.00000000001 as prop_numeric,
             |array(1,2,3) as arr_int32,
             |array(1,2,3) as arr_int64,
             |array(0.618, 1.618) as arr_float32,
             |NULL as arr_float64,
             |array(1.00000000001) as arr_numeric,
             |'numeric_test2' as externalId""".stripMargin
        )
    )

    val metricPrefix = shortRandomString()
    val df = readRows(multiValuedExtId2, metricPrefix)
    val andDf = df.where("prop_numeric > 1.5 and prop_float64 = 0.8")
    andDf.count() shouldBe 1
    getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
    collectExternalIds(andDf) should contain only "numeric_test"

    val metricPrefix2 = shortRandomString()
    val df2 = readRows(multiValuedExtId2, metricPrefix2)
      .where("not (prop_numeric > 1.5 and prop_float64 >= 0.7)")
    df2.count() shouldBe 1
    getNumberOfRowsRead(metricPrefix2, "datamodelinstances") shouldBe 1
    collectExternalIds(df2) should contain only "numeric_test2"

    val metricPrefix3 = shortRandomString()
    val df3 = readRows(multiValuedExtId2, metricPrefix3).where("prop_float32 is not null")
    df3.count() shouldBe 1
    getNumberOfRowsRead(metricPrefix3, "datamodelinstances") shouldBe 1
    collectExternalIds(df3) should contain only "numeric_test"

    bluefieldAlphaClient.dataModelInstances.deleteByExternalIds(
      Seq("numeric_test", "numeric_test2")).unsafeRunSync()
  }


  it should "filter instances using or" in {
    insertRows(
      primitiveExtId,
      spark
        .sql(
          s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'prim_test' as externalId
             |
             |union all
             |
             |select 5.0 as prop_float,
             |true as prop_bool,
             |'zzzz' as prop_string,
             |'prim_test2' as externalId
             |
             |union all
             |
             |select 9.0 as prop_float,
             |false as prop_bool,
             |'xxxx' as prop_string,
             |'prim_test3' as externalId
             |
             |union all
             |
             |select 8.0 as prop_float,
             |false as prop_bool,
             |'yyyy' as prop_string,
             |'prim_test4' as externalId""".stripMargin))

    val metricPrefix = shortRandomString()
    val df = readRows(primitiveExtId, metricPrefix).where("prop_string = 'abc' or prop_bool = false")
    df.count() shouldBe 3
    getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 3
    collectExternalIds(df) should contain only("prim_test", "prim_test3", "prim_test4")

    val metricPrefix2 = shortRandomString()
    val df2 = readRows(primitiveExtId, metricPrefix2)
      .where("prop_string in('abc', 'yyyy') or prop_float < 6.8")
    df2.count() shouldBe 3
    getNumberOfRowsRead(metricPrefix2, "datamodelinstances") shouldBe 3
    collectExternalIds(df2) should contain only("prim_test", "prim_test2", "prim_test4")

    bluefieldAlphaClient.dataModelInstances.deleteByExternalIds(
      Seq("prim_test", "prim_test2", "prim_test3", "prim_test4"))
      .unsafeRunSync()
  }
  it should "delete data model instances" in {
    insertRows(
      primitiveExtId,
      spark
        .sql(
          s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'prim_test' as externalId
             |
             |union all
             |
             |select 5.0 as prop_float,
             |true as prop_bool,
             |'zzzz' as prop_string,
             |'prim_test2' as externalId""".stripMargin))

    val metricPrefix = shortRandomString()
    val df = readRows(primitiveExtId, metricPrefix)
    df.count() shouldBe 2

    insertRows(
      modelExternalId = primitiveExtId,
      spark
        .sql(
          """select 'prim_test' as externalId
            |union all
            |select 'prim_test2' as externalId""".stripMargin),
      "delete")
    getNumberOfRowsDeleted(primitiveExtId, "datamodelinstances") shouldBe 2
    val df2 = readRows(primitiveExtId, metricPrefix).where("externalId in('prim_test', 'prim_test2')")
    df2.count() shouldBe 0
  }
}
