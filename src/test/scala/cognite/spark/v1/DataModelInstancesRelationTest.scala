package cognite.spark.v1

import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1._
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration.DurationInt
import scala.util.Try
import scala.util.control.NonFatal

class DataModelInstancesRelationTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with BeforeAndAfterAll {
  import CdpConnector.ioRuntime

  val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val bluefieldAlphaClient = getBlufieldClient(Some("alpha"))

  private def listInstances(
      modelExternalId: String,
      filter: Option[DataModelInstanceFilter] = None): Seq[DataModelInstanceQueryResponse] =
    bluefieldAlphaClient.dataModelInstances
      .query(
        DataModelInstanceQuery(
          modelExternalId = modelExternalId,
          filter = filter,
          sort = None,
          limit = None))
      .unsafeRunTimed(30.seconds)
      .get
      .items

  private def getExternalIdList(modelExternalId: String): Seq[String] =
    listInstances(modelExternalId)
      .flatMap(_.properties.flatMap(_.get("externalId")).toList).map(_.asInstanceOf[StringProperty].value)

  private def byExternalId(modelExternalId: String, externalId: String): String =
    listInstances(
      modelExternalId,
      filter = Some(DMIEqualsFilter(Seq("instance", "externalId"), StringProperty(externalId)))).head.properties.flatMap(_.get("externalId")).get.asInstanceOf[StringProperty].value

  private val multiValuedExtId = "MultiValues_" + shortRandomString()
  private val primitiveExtId = "Primitive_" + shortRandomString()
  private val multiValuedExtId2 = "MultiValues2_" + shortRandomString
  private val allModelExternalIds = Set(multiValuedExtId, primitiveExtId, multiValuedExtId2)

  private val props = Map(
    "arr_int" -> DataModelProperty(`type` = "int[]", nullable = false),
    "arr_boolean" -> DataModelProperty(`type` = "boolean[]", nullable = true),
    "arr_str" -> DataModelProperty(`type` = "text[]", nullable = true),
    "str_prop" -> DataModelProperty(`type` = "text", nullable = true)
  )
  private val props2 = Map(
    "prop_float" -> DataModelProperty(`type` = "float64", nullable = true),
    "prop_bool" -> DataModelProperty(`type` = "boolean", nullable = true),
    "prop_string" -> DataModelProperty(`type` = "text", nullable = true)
  )
  private val props3 = Map(
    "prop_int32" -> DataModelProperty(`type` = "int32", nullable = false),
    "prop_int64" -> DataModelProperty(`type` = "int64", nullable = false),
    "prop_float32" -> DataModelProperty(`type` = "float32", nullable = true),
    "prop_float64" -> DataModelProperty(`type` = "float64", nullable = true),
    "prop_numeric" -> DataModelProperty(`type` = "numeric", nullable = true),
    "arr_int32" -> DataModelProperty(`type` = "int32[]", nullable = false),
    "arr_int64" -> DataModelProperty(`type` = "int64[]", nullable = false),
    "arr_float32" -> DataModelProperty(`type` = "float32[]", nullable = true),
    "arr_float64" -> DataModelProperty(`type` = "float64[]", nullable = true),
    "arr_numeric" -> DataModelProperty(`type` = "numeric[]", nullable = true)
  )

  override def beforeAll(): Unit = {
    def createAndGetModels(): Seq[DataModel] = {
      bluefieldAlphaClient.dataModels
        .createItems(
          Items[DataModel](Seq(
            DataModel(externalId = multiValuedExtId, properties = Some(props)),
            DataModel(externalId = primitiveExtId, properties = Some(props2)),
            DataModel(externalId = multiValuedExtId2, properties = Some(props3))
          )))
        .unsafeRunTimed(30.seconds)
      bluefieldAlphaClient.dataModels.list().unsafeRunSync()
    }

    retryWhile[scala.Seq[DataModel]](
      createAndGetModels(),
      dm => !allModelExternalIds.subsetOf(dm.map(_.externalId).toSet)
    )
    ()
  }

  override def afterAll(): Unit = {
    def deleteAndGetModels(): Seq[DataModel] = {
      bluefieldAlphaClient.dataModels
        .deleteItems(Seq(multiValuedExtId, primitiveExtId, multiValuedExtId2))
        .unsafeRunSync()
      bluefieldAlphaClient.dataModels.list().unsafeRunSync()
    }
    retryWhile[scala.Seq[DataModel]](
      deleteAndGetModels(),
      dm => dm.map(_.externalId).toSet.intersect(allModelExternalIds).nonEmpty
    )
    ()
  }

  private def collectExternalIds(df: DataFrame): List[String] =
    df.select("externalId")
      .collect()
      .map(_.getAs[String]("externalId"))
      .toList

  private def readRows(modelExternalId: String, metricPrefix: String) =
    spark.read
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
    val randomId = "prim_test_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
            insertRows(
              primitiveExtId,
              spark
                .sql(s"""select 2.0 as prop_float,
                  |true as prop_bool,
                  |'abc' as prop_string,
                  |'${randomId}' as externalId""".stripMargin)
            )
          }.isFailure
        },
        failure => failure
      )
      byExternalId(primitiveExtId, randomId) shouldBe randomId
      getNumberOfRowsUpserted(primitiveExtId, "datamodelinstances") shouldBe 1
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances.deleteByExternalId(randomId).unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  it should "ingest multi valued data" in {
    val randomId1 = "test_multi_" + shortRandomString()
    val randomId2 = "test_multi_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
            insertRows(
              multiValuedExtId,
              spark
                .sql(s"""select array() as arr_int,
                |array(true, false) as arr_boolean,
                |NULL as arr_str,
                |NULL as str_prop,
                |'${randomId1}' as externalId
                |
                |union all
                |
                |select array(1,2) as arr_int,
                |NULL as arr_boolean,
                |array('hehe') as arr_str,
                |'hehe' as str_prop,
                |'${randomId2}' as externalId""".stripMargin)
            )
          }.isFailure
        },
        failure => failure
      )
      (getExternalIdList(multiValuedExtId) should contain).allOf(randomId1, randomId2)
      getNumberOfRowsUpserted(multiValuedExtId, "datamodelinstances") shouldBe 2
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances
          .deleteByExternalIds(Seq(randomId1, randomId2))
          .unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  ignore should "read instances" in {
    val randomId = "prim_test2_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
            insertRows(
              primitiveExtId,
              spark
                .sql(s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'$randomId' as externalId""".stripMargin)
            )
          }.isFailure
        },
        failure => failure
      )

      val metricPrefix = shortRandomString()
      val df = readRows(primitiveExtId, metricPrefix)
      df.limit(1).count() shouldBe 1
      getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances.deleteByExternalId(randomId).unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  ignore should "read multi valued instances" in {
    val randomId1 = "numeric_test_" + shortRandomString()
    val randomId2 = "numeric_test_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
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
             |'$randomId1' as externalId
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
             |'$randomId2' as externalId""".stripMargin
                )
            )
          }.isFailure
        },
        failure => failure
      )

      val metricPrefix = shortRandomString()
      val df = readRows(multiValuedExtId2, metricPrefix)
      df.limit(1).count() shouldBe 1
      getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
      (df
        .select("externalId")
        .collect()
        .map(_.getAs[String]("externalId"))
        .toList should contain).allOf(randomId1, randomId2)
      getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 3
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances
          .deleteByExternalIds(Seq(randomId1, randomId2))
          .unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  ignore should "fail when writing null to a non nullable property" in {
    val ex = sparkIntercept {
      insertRows(
        multiValuedExtId,
        spark
          .sql(s"""select NULL as arr_int,
             |array(true, false) as arr_boolean,
             |NULL as arr_str,
             |NULL as str_prop,
             |'test_multi' as externalId""".stripMargin)
      )
    }
    ex shouldBe an[CdfSparkException]
    ex.getMessage shouldBe s"Property of int[] type is not nullable."
  }

  ignore should "filter instances by externalId" in {
    val randomId1 = "numeric_test_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
            insertRows(
              primitiveExtId,
              spark
                .sql(s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'$randomId1' as externalId""".stripMargin)
            )
          }.isFailure
        },
        failure => failure
      )
      val metricPrefix = shortRandomString()
      val df = readRows(primitiveExtId, metricPrefix)
      df.where(s"externalId = '$randomId1'").count() shouldBe 1
      getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances.deleteByExternalId(randomId1).unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  ignore should "filter instances" in {
    val randomId1 = "numeric_test_" + shortRandomString()
    val randomId2 = "numeric_test_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
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
             |'$randomId1' as externalId
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
             |'$randomId2' as externalId""".stripMargin
                )
            )
          }.isFailure
        },
        failure => failure
      )

      val metricPrefix = shortRandomString()
      val df = readRows(multiValuedExtId2, metricPrefix)
      val andDf = df.where("prop_numeric > 1.5 and prop_float64 = 0.8")
      andDf.count() shouldBe 1
      getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 1
      (collectExternalIds(andDf) should contain).only(randomId1)

      val metricPrefix2 = shortRandomString()
      val df2 = readRows(multiValuedExtId2, metricPrefix2)
        .where("not (prop_numeric > 1.5 and prop_float64 >= 0.7)")
      df2.count() shouldBe 1
      getNumberOfRowsRead(metricPrefix2, "datamodelinstances") shouldBe 1
      (collectExternalIds(df2) should contain).only(randomId2)

      val metricPrefix3 = shortRandomString()
      val df3 = readRows(multiValuedExtId2, metricPrefix3).where("prop_float32 is not null")
      df3.count() shouldBe 1
      getNumberOfRowsRead(metricPrefix3, "datamodelinstances") shouldBe 1
      (collectExternalIds(df3) should contain).only(randomId1)
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances
          .deleteByExternalIds(Seq(randomId1, randomId2))
          .unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }

  ignore should "filter instances using or" in {
    val randomId1 = "prim_test_" + shortRandomString()
    val randomId2 = "prim_test_" + shortRandomString()
    val randomId3 = "prim_test_" + shortRandomString()
    val randomId4 = "prim_test_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
            insertRows(
              primitiveExtId,
              spark
                .sql(s"""select 2.1 as prop_float,
                    |false as prop_bool,
                    |'abc' as prop_string,
                    |'$randomId1' as externalId
                    |
                    |union all
                    |
                    |select 5.0 as prop_float,
                    |true as prop_bool,
                    |'zzzz' as prop_string,
                    |'$randomId2' as externalId
                    |
                    |union all
                    |
                    |select 9.0 as prop_float,
                    |false as prop_bool,
                    |'xxxx' as prop_string,
                    |'$randomId3' as externalId
                    |
                    |union all
                    |
                    |select 8.0 as prop_float,
                    |false as prop_bool,
                    |'yyyy' as prop_string,
                    |'$randomId4' as externalId""".stripMargin)
            )
          }.isFailure
        },
        failure => failure
      )

      val metricPrefix = shortRandomString()
      val df = readRows(primitiveExtId, metricPrefix).where("prop_string = 'abc' or prop_bool = false")
      df.count() shouldBe 3
      getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 3
      (collectExternalIds(df) should contain).only(randomId1, randomId3, randomId4)

      val metricPrefix2 = shortRandomString()
      val df2 = readRows(primitiveExtId, metricPrefix2)
        .where("prop_string in('abc', 'yyyy') or prop_float < 6.8")
      df2.count() shouldBe 3
      getNumberOfRowsRead(metricPrefix2, "datamodelinstances") shouldBe 3

      val metricPrefix3 = shortRandomString()
      val df3 = readRows(primitiveExtId, metricPrefix3)
        .where("prop_string LIKE 'xx%'")
      df3.count() shouldBe 1
      getNumberOfRowsRead(metricPrefix3, "datamodelinstances") shouldBe 1

      (collectExternalIds(df3) should contain).only(randomId3)
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances
          .deleteByExternalIds(Seq(randomId1, randomId2, randomId3, randomId4))
          .unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }

  }
  ignore should "delete data model instances" in {
    val randomId1 = "prim_test_" + shortRandomString()
    val randomId2 = "prim_test2_" + shortRandomString()
    try {
      retryWhile[Boolean](
        {
          Try {
            insertRows(
              primitiveExtId,
              spark
                .sql(s"""select 2.1 as prop_float,
             |false as prop_bool,
             |'abc' as prop_string,
             |'$randomId1' as externalId
             |
             |union all
             |
             |select 5.0 as prop_float,
             |true as prop_bool,
             |'zzzz' as prop_string,
             |'$randomId2' as externalId""".stripMargin)
            )
          }.isFailure
        },
        failure => failure
      )

      val metricPrefix = shortRandomString()
      val df = readRows(primitiveExtId, metricPrefix)
      df.count() shouldBe 2

      insertRows(
        modelExternalId = primitiveExtId,
        spark
          .sql(s"""select '$randomId1' as externalId
            |union all
            |select '$randomId2' as externalId""".stripMargin),
        "delete"
      )
      getNumberOfRowsDeleted(primitiveExtId, "datamodelinstances") shouldBe 2
      val df2 =
        readRows(primitiveExtId, metricPrefix).where(s"externalId in('$randomId1', '$randomId2')")
      df2.count() shouldBe 0
    } finally {
      try {
        bluefieldAlphaClient.dataModelInstances
          .deleteByExternalIds(Seq(randomId1, randomId2))
          .unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }
}
