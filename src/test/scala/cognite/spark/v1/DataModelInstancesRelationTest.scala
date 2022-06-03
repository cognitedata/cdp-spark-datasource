package cognite.spark.v1

import com.cognite.sdk.scala.common.{DSLEqualsFilter, DSLPrefixFilter, DomainSpecificLanguageFilter, EmptyFilter, Items}
import com.cognite.sdk.scala.v1.DataModelType.NodeType
import com.cognite.sdk.scala.v1._
import io.circe.Json
import org.apache.spark.sql.DataFrame
import org.scalatest.{Assertion, BeforeAndAfterAll, FlatSpec, Matchers}

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
  private val spaceExternalId = "test-space"

  private def listInstances(
      isNode: Boolean,
      modelExternalId: String,
      filter: DomainSpecificLanguageFilter = EmptyFilter): DataModelInstanceQueryResponse = if (isNode){
    bluefieldAlphaClient.nodes
      .query(
        DataModelInstanceQuery(
          model = DataModelIdentifier(space = Some(spaceExternalId), model = modelExternalId),
          filter = filter,
          sort = None,
          limit = None)
      )
      .unsafeRunTimed(30.seconds)
      .get
  } else  {
    ???
  }

  private def getExternalIdList(
      isNode: Boolean,
      modelExternalId: String): Seq[String] = listInstances(isNode = isNode, modelExternalId).items.map(_.externalId)

  private def getByExternalId(isNode: Boolean,
                              modelExternalId: String,
                              externalId: String): PropertyMap =
    if (isNode) {
      bluefieldAlphaClient.nodes.retrieveByExternalIds(model = DataModelIdentifier(space = Some(spaceExternalId),
        model = modelExternalId), externalIds =  Seq(externalId)).unsafeRunSync().items.head
    }
    else {
      ???
    }

  private def byExternalId(isNode: Boolean, modelExternalId: String, externalId: String): String =
    getByExternalId(isNode = isNode, modelExternalId, externalId).externalId

  // TODO reenable shortRandomString suffix after delete is implemented in DMS. Now just create once for tests.
  private val multiValuedExtId = "MultiValues" // + shortRandomString()
  private val primitiveExtId = "Primitive" // + shortRandomString()
  private val multiValuedExtId2 = "MultiValues2" // + shortRandomString
  private val primitiveExtId2 = "Primitive2" // + shortRandomString()
  private val allModelExternalIds = Set(multiValuedExtId, primitiveExtId, multiValuedExtId2, primitiveExtId2)

  private val props = Map(
    "arr_int" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Int, nullable = false),
    "arr_boolean" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Boolean, nullable = true),
    "arr_str" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Text, nullable = true),
    "str_prop" -> DataModelPropertyDefinition(`type` = PropertyType.Text, nullable = true)
  )

  private val props2 = Map(
    "prop_float" -> DataModelPropertyDefinition(`type` = PropertyType.Float64, nullable = true),
    "prop_bool" -> DataModelPropertyDefinition(`type` = PropertyType.Boolean, nullable = true),
    "prop_string" -> DataModelPropertyDefinition(`type` = PropertyType.Text, nullable = true)
  )
  private val props3 = Map(
    "prop_int32" -> DataModelPropertyDefinition(`type` = PropertyType.Int32, nullable = false),
    "prop_int64" -> DataModelPropertyDefinition(`type` = PropertyType.Int64, nullable = false),
    "prop_float32" -> DataModelPropertyDefinition(`type` = PropertyType.Float32, nullable = true),
    "prop_float64" -> DataModelPropertyDefinition(`type` = PropertyType.Float64, nullable = true),
    "prop_numeric" -> DataModelPropertyDefinition(`type` = PropertyType.Numeric, nullable = true),
    "arr_int32" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Int32, nullable = false),
    "arr_int64" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Int64, nullable = false),
    "arr_float32" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Float32, nullable = true),
    "arr_float64" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Float64, nullable = true),
    "arr_numeric" -> DataModelPropertyDefinition(`type` = PropertyType.Array.Numeric, nullable = true)
  )

  private val props4 = Map(
    "prop_direct_relation" -> DataModelPropertyDefinition(`type` = PropertyType.DirectRelation, nullable = true),
    "prop_timestamp" -> DataModelPropertyDefinition(`type` = PropertyType.Timestamp, nullable = true),
    "prop_date" ->DataModelPropertyDefinition(`type` = PropertyType.Date, nullable = true)
  )

  override def beforeAll(): Unit = {
    def createAndGetModels(): Seq[DataModel] = {
      /*bluefieldAlphaClient.dataModels
        .createItems(
          Seq(
            DataModel(externalId = multiValuedExtId, dataModelType = NodeType, properties = Some(props)),
            DataModel(externalId = primitiveExtId, dataModelType = NodeType, properties = Some(props2)),
            DataModel(externalId = multiValuedExtId2, dataModelType = NodeType, properties = Some(props3)),
            DataModel(externalId = primitiveExtId2, dataModelType = NodeType, properties = Some(props4)),
          ), spaceExternalId)
        .unsafeRunSync()*/
      bluefieldAlphaClient.dataModels.list(spaceExternalId).unsafeRunSync()
    }

    retryWhile[scala.Seq[DataModel]](
      createAndGetModels(),
      dm => !allModelExternalIds.subsetOf(dm.map(_.externalId).toSet)
    )
    ()
  }

  override def afterAll(): Unit = {
    // TODO enable this after delete is supported
    /* def deleteAndGetModels(): Seq[DataModel] = {
      bluefieldAlphaClient.dataModels
        .deleteItems(Seq(
          multiValuedExtId,
          primitiveExtId,
          multiValuedExtId2,
          primitiveExtId2)
        )
        .unsafeRunSync()
      bluefieldAlphaClient.dataModels.list().unsafeRunSync()
    }
    retryWhile[scala.Seq[DataModel]](
      deleteAndGetModels(),
      dm => dm.map(_.externalId).toSet.intersect(allModelExternalIds).nonEmpty
    )
    */
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
      .option("spaceExternalId", spaceExternalId)
      .option("instanceSpaceExternalId", spaceExternalId)
      .option("onconflict", onconflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", modelExternalId)
      .save

  private def tryTestAndCleanUp(externalIds: Seq[String], testCode: Assertion) =
    try {
      testCode
    } finally {
      try {
        bluefieldAlphaClient.nodes.deleteByExternalIds(externalIds).unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }

 it should "ingest data" in {
    val randomId = "prim_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId), {
        retryWhile[Boolean](
          {
            val res = Try {
              insertRows(
                primitiveExtId,
                spark
                  .sql(s"""select 2.0 as prop_float,
                        |true as prop_bool,
                        |'abc' as prop_string,
                        |'${randomId}' as externalId""".stripMargin)
              )
            }
            println(res)
              res.isFailure
          },
          failure => failure
        )
        byExternalId(true, primitiveExtId, randomId) shouldBe randomId
        getNumberOfRowsUpserted(primitiveExtId, "datamodelinstances") shouldBe 1
      }
    )
  }


  it should "return an informative error when a value with wrong type is attempted to be ingested" in {
    val ex = sparkIntercept {
      insertRows(
        primitiveExtId,
        spark
          .sql(s"""select '2.0' as prop_float,
                  |true as prop_bool,
                  |'abc' as prop_string,
                  |'test' as externalId""".stripMargin)
      )
    }
    ex shouldBe an[CdfSparkException]
    ex.getMessage shouldBe "2.0 of type class java.lang.String is not a valid float64. " +
      "Try to cast the value to double. For example, ‘double(col_name) as prop_name’ or ‘cast(col_name as double) as prop_name’."
  }



  it should "return an informative error when schema inference fails" in {
    val ex = sparkIntercept {
      insertRows(
        "non-existing-model",
        spark
          .sql(s"""select '2.0' as prop_float,
                  |true as prop_bool,
                  |'abc' as prop_string,
                  |'test' as externalId""".stripMargin)
      )
    }
    // TODO Missing model externalId used to result in CdpApiException, now it returns empty list
    //  Check with dms team
    // ex.getMessage shouldBe "Could not resolve schema of data model non-existing-model. " +
    //  "Got an exception from CDF API: ids not found: non-existing-model (code: 400)"
    ex.getMessage shouldBe "Could not resolve schema of data model non-existing-model. Please check if the model exists."
    ex shouldBe an[CdfSparkException]
  }


  it should "cast correctly using float() and ingest data " in {
    val randomId = "prim_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId), {
        retryWhile[Boolean](
          {
            Try {
              insertRows(
                primitiveExtId,
                spark
                  .sql(s"""select float('2.0') as prop_float,
                          |true as prop_bool,
                          |'abc' as prop_string,
                          |'${randomId}' as externalId""".stripMargin)
              )
            }.isFailure
          },
          failure => failure
        )
        getByExternalId(true, primitiveExtId, randomId).allProperties.get("prop_float") shouldBe Some(PropertyType.Float64.Property(2.0))
      }
    )
  }

  it should "cast correctly using cast() and ingest data " in {
    val randomId = "prim_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId), {
        retryWhile[Boolean](
          {
            Try {
              insertRows(
                primitiveExtId,
                spark
                  .sql(s"""select cast('3.0' as float) as prop_float,
                          |true as prop_bool,
                          |'abc' as prop_string,
                          |'${randomId}' as externalId""".stripMargin)
              )
            }.isFailure
          },
          failure => failure
        )
        getByExternalId(true, primitiveExtId, randomId).allProperties.get("prop_float") shouldBe Some(PropertyType.Float64.Property(3.0))
      }
    )
  }

 it should "ingest multi valued data" in {
    val randomId1 = "test_multi_" + shortRandomString()
    val randomId2 = "test_multi_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId1, randomId2), {
        retryWhile[Boolean](
          {
            val res = Try {
              insertRows(
                multiValuedExtId,
                spark.sql(s"""select array() as arr_int,
                |array(true, false) as arr_boolean,
                |NULL as arr_str,
                |NULL as str_prop,
                |'${randomId1}' as externalId
                |
                |union all
                |
                |select array(1,2) as arr_int,
                |NULL as arr_boolean,
                |array('x', 'y') as arr_str,
                |'hehe' as str_prop,
                |'${randomId2}' as externalId""".stripMargin)
              )
            }
            println(res)
              res.isFailure
          },
          failure => failure
        )
        (getExternalIdList(true, multiValuedExtId) should contain).allOf(randomId1, randomId2)
        getNumberOfRowsUpserted(multiValuedExtId, "datamodelinstances") shouldBe 2
      }
    )
  }
  /*

 it should "read instances" in {
    val randomId = "prim_test2_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId), {
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
      }
    )
  }

 it should "read multi valued instances" in {
    val randomId1 = "numeric_test_" + shortRandomString()
    val randomId2 = "numeric_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId1, randomId2), {
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
      }
    )
  }

 it should "fail when writing null to a non nullable property" in {
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

 it should "filter instances by externalId" in {
    val randomId1 = "numeric_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId1), {
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
      }
    )
  }

 it should "filter instances" in {
    val randomId1 = "numeric_test_" + shortRandomString()
    val randomId2 = "numeric_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId1, randomId2), {
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
      }
    )
  }

 it should "filter instances using or" in {
    val randomId1 = "prim_test_" + shortRandomString()
    val randomId2 = "prim_test_" + shortRandomString()
    val randomId3 = "prim_test_" + shortRandomString()
    val randomId4 = "prim_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId1, randomId2, randomId3, randomId4), {
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
      }
    )
  }
 it should "delete data model instances" in {
    val randomId1 = "prim_test_" + shortRandomString()
    val randomId2 = "prim_test2_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId1, randomId2), {
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
      }
    )
  }

 it should "ingest data with special property types" in {
    val randomId = "prim_test_" + shortRandomString()
    tryTestAndCleanUp(
      Seq(randomId), {
        retryWhile[Boolean](
          {
            Try {
              insertRows(
                primitiveExtId2,
                spark
                  .sql(s"""select 'asset' as prop_direct_relation,
                          |timestamp('2022-01-01T12:34:56.789+00:00') as prop_timestamp,
                          |date('2022-01-01') as prop_date,
                          |'${randomId}' as externalId""".stripMargin)
              )
            }.isFailure
          },
          failure => failure
        )
        byExternalId(primitiveExtId2, randomId) shouldBe Some(randomId)
        getNumberOfRowsUpserted(primitiveExtId2, "datamodelinstances") shouldBe 1
      }
    )
  }

 it should "read instances with special property types" in {
    val randomId = "prim_test_" + shortRandomString()
   val randomId2 = "prim_test_" + shortRandomString() + "_2"
   tryTestAndCleanUp(
      Seq(randomId, randomId2), {
        retryWhile[Boolean](
          {
            Try {
              insertRows(
                primitiveExtId2,
                spark
                  .sql(
                    s"""select 'asset' as prop_direct_relation,
                       |timestamp('2022-01-01T12:34:56.789+00:00') as prop_timestamp,
                       |date('2022-01-20') as prop_date,
                       |'${randomId}' as externalId
                       |
                       |union all
                       |
                       |select 'asset2' as prop_direct_relation,
                       |timestamp('2022-01-10T12:34:56.789+00:00') as prop_timestamp,
                       |date('2022-01-01') as prop_date,
                       |'${randomId2}' as externalId""".stripMargin)
              )
            }.isFailure
          },
          failure => failure
        )
       val metricPrefix = shortRandomString()
        val df = readRows(primitiveExtId2, metricPrefix)
        df.count() shouldBe 2
        getNumberOfRowsRead(metricPrefix, "datamodelinstances") shouldBe 2
     }
    )
  }

  */
}
