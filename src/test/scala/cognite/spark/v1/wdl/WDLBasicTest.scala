package cognite.spark.v1.wdl

import cognite.spark.v1.{WDLSparkTest, WriteTest}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors, Matchers}

class WDLBasicTest extends FlatSpec with Matchers with WDLSparkTest with Inspectors with BeforeAndAfter {

  before {
    SQLConf.get.setConfString("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
    client.deleteAll()
    client.miniSetup()
  }

  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .option("wdlDataType", "Well")
    .useOIDCWrite
    .load()
  destinationDf.createOrReplaceTempView("wdl_test")

  it should "be possible to write a well" taggedAs (WriteTest) in {
    spark
      .sql(s"""select 'hello' as name,
           |       named_struct('x', 20.0, 'y', 30.0, 'crs', 'EPSG:4326') as wellhead,
           |       named_struct('sourceName', 'A', 'assetExternalId', 'A:well1') as source
           |""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "WellIngestion")
      .useOIDCWrite
      .save()
  }

  it should "be able to read a well" in {
    val sparkSql = spark
      .sql("select * from wdl_test")

    val rows = sparkSql.collect()
    rows.length shouldEqual 1
  }

  it should "be able to read subset of fields from a well" in {
    val sparkSql = spark
      .sql("select matchingId, name, description, uniqueWellIdentifier, waterDepth from wdl_test")

    val rows = sparkSql.collect()
    rows.length shouldEqual 1
  }
}
