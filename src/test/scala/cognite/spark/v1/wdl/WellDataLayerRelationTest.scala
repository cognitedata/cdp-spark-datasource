package cognite.spark.v1.wdl

import cognite.spark.v1.{WDLSparkTest, WriteTest}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class WellDataLayerRelationTest extends FlatSpec with Matchers with WDLSparkTest with Inspectors {

  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("apiKey", writeApiKey)
    .option("type", "wdl")
    .option("wdlDataType", "Well")
    .load()
  destinationDf.createOrReplaceTempView("wdl_test")

  it should "be possible to write a well" taggedAs (WriteTest) in {
    spark
      .sql(s"""select 'hello' as name,
           |       named_struct('x', 20.0, 'y', 30.0, 'crs', 'EPSG:4326') as wellhead,
           |       named_struct('sourceName', 'EDM', 'assetExternalId', 'EDM:well1') as source
           |""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "wdl")
      .option("wdlDataType", "WellIngestion")
      .option("apiKey", writeApiKey)
      .save()
  }

  it should "be able to read a well" in {
    val sparkSql = spark
      .sql("select * from wdl_test")

    sparkSql.printSchema()
    sparkSql.show(50, false)
    val rows = sparkSql.collect()
    assert(rows.length > -1)
  }

  it should "be able to read subset of fields from a well" in {
    val sparkSql = spark
      .sql("select matchingId, name, description, uniqueWellIdentifier, waterDepth from wdl_test")

    sparkSql.printSchema()
    sparkSql.show(50, false)
    val rows = sparkSql.collect()
    assert(rows.length > -1)
  }
}
