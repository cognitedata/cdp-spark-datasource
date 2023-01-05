package cognite.spark.v1.wdl

import cognite.spark.v1.SparkTest
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class WDLSourcesRelationTest extends FlatSpec with Matchers with SparkTest with Inspectors {

  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "wdl")
    .option("wdlDataType", "Source")
    .load()
  destinationDf.createOrReplaceTempView("wdl_test")

  it should "be able to read sources" in {
    val sparkSql = spark
      .sql("select * from wdl_test")

    sparkSql.printSchema()
    sparkSql.show(50, false)
    val rows = sparkSql.collect()
    assert(rows.length > -1)
  }
}
