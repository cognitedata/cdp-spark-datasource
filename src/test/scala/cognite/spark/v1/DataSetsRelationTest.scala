package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import com.cognite.sdk.scala.v1.DataSet
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Inspectors, Matchers, ParallelTestExecution}

class DataSetsRelationTest extends FlatSpec
  with Matchers
  with ParallelTestExecution
  with SparkTest
  with Inspectors {

  // Data sets doesn't support deletes, and we don't want our tests to flood the tenant with datasets until we hit the limit.
  // Therefore just using one existing data set here.

  val name = "SparkTestDataSet"
  val description = "data set for Spark DataSource"
  val isWriteProtected = false
  val id = 86163806167772L

  val dataSetDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "datasets")
    .load()
    .where(s"name = '$name'")
  dataSetDf.createOrReplaceTempView("datasets")

  it should "be able to read a data set" taggedAs (ReadTest) in {

    val rows = dataSetDf.collect()

    assert(rows.length == 1)
    val dataset = fromRow[DataSet](rows.head)
    assert(dataset.name.contains(name))
    assert(dataset.description.contains(description))
    assert(dataset.id == id)
    assert(dataset.writeProtected == isWriteProtected)
  }

  it should "be able to update a data set" taggedAs (ReadTest) in {
    val externalId = s"sparktest-${shortRandomString()}"

    spark.sql(
      s"""
         |select '$externalId' as externalId,
         |$id as id
         |""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "datasets")
      .option("onconflict", "update")
      .option("collectMetrics", "true")
      .save()

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from datasets where id = $id and externalId = '$externalId'").collect(),
      rows => rows.length < 1
    )

    assert(rows.length == 1)
    val dataset = fromRow[DataSet](rows.head)
    assert(dataset.name.contains(name))
    assert(dataset.id == id)
    assert(dataset.writeProtected == isWriteProtected)
    assert(dataset.externalId.contains(externalId))
  }
}
