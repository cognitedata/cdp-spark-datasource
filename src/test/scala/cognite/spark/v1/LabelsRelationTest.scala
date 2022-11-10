package cognite.spark.v1

import cognite.spark.v1.SparkSchemaHelper.fromRow
import com.cognite.sdk.scala.v1.{DataSet, DataSetCreate, Label, LabelCreate, LabelsFilter}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Inspectors, Matchers, ParallelTestExecution}

class LabelsRelationTest
    extends FlatSpec
    with Matchers
    with ParallelTestExecution
    with SparkTest
    with Inspectors {
  val datasetLabels: String = "spark-ds-labels-test"
  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "labels")
    .load()
  destinationDf.createOrReplaceTempView("destinationLabel")

  val ds: Seq[DataSet] = writeClient.dataSets.retrieveByExternalIds(Seq(datasetLabels), ignoreUnknownIds = true)
  val dsId: Long = if (ds.isEmpty) {
    writeClient
      .dataSets.createOne(DataSetCreate(externalId = Some(datasetLabels), name = Some(datasetLabels))).id
  } else {
    ds.head.id
  }

  it should "be able to read a label" taggedAs (ReadTest) in {
    val externalId = s"sparktest-${shortRandomString()}"
    val name = "test-read"
    val description = "Created by test for spark data source"

    writeClient.labels.create(Seq(LabelCreate(externalId, name, Some(description), dataSetId = Some(dsId))))

    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "labels")
      .load()
      .where(s"externalId = '$externalId'")
      .collect()

    assert(rows.length == 1)
    val label = fromRow[Label](rows.head)
    assert(label.name == name)
    assert(label.externalId == externalId)
    assert(label.description.contains(description))
    assert(label.dataSetId.contains(dsId))

    writeClient.labels.deleteByExternalId(externalId)
  }

  it should "be able to write a label" taggedAs (WriteTest) in {
    val externalId = s"sparktest-${shortRandomString()}"
    val name = "test-insert"
    val description = "Created by test for spark data source"

    spark
      .sql(
        s"""select '$externalId' as externalId,
           |'$name' as name, '$description' as description,
           |$dsId as dataSetId""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("type", "labels")
      .option("apiKey", writeApiKey)
      .save()

    val labels = writeClient.labels
      .filter(LabelsFilter(externalIdPrefix = Some(externalId)))
      .compile
      .toList

    assert(labels.length == 1)

    val label = labels.head

    assert(label.name == name)
    assert(label.externalId == externalId)
    assert(label.description.contains(description))
    assert(label.dataSetId.contains(dsId))

    writeClient.labels.deleteByExternalId(externalId)
  }

  it should "be able to delete a label" taggedAs (WriteTest) in {
    val externalId = s"sparktest-${shortRandomString()}"
    val name = "test-delete"
    val description = "Created by test for spark data source"

    writeClient.labels.create(Seq(LabelCreate(externalId, name, Some(description))))

    spark
      .sql(s"select externalId from destinationLabel where externalId = '$externalId'")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "labels")
      .option("onconflict", "delete")
      .save()

    val labels = writeClient.labels.filter(LabelsFilter(Some(externalId))).compile.toList
    assert(labels.isEmpty)
  }
}
