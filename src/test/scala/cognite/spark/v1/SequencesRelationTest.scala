package cognite.spark.v1

import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.SequenceColumnCreate
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}

import scala.util.control.NonFatal

class SequencesRelationTest extends FlatSpec with Matchers with SparkTest {
  import spark.implicits._

  private val sequencesSourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "sequences")
    .load()
  sequencesSourceDf.createOrReplaceTempView("sequences")

  it should "create and read sequence" in {
    val key = shortRandomString()
    val sequence = SequenceUpdateSchema(
      externalId = Some("a"),
      name = Some("a"),
      columns = Some(Seq(
        SequenceColumnCreate(
          name = Some("col1"),
          externalId = "col1",
          description = Some("col1 description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      ))
    )

    ingest(key, Seq(sequence))

    val sequences = retryWhile[Array[Row]](
      spark.sql(s"select * from sequences where externalId = 'a|$key'").collect,
      _.length != 1)
    sequences.length shouldBe 1
    sequences.head.getAs[String]("name") shouldBe "a"

    val columns = retryWhile[Array[Row]](
      spark.sql(s"select c.* from (select explode(columns) as c from sequences where externalId = 'a|$key')").collect,
      _.length != 1)
    columns.length shouldBe 1
    columns.head.getAs[String]("name") shouldBe "col1"

    cleanupSequence(key, "a")
  }

  it should "create using SQL" in {
    val key = shortRandomString()
    spark.sql(
      s"""select 'c|$key' as externalId,
         |       'c seq' as name,
         |       'Sequence C detailed description' as description,
         |       array(
         |           named_struct(
         |               'metadata', map('foo', 'bar', 'nothing', NULL),
         |               'name', 'column 1',
         |               'externalId', 'c_col1',
         |               'valueType', 'STRING'
         |           )
         |       ) as columns
         |""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequences")
      .option("onconflict", "abort")
      .save

    val sequence = writeClient.sequences.retrieveByExternalId(s"c|$key")
    sequence.name shouldBe Some("c seq")
    sequence.description shouldBe Some("Sequence C detailed description")
    sequence.assetId shouldBe None
    sequence.dataSetId shouldBe None
    sequence.columns should have size 1
    val col = sequence.columns.head
    col.externalId shouldBe "c_col1"
    col.name shouldBe Some("column 1")
    col.valueType shouldBe "STRING"
    col.metadata shouldBe Some(Map("foo" -> "bar"))
    col.description shouldBe None


    cleanupSequence(key, "c")
  }

  it should "create and update sequence" in {
    val key = shortRandomString()
    val sequence = SequenceUpdateSchema(
      externalId = Some("a"),
      name = Some("a"),
      columns = Some(Seq(
        SequenceColumnCreate(
          name = Some("col1"),
          externalId = "col1",
          description = Some("col1 description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      ))
    )

    ingest(key, Seq(sequence))

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequences where externalId = 'a|$key'").collect,
      rows => rows.length != 1)

    val sequenceUpdate = SequenceUpdateSchema(
      externalId = Some("a"),
      name = Some("a"),
      description = Some("description abc"),
      columns = None
    )

    ingest(key, Seq(sequenceUpdate), conflictMode = "update")

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequences where description = 'description abc' and externalId = 'a|$key'").collect,
      _.length != 1)

    cleanupSequence(key, "a")
  }

  def ingest(
    key: String,
    tree: Seq[SequenceUpdateSchema],
    metricsPrefix: Option[String] = None,
    conflictMode: String = "abort"
  ): Unit = {
    val processedTree = tree.map(s => s.copy(
      externalId = s.externalId.map(id => s"$id|$key")
    ))
    spark.sparkContext
      .parallelize(processedTree)
      .toDF()
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequences")
      .option("onconflict", conflictMode)
      .option("collectMetrics", metricsPrefix.isDefined)
      .option("metricsPrefix", metricsPrefix.getOrElse(""))
      .save

    val checkedAssets = processedTree.filter(_.externalId.isDefined)
    val storedCheckedAssets = writeClient.sequences.retrieveByExternalIds(checkedAssets.map(_.externalId.get))

    // check that the sequences are inserted correctly, before failing on long retries
    for ((inserted, stored) <- checkedAssets.zip(storedCheckedAssets)) {
      assert(inserted.externalId == stored.externalId)
      assert(inserted.name == stored.name)
      assert(inserted.metadata.getOrElse(Map()) == stored.metadata.getOrElse(Map()))
      assert(inserted.columns.map(_.toList).forall(_ == stored.columns.map(_.transformInto[SequenceColumnCreate]).toList))
      assert(inserted.description == stored.description)
      assert(inserted.assetId == stored.assetId)
      assert(inserted.dataSetId == stored.dataSetId)
    }
  }

  def cleanupSequence(key: String, ids: String*): Unit =
    writeClient.sequences.deleteByExternalIds(ids.map(id => s"$id|$key"))

}
