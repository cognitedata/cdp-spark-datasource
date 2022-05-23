package cognite.spark.v1

import com.cognite.sdk.scala.v1.SequenceColumnCreate
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers, OptionValues, ParallelTestExecution}
import java.util.UUID
import scala.util.control.NonFatal

class SequencesRelationTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with ParallelTestExecution
    with SparkTest {
  import spark.implicits._

  private val sequencesSourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "sequences")
    .load()
  sequencesSourceDf.createOrReplaceTempView("sequences")

  it should "create and read sequence" in {
    val key = shortRandomString()
    val id = UUID.randomUUID().toString
    val sequence = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      columns = Seq(
        SequenceColumnCreate(
          name = Some("col1"),
          externalId = "col1",
          description = Some("col1 description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      )
    )

    ingests(Seq(sequence))

    val sequences = retryWhile[Array[Row]](
      spark.sql(s"select * from sequences where externalId = '$id'").collect,
      _.length != 1)
    sequences.length shouldBe 1
    sequences.head.getAs[String]("name") shouldBe "a"

    val columns = retryWhile[Array[Row]](
      spark
        .sql(s"select c.* from (select explode(columns) as c from sequences where externalId = '$id')")
        .collect,
      _.length != 1)
    columns.length shouldBe 1
    columns.head.getAs[String]("name") shouldBe "col1"

    cleanupSequences(Seq(id))
  }

  it should "create using SQL" in {
    val id = UUID.randomUUID().toString
    spark
      .sql(s"""select '$id' as externalId,
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

    val sequence = writeClient.sequences.retrieveByExternalId(s"$id")
    sequence.name shouldBe Some("c seq")
    sequence.description shouldBe Some("Sequence C detailed description")
    sequence.assetId shouldBe None
    sequence.dataSetId shouldBe None
    (sequence.columns should have).size(1)
    val col = sequence.columns.head
    col.externalId.value shouldBe "c_col1"
    col.name shouldBe Some("column 1")
    col.valueType shouldBe "STRING"
    col.metadata shouldBe Some(Map("foo" -> "bar"))
    col.description shouldBe None

    cleanupSequences(Seq(id))
  }

  it should "create and update sequence" in {
    val id = UUID.randomUUID().toString
    val sequence = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      columns = Seq(
        SequenceColumnCreate(
          name = Some("col1"),
          externalId = "col1",
          description = Some("col1 description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      )
    )

    ingests(Seq(sequence))

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequences where externalId = '$id'").collect,
      rows => rows.length != 1)

    val sequenceUpdate = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      description = Some("description abc"),
      columns = Seq(SequenceColumnCreate(
        name = Some("col2"),
        externalId = "col1",
        description = Some("col2 description"),
        valueType = "STRING",
        metadata = Some(Map("foo2" -> "bar2"))
      ))
    )

    ingests(Seq(sequenceUpdate), conflictMode = "update")

    retryWhile[Array[Row]](
      spark
        .sql(s"select * from sequences where description = 'description abc' and externalId = '$id'")
        .collect,
      rows => rows.length != 1)

    val colHead = writeClient.sequences.retrieveByExternalId(s"$id").columns.head
    colHead.name shouldBe Some("col2")
    colHead.description shouldBe Some("col2 description")
    colHead.metadata shouldBe Some(Map("foo2" -> "bar2"))
    cleanupSequences(Seq(id))
  }


  it should "upsert using SQL" in {
    val id = UUID.randomUUID().toString
    val sequenceToCreate = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      columns = Seq(
        SequenceColumnCreate(
          name = Some("col1"),
          externalId = "c_col1",
          description = Some("col1 description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        ),
        SequenceColumnCreate(
          name = Some("col10"),
          externalId = "c_col10",
          description = Some("col10 description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      )
    )

    ingests(Seq(sequenceToCreate))

    spark
      .sql(s"""select '$id' as externalId,
              |       'seq name1' as name,
              |       'desc1' as description,
              |       array(
              |           named_struct(
              |               'metadata', map('m1', 'v1', 'm2', NULL),
              |               'name', 'col1_updated',
              |               'description', 'updated desc1',
              |               'externalId', 'c_col1',
              |               'valueType', NULL
              |           ),
              |           named_struct(
              |               'metadata', map('m1', 'v1', 'm2', 'v2'),
              |               'name', 'col2',
              |               'description', 'updated desc2',
              |               'externalId', 'c_col2',
              |               'valueType', 'STRING'
              |           )
              |       ) as columns
              |
              |union all
              |
              |select '$id-2' as externalId,
              |       'seq name2' as name,
              |       'desc2' as description,
              |       array(
              |           named_struct(
              |               'metadata', map('foo', 'bar', 'nothing', NULL),
              |               'name', 's2c3',
              |               'description', 'hey',
              |               'externalId', 'c_col3',
              |               'valueType', 'STRING'
              |           )
              |       ) as columns
              |""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequences")
      .option("onconflict", "upsert")
      .save

    val sequence1 = writeClient.sequences.retrieveByExternalId(id)
    val columns1 = sequence1.columns
    val sequence2 = writeClient.sequences.retrieveByExternalId(s"$id-2")
    val columns2 = sequence2.columns

    sequence1.name shouldBe Some("seq name1")
    sequence1.description shouldBe Some("desc1")

    sequence2.name shouldBe Some("seq name2")
    sequence2.description shouldBe Some("desc2")

    columns1.toList.flatMap(_.name.toList).toSet shouldBe Set("col1_updated", "col2")
    columns1.toList.flatMap(_.externalId.toList).toSet shouldBe Set("c_col1", "c_col2")
    columns1.toList.flatMap(_.description).toSet shouldBe Set("updated desc1", "updated desc2")
    columns1.toList.flatMap(_.metadata.toList).toSet shouldBe Set(Map("m1" -> "v1"), Map("m1" -> "v1", "m2" -> "v2"))
    columns1.map(_.valueType).toList shouldBe List("STRING", "STRING")

    columns2.head.name shouldBe Some("s2c3")
    columns2.head.externalId shouldBe Some("c_col3")
    columns2.head.description shouldBe Some("hey")
    columns2.head.metadata shouldBe Some(Map("foo" -> "bar"))
    columns2.head.valueType shouldBe "STRING"

    cleanupSequences(Seq(id, s"$id-2"))
  }

  it should "chunk sequence if more than 10000 columns in the request" in {
    val key = shortRandomString()

    //650 * 200 give us 130000 columns in total
    val ids = (1 to 650).map(_ => UUID.randomUUID().toString)
    try {
      val sequencesToInsert = ids.map(
        id =>
          SequenceInsertSchema(
            externalId = Some(id),
            name = Some(s"a|${key}"),
            columns = (1 to 200).map(
              i =>
                SequenceColumnCreate(
                  name = Some("col" + i),
                  externalId = "col" + i,
                  description = Some("col1 description"),
                  valueType = "STRING",
                  metadata = Some(Map("foo" -> "bar"))
              ))
        ))

      ingests(sequencesToInsert)

      val sequences = retryWhile[Array[Row]](
        spark.sql(s"select * from sequences where name = 'a|$key'").collect,
        _.length != 650)
      sequences.length shouldBe 650
      sequences.head.getAs[String]("name").startsWith("a|") shouldBe true

    } finally {
      try {
        cleanupSequences(ids)
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  def ingests(
      tree: Seq[SequenceInsertSchema],
      metricsPrefix: Option[String] = None,
      conflictMode: String = "abort"
  ): Unit = {
    val processedTree = tree
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
    val storedCheckedAssets =
      writeClient.sequences.retrieveByExternalIds(checkedAssets.map(_.externalId.get))

    // check that the sequences are inserted correctly, before failing on long retries
    for ((inserted, stored) <- checkedAssets.zip(storedCheckedAssets)) {
      assert(inserted.externalId == stored.externalId)
      assert(inserted.name == stored.name)
      assert(inserted.metadata.getOrElse(Map()) == stored.metadata.getOrElse(Map()))
      if (conflictMode != "update") {
        assert(
          inserted.columns.toList == stored.columns.toList.map(_.transformInto[SequenceColumnCreate]))
      }
      assert(inserted.description == stored.description)
      assert(inserted.assetId == stored.assetId)
      assert(inserted.dataSetId == stored.dataSetId)
    }
  }

  def cleanupSequences(ids: Seq[String]): Unit =
    writeClient.sequences.deleteByExternalIds(ids)

}
