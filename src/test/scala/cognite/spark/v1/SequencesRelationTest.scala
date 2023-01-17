package cognite.spark.v1

import cats.data.NonEmptyList
import cognite.spark.v1.CdpConnector.ioRuntime
import com.cognite.sdk.scala.v1.{SequenceColumnCreate, SequenceCreate}
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers, OptionValues, ParallelTestExecution}

import java.util.UUID
import org.apache.spark.SparkException

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
    .option("tokenUri", OIDCWrite.tokenUri)
    .option("clientId", OIDCWrite.clientId)
    .option("clientSecret", OIDCWrite.clientSecret)
    .option("project", OIDCWrite.project)
    .option("scopes", OIDCWrite.scopes)
    .option("type", "sequences")
    .load()
  sequencesSourceDf.createOrReplaceTempView("sequences")

  it should "create and read sequence" in {
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
      spark.sql(s"select * from sequences where externalId = '$id'").collect(),
      _.length != 1)
    sequences.length shouldBe 1
    sequences.head.getAs[String]("name") shouldBe "a"

    val columns = retryWhile[Array[Row]](
      spark
        .sql(s"select c.* from (select explode(columns) as c from sequences where externalId = '$id')")
        .collect(),
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
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "sequences")
      .option("onconflict", "abort")
      .save()

    val sequence = writeClient.sequences.retrieveByExternalId(s"$id").unsafeRunSync()
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
      spark.sql(s"select * from sequences where externalId = '$id'").collect(),
      rows => rows.length != 1)

    val sequenceUpdate = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      description = Some("description abc"),
      columns = Seq(
        SequenceColumnCreate(
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
        .collect(),
      rows => rows.length != 1)

    val colHead = writeClient.sequences.retrieveByExternalId(s"$id").unsafeRunSync().columns.head
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
          name = Some("col_will_be_updated_name"),
          externalId = "col_will_be_updated",
          description = Some("col_will_be_updated_description"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        ),
        SequenceColumnCreate(
          name = Some("col_will_be_removed_name"),
          externalId = "col_will_be_removed",
          description = Some("col_will_be_removed_description"),
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
              |               'name', 'col_updated_name',
              |               'description', 'col_updated_description',
              |               'externalId', 'col_updated',
              |               'valueType', NULL
              |           ),
              |           named_struct(
              |               'metadata', map('m1', 'v1', 'm2', 'v2'),
              |               'name', 'new_col_added_name',
              |               'description', 'new_col_added_description',
              |               'externalId', 'new_col_added',
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
              |               'name', 'new_sequence_new_column_name',
              |               'description', 'new_sequence_new_column_description',
              |               'externalId', 'new_sequence_new_column',
              |               'valueType', 'STRING'
              |           )
              |       ) as columns
              |""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "sequences")
      .option("onconflict", "upsert")
      .save()

    val sequence1 = writeClient.sequences.retrieveByExternalId(id).unsafeRunSync()
    val columns1 = sequence1.columns
    val sequence2 = writeClient.sequences.retrieveByExternalId(s"$id-2").unsafeRunSync()
    val columns2 = sequence2.columns

    sequence1.name shouldBe Some("seq name1")
    sequence1.description shouldBe Some("desc1")

    sequence2.name shouldBe Some("seq name2")
    sequence2.description shouldBe Some("desc2")

    columns1.toList.flatMap(_.name).toSet shouldBe Set("col_updated_name", "new_col_added_name")
    columns1.toList.flatMap(_.externalId).toSet shouldBe Set("col_updated", "new_col_added")
    columns1.toList.flatMap(_.description).toSet shouldBe Set(
      "col_updated_description",
      "new_col_added_description")
    columns1.toList.flatMap(_.metadata).toSet shouldBe Set(
      Map("m1" -> "v1"),
      Map("m1" -> "v1", "m2" -> "v2"))
    columns1.map(_.valueType).toList shouldBe List("STRING", "STRING")

    columns2.head.name shouldBe Some("new_sequence_new_column_name")
    columns2.head.externalId shouldBe Some("new_sequence_new_column")
    columns2.head.description shouldBe Some("new_sequence_new_column_description")
    columns2.head.metadata shouldBe Some(Map("foo" -> "bar"))
    columns2.head.valueType shouldBe "STRING"

    cleanupSequences(Seq(id, s"$id-2"))
  }

  it should "not update columns when none provided" in {
    val id = UUID.randomUUID().toString
    val sequenceToCreate = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      columns = Seq(
        SequenceColumnCreate(
          name = Some("c1"),
          externalId = "c_c1",
          description = Some("hehe"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      )
    )

    ingests(Seq(sequenceToCreate))

    spark
      .sql(s"""select '$id' as externalId,
              |'xD' as name,
              |'lol' as description""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "sequences")
      .option("onconflict", "update")
      .save()

    val sequence = writeClient.sequences.retrieveByExternalId(id).unsafeRunSync()
    val columns = sequence.columns

    sequence.name shouldBe Some("xD")
    sequence.description shouldBe Some("lol")

    columns.head.name shouldBe Some("c1")
    columns.head.externalId shouldBe Some("c_c1")
    columns.head.description shouldBe Some("hehe")
    columns.head.metadata shouldBe Some(Map("foo" -> "bar"))
    columns.head.valueType shouldBe "STRING"

    cleanupSequences(Seq(id))
  }

  it should "return error when column valueType is attempted to be changed" in {
    val id = UUID.randomUUID().toString
    val sequenceToCreate = SequenceInsertSchema(
      externalId = Some(id),
      name = Some("a"),
      columns = Seq(
        SequenceColumnCreate(
          name = Some("hey"),
          externalId = "hey",
          description = Some("hey"),
          valueType = "STRING",
          metadata = Some(Map("foo" -> "bar"))
        )
      )
    )

    ingests(Seq(sequenceToCreate))

    val exception = intercept[SparkException] {
      spark
        .sql(s"""select '$id' as externalId,
             |'xD' as name,
             |'lol' as description,
             |array(
             |   named_struct(
             |     'metadata', map('foo', 'bar', 'nothing', NULL),
             |     'name', 'hey',
             |     'description', 'hey',
             |     'externalId', 'hey',
             |     'valueType', 'LONG'
             |   )
             |) as columns""".stripMargin)
        .write
        .format("cognite.spark.v1")
        .option("tokenUri", OIDCWrite.tokenUri)
        .option("clientId", OIDCWrite.clientId)
        .option("clientSecret", OIDCWrite.clientSecret)
        .option("project", OIDCWrite.project)
        .option("scopes", OIDCWrite.scopes)
        .option("type", "sequences")
        .option("onconflict", "update")
        .save()
    }

    cleanupSequences(Seq(id))

    exception.getMessage should
      include(
        "Column valueType cannot be modified: the previous value is STRING and the user attempted to update it with LONG")
  }

  it should "chunk sequence if more than 10000 columns in the request" in {
    Seq("abort", "upsert").foreach { mode =>
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

        ingests(sequencesToInsert, conflictMode = mode)

        val sequences = retryWhile[Array[Row]](
          spark.sql(s"select * from sequences where name = 'a|$key'").collect(),
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
  }

  it should "delete sequence" in {
    val key = UUID.randomUUID().toString
    writeClient.sequences
      .createOne(SequenceCreate(
        Some(s"name-${key}"),
        Some("description"),
        None,
        Some(s"externalId-$key"),
        None,
        NonEmptyList.fromListUnsafe(
          List(SequenceColumnCreate(Some("col1"), "col1", None, "STRING", None)))
      ))
      .unsafeRunSync()
    val idsAfterDelete = retryWhile[Array[Row]](
      {
        spark
          .sql(s"select id from sequences where externalId = 'externalId-$key'")
          .write
          .format("cognite.spark.v1")
          .option("tokenUri", OIDCWrite.tokenUri)
          .option("clientId", OIDCWrite.clientId)
          .option("clientSecret", OIDCWrite.clientSecret)
          .option("project", OIDCWrite.project)
          .option("scopes", OIDCWrite.scopes)
          .option("type", "sequences")
          .option("onconflict", "delete")
          .option("collectMetrics", "true")
          .save()
        spark
          .sql(s"select id from sequences where externalId = 'externalId-$key'")
          .collect()
      },
      df => df.length > 0
    )
    idsAfterDelete.isEmpty shouldBe true
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
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "sequences")
      .option("onconflict", conflictMode)
      .option("collectMetrics", metricsPrefix.isDefined)
      .option("metricsPrefix", metricsPrefix.getOrElse(""))
      .save()

    val checkedAssets = processedTree.filter(_.externalId.isDefined)
    val storedCheckedAssets =
      writeClient.sequences.retrieveByExternalIds(checkedAssets.map(_.externalId.get)).unsafeRunSync()

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
    writeClient.sequences.deleteByExternalIds(ids).unsafeRunSync()

}
