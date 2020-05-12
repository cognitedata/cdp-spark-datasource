package cognite.spark.v1

import cats.data.NonEmptyList
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{Sequence, SequenceColumn, SequenceColumnCreate}
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}

import scala.util.control.NonFatal

class SequenceRowsRelationTest extends FlatSpec with Matchers with SparkTest {
  import spark.implicits._

  private val sequencesSourceDf = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "sequences")
    .load()
  sequencesSourceDf.createOrReplaceTempView("sequences")

  val sequenceA = SequenceUpdateSchema(
    externalId = Some("a"),
    name = Some("Rows test sequence"),
    columns = Some(Seq(
      SequenceColumnCreate(
        externalId = "num1",
        valueType = "LONG"
      ),
      SequenceColumnCreate(
        externalId = "str1",
        valueType = "STRING"
      ),
      SequenceColumnCreate(
        externalId = "num2",
        valueType = "DOUBLE"
      )
    ))
  )
  val sequenceB = SequenceUpdateSchema(
    externalId = Some("b"),
    name = Some("Rows test many sequence"),
    columns = Some(
      Seq(
        SequenceColumnCreate(
          externalId = "num1",
          valueType = "LONG"
        )
      ))
  )

  it should "create and read rows" in withSequences(Seq(sequenceA)) { case Seq(sequenceId) =>

    spark.sparkContext
      .parallelize(1 to 100)
      .toDF()
      .createOrReplaceTempView("numbers")

    insertRows(
      sequenceId,
      spark
        .sql("select value as rowNumber, 'abc' as str1, 1.1 as num2, value * 6 as num1 from numbers"))

    val allColumns = retryWhile[Array[Row]](
      spark.sql(s"select * from sequenceRows_a order by rowNumber").collect,
      _.length != 100
    )
    allColumns(0).schema.fieldNames shouldBe Array("rowNumber", "num1", "str1", "num2")
    allColumns(0).get(0) shouldBe 1L
    allColumns(0).get(1) shouldBe 6L
    allColumns(0).get(2) shouldBe "abc"
    allColumns(0).get(3) shouldBe 1.1

    val rowNumberOnly = retryWhile[Array[Row]](
      spark.sql(s"select rowNumber from sequenceRows_a order by rowNumber").collect,
      _.length != 100
    )
    rowNumberOnly(0).get(0) shouldBe 1L

    val differentOrderProjection = retryWhile[Array[Row]](
      spark.sql(s"select num1, num2, rowNumber, str1 from sequenceRows_a order by rowNumber").collect,
      _.length != 100
    )
    differentOrderProjection(0).get(2) shouldBe 1L
    differentOrderProjection(0).get(3) shouldBe "abc"
    differentOrderProjection(0).get(1) shouldBe 1.1
    differentOrderProjection(0).get(0) shouldBe 6L

    val oneColumn = retryWhile[Array[Row]](
      spark.sql(s"select num2 from sequenceRows_a order by rowNumber").collect,
      _.length != 100
    )
    oneColumn.map(_.get(0)) shouldBe Seq.fill(100)(1.1)
  }
  it should "insert NULL values" in withSequences(Seq(sequenceA)) { case Seq(sequenceId) =>
    // num1, str1, num2
    insertRows(
      sequenceId,
      spark
        .sql("select 1 as rowNumber, 1 as num1"))

    insertRows(
      sequenceId,
      spark
        .sql("select 2 as rowNumber, NULL as num1, 'abc' as str1, 1 as num2"))

    insertRows(
      sequenceId,
      spark
        .sql("select 3 as rowNumber, 2 as num1, NULL as str1, NULL as num2"))

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from sequenceRows_a order by rowNumber").collect,
      _.length != 3
    )
    rows.map(_.getAs[Long]("rowNumber")) shouldBe Array(1, 2, 3)
    rows.map(_.getAs[Any]("num1")) shouldBe Array[Any](1L, null, 2L)
    rows.map(_.getAs[String]("str1")) shouldBe Array(null, "abc", null)
    rows.map(_.getAs[Any]("num2")) shouldBe Array[Any](null, 1.0, null)
  }

  it should "insert and update rows" in withSequences(Seq(sequenceA)) { case Seq(sequenceId) =>
    // num1, str1, num2
    insertRows(
      sequenceId,
      spark
        .sql("select 1 as rowNumber, 1 as num1, 1 as num2, 'a' as str1"))

    insertRows(
      sequenceId,
      spark
        .sql("select 1 as rowNumber, 2 as num1"))

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from sequenceRows_a order by rowNumber").collect,
      rows => rows.length != 1 || rows(0).getAs[Long]("num1") != 2
    )
    println(rows(0))
  }

  it should "create and read many rows" in withSequences(Seq(sequenceB)) { case Seq(sequenceId) =>
    // exceed the page size
    val testSize = 20 * 1000
    spark.sparkContext
      .parallelize(1 to testSize)
      .toDF()
      .createOrReplaceTempView("numbers")
    insertRows(
      sequenceId,
      spark
        .sql("select value as rowNumber, value * 6 as num1 from numbers"))

    createRowsRelation(sequenceId).createOrReplaceTempView("sequenceRows_b")

    val allColumns = retryWhile[Array[Row]](
      spark.sql(s"select * from sequenceRows_b order by rowNumber").collect,
      _.length != testSize
    )
    allColumns(0).schema.fieldNames shouldBe Array("rowNumber", "num1")
    allColumns.map(_.getAs[Long]("num1")) shouldBe (1 to testSize).map(_ * 6)
  }

  // ----------

  def withSequences(sequences: Seq[SequenceUpdateSchema])(testCode: Seq[String] => Unit): Unit = {
    val key = shortRandomString()
    createSequences(key, sequences)
    for (s <- sequences) {
      createRowsRelation(s"${s.externalId.get}|$key").createOrReplaceTempView(s"sequenceRows_${s.externalId.get}")
    }
    try {
      testCode(sequences.map(s => s"${s.externalId.get}|$key"))
    } finally {
      cleanupSequence(key, sequences.map(_.externalId.get) :_*)
    }
  }

  def createRowsRelation(externalId: String) =
    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequenceRows")
      .option("sequenceExternalId", externalId)
      .load()

  def insertRows(seqId: String, df: DataFrame) =
    df.write
      .format("cognite.spark.v1")
      .option("type", "sequenceRows")
      .option("apiKey", writeApiKey)
      .option("sequenceExternalId", seqId)
      .option("onconflict", "upsert")
      .save

  def createSequences(
      key: String,
      tree: Seq[SequenceUpdateSchema],
      metricsPrefix: Option[String] = None,
      conflictMode: String = "abort"
  ): Unit = {
    val processedTree = tree.map(
      s =>
        s.copy(
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
    val storedCheckedAssets =
      writeClient.sequences.retrieveByExternalIds(checkedAssets.map(_.externalId.get))

    // check that the sequences are inserted correctly, before failing on long retries
    for ((inserted, stored) <- checkedAssets.zip(storedCheckedAssets)) {
      assert(inserted.externalId == stored.externalId)
      assert(inserted.name == stored.name)
      assert(inserted.metadata.getOrElse(Map()) == stored.metadata.getOrElse(Map()))
      val columns = stored.columns.map(_.transformInto[SequenceColumnCreate]).toList
      inserted.columns.map(_.toList).foreach { c =>
        val col = c.map(c => c.copy(metadata = c.metadata.orElse(Some(Map()))))
        assert(col == columns)
      }
      assert(inserted.description == stored.description)
      assert(inserted.assetId == stored.assetId)
      assert(inserted.dataSetId == stored.dataSetId)
    }
  }

  def cleanupSequence(key: String, ids: String*): Unit =
    writeClient.sequences.deleteByExternalIds(ids.map(id => s"$id|$key"))

}
