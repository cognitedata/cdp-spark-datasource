package cognite.spark.v1

import com.cognite.sdk.scala.common.sequenceColumnToCreateTransformer
import com.cognite.sdk.scala.v1.SequenceColumnCreate
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class SequenceRowsRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
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
      .createOrReplaceTempView("numbers_create")

    insertRows(
      sequenceId,
      spark
        .sql(s"select value as rowNumber, '$sequenceId' as externalId, 'abc' as str1, 1.1 as num2, value * 6 as num1 from numbers_create"))
    getNumberOfRowsCreated(sequenceId, "sequencerows") shouldBe 100

    val allColumns = retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length < 100
    )
    allColumns should have length 100
    allColumns(0).schema.fieldNames shouldBe Array("rowNumber", "externalId", "num1", "str1", "num2")
    allColumns(0).get(1) shouldBe 1L
    allColumns(0).get(2) shouldBe 6L
    allColumns(0).get(3) shouldBe "abc"
    allColumns(0).get(4) shouldBe 1.1

    val sparkReadResult = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequencerows")
      .option("externalId", sequenceId)
      .option("metricsPrefix", sequenceId)
      .option("collectMetrics", true)
      .load()
      .collect
    getNumberOfRowsRead(sequenceId, "sequencerows") shouldBe 100

    sparkReadResult should contain theSameElementsAs(allColumns)

    val rowNumberOnly = retryWhile[Array[Row]](
      spark.sql(s"select rowNumber from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length < 100
    )
    rowNumberOnly(0).get(0) shouldBe 1L

    val differentOrderProjection = retryWhile[Array[Row]](
      spark.sql(s"select num1, num2, rowNumber, str1 from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length < 100
    )
    differentOrderProjection(0).get(2) shouldBe 1L
    differentOrderProjection(0).get(3) shouldBe "abc"
    differentOrderProjection(0).get(1) shouldBe 1.1
    differentOrderProjection(0).get(0) shouldBe 6L

    val oneColumn = retryWhile[Array[Row]](
      spark.sql(s"select num2 from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length < 100
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
    getNumberOfRowsCreated(sequenceId, "sequencerows") shouldBe 3

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
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
        .sql("select 1 as rowNumber, 1 as num1, 1.0 as num2, 'a' as str1"))

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      rows => rows.length != 1
    )

    insertRows(
      sequenceId,
      spark
        .sql("select 1 as rowNumber, 2 as num1"))

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      rows => rows.length != 1 || rows(0).getAs[Long]("num1") != 2 || rows(0).getAs[Double]("num2") < 1.0
    )
    rows(0).getAs[Long]("num1") shouldBe 2 // the updated value
    rows(0).getAs[Long]("str1") shouldBe "a" // an old value is not replaced
    rows(0).getAs[Double]("num2") shouldBe 1.0
  }

  it should "create and read many rows" in withSequences(Seq(sequenceB)) { case Seq(sequenceId) =>
    // exceed the page size
    val testSize = 20 * 1000
    spark.sparkContext
      .parallelize(1 to testSize)
      .toDF()
      .createOrReplaceTempView("numbers_many")
    insertRows(
      sequenceId,
      spark
        .sql("select value as rowNumber, value * 6 as num1 from numbers_many"))
    getNumberOfRowsCreated(sequenceId, "sequencerows") shouldBe testSize

    val allColumns = retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length != testSize
    )
    allColumns(0).schema.fieldNames shouldBe Array("rowNumber", "num1")
    allColumns.map(_.getAs[Long]("num1")) shouldBe (1 to testSize).map(_ * 6)
  }

  it should "create and delete rows" in withSequences(Seq(sequenceB)) { case Seq(sequenceId) =>
    spark.sparkContext
      .parallelize(1 to 100)
      .toDF()
      .createOrReplaceTempView("numbers_delete")
    insertRows(
      sequenceId,
      spark
        .sql("select value as rowNumber, value * 6 as num1 from numbers_delete"))

    getNumberOfRowsCreated(sequenceId, "sequencerows") shouldBe 100

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length != 100
    )

    // delete every second row
    insertRows(
      sequenceId,
      spark
        .sql("select value * 2 as rowNumber from numbers_delete"),
      "delete")

    // we count even the items that are not deleted
    getNumberOfRowsDeleted(sequenceId, "sequencerows") shouldBe 100

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length != 50
    )
  }

  private def testPushdown(sequenceId: String, query: String, shouldBeExact: Boolean = false) = {
    val prefix = shortRandomString()
    val sparkReadResult = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequencerows")
      .option("externalId", sequenceId)
      .option("metricsPrefix", prefix)
      .option("collectMetrics", "true")
      .load()
      .where(query)
      .collect
    if (shouldBeExact) {
      sparkReadResult.length shouldBe getNumberOfRowsRead(prefix, "sequencerows")
    }
    (sparkReadResult, getNumberOfRowsRead(prefix, "sequencerows"))
  }

  it should "support filter pushdown on rowNumber" in withSequences(Seq(sequenceB)) { case Seq(sequenceId) =>
    spark.sparkContext
      .parallelize(1 to 100)
      .toDF()
      .createOrReplaceTempView("numbers_pushdown")
    insertRows(
      sequenceId,
      spark
        .sql("select value as rowNumber, value * 6 as num1 from numbers_pushdown"))
    getNumberOfRowsCreated(sequenceId, "sequencerows") shouldBe 100

    retryWhile[Array[Row]](
      spark.sql(s"select * from sequencerows_${sequenceId} order by rowNumber").collect,
      _.length != 100
    )

    testPushdown(sequenceId, "rowNumber = 1", shouldBeExact = true)._2 shouldBe 1
    testPushdown(sequenceId, "rowNumber in (1, 2, 3, 4, 5, 6, 7, 8)", shouldBeExact = true)._2 shouldBe 8
    testPushdown(sequenceId, "rowNumber not in (1, 2, 3, 4, 5, 6, 7, 8)", shouldBeExact = true)._2 shouldBe 92
    testPushdown(sequenceId, "rowNumber < 50", shouldBeExact = true)._2 shouldBe 49
    testPushdown(sequenceId, "rowNumber <= 50 and rowNumber not in (30, 80)", shouldBeExact = true)._2 shouldBe 49
    testPushdown(sequenceId, "(rowNumber >= 50 and rowNumber != 80) or rowNumber = 2", shouldBeExact = true)._2 shouldBe 51
    testPushdown(sequenceId, "(rowNumber >= 50 and rowNumber != 80 and rowNumber > 50) or rowNumber = 2", shouldBeExact = true)._2 shouldBe 50
    testPushdown(sequenceId, "(rowNumber >= 50 and rowNumber < 60) or (rowNumber >= 70 and rowNumber < 80) or (rowNumber >= 10 and rowNumber < 20)", shouldBeExact = true)._2 shouldBe 30
    testPushdown(sequenceId, "rowNumber in (2, NULL, 1)", shouldBeExact = true)._2 shouldBe 2
    val (readResult, numberRead) = testPushdown(sequenceId, "rowNumber <= 50 and num1 <= 60")
    numberRead shouldBe 50
    readResult.length shouldBe 10
    // should not read more rows, even though the disjunction would suggest that
    testPushdown(sequenceId, "rowNumber <= 50 or rowNumber >= 30", shouldBeExact = true)._2 shouldBe 100
    testPushdown(sequenceId, "(rowNumber < 50 and rowNumber >= 30) or (rowNumber >= 40 and rowNumber < 60)", shouldBeExact = true)._2 shouldBe 30
    testPushdown(sequenceId, "(rowNumber < 50 and rowNumber >= 30 and num1 > 0) or (rowNumber >= 40 and rowNumber < 60 and num1 < 3000)", shouldBeExact = true)._2 shouldBe 30
    val (readResult2, numberRead2) = testPushdown(sequenceId, "rowNumber <= 50 and (rowNumber >= 30 or num1 <= 30)")
    numberRead2 shouldBe 50
    readResult2.length shouldBe 26
  }

  // ----------

  def withSequences(sequences: Seq[SequenceUpdateSchema])(testCode: Seq[String] => Unit): Unit = {
    val key = shortRandomString()
    createSequences(key, sequences)
    for (s <- sequences) {
      createRowsRelation(s"${s.externalId.get}_$key").createOrReplaceTempView(s"sequencerows_${s.externalId.get}_$key")
    }
    try {
      testCode(sequences.map(s => s"${s.externalId.get}_$key"))
    } finally {
      cleanupSequence(key, sequences.map(_.externalId.get) :_*)
    }
  }

  def createRowsRelation(externalId: String) =
    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "sequencerows")
      .option("externalId", externalId)
      .load()

  def insertRows(seqId: String, df: DataFrame, onconflict: String = "upsert") =
    df.write
      .format("cognite.spark.v1")
      .option("type", "sequencerows")
      .option("apiKey", writeApiKey)
      .option("externalId", seqId)
      .option("onconflict", onconflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", seqId)
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
          externalId = s.externalId.map(id => s"${id}_$key")
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
      println(s"Inserted ${inserted.externalId}")
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
    writeClient.sequences.deleteByExternalIds(ids.map(id => s"${id}_$key"))

}
