package cognite.spark.v1

import java.time.Instant
import cognite.spark.v1.SparkSchemaHelper.fromRow
import com.cognite.sdk.scala.v1.{CogniteExternalId, RelationshipCreate}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class RelationshipsRelationTest extends FlatSpec with Matchers with SparkTest with Inspectors {

  val destinationDf: DataFrame = spark.read
    .format("cognite.spark.v1")
    .option("apiKey", writeApiKey)
    .option("type", "relationships")
    .load()
  destinationDf.createOrReplaceTempView("destinationRelationship")

  private def getBaseReader(metricsPrefix: String): DataFrame =
    spark.read
      .format("cognite.spark.v1")
      .option("type", "relationships")
      .option("apiKey", writeApiKey)
      .option("collectMetrics", true)
      .option("metricsPrefix", metricsPrefix)
      .load()

  val labelList = Seq(CogniteExternalId(externalId = "scala-sdk-relationships-test-label1"))
  val dataSetId = 86163806167772L
  val assetExtId1 = "scala-sdk-relationships-test-asset1"
  val assetExtId2 = "scala-sdk-relationships-test-asset2"
  val eventExtId1 = "scala-sdk-relationships-test-event2"
  val externalIdPrefix = s"sparktest-relationship-${shortRandomString()}"

  def createResources(externalIdPrefix: String): Unit =
    writeClient.relationships.create(
      Seq(
        RelationshipCreate(
          externalId = s"${externalIdPrefix}-1",
          sourceExternalId = assetExtId1,
          sourceType = "asset",
          targetExternalId = assetExtId2,
          targetType = "asset",
          confidence = Some(0.3),
          startTime = Some(Instant.ofEpochMilli(1601565769000L)),
          endTime = Some(Instant.ofEpochMilli(1603207369000L)),
          dataSetId = Some(dataSetId)
        ),
        RelationshipCreate(
          externalId = s"${externalIdPrefix}-2",
          sourceExternalId = assetExtId2,
          sourceType = "asset",
          targetExternalId = assetExtId1,
          targetType = "asset",
          confidence = Some(0.7),
          dataSetId = Some(dataSetId)
        ),
        RelationshipCreate(
          externalId = s"${externalIdPrefix}-3",
          sourceExternalId = assetExtId1,
          sourceType = "asset",
          targetExternalId = assetExtId2,
          targetType = "asset",
          labels = Some(labelList),
          dataSetId = Some(dataSetId)
        ),
        RelationshipCreate(
          externalId = s"${externalIdPrefix}-4",
          sourceExternalId = eventExtId1,
          sourceType = "event",
          targetExternalId = assetExtId2,
          targetType = "asset",
          startTime = Some(Instant.ofEpochMilli(1604244169000L)),
          labels = Some(labelList),
          dataSetId = Some(dataSetId)
        )
      )
    )

  it should "be able to read a relationship" taggedAs (ReadTest) in {
    val externalId = s"sparktest-relationship-${shortRandomString()}"
    writeClient.relationships.create(
      Seq(
        RelationshipCreate(
          externalId = externalId,
          sourceExternalId = assetExtId1,
          sourceType = "asset",
          targetExternalId = assetExtId2,
          targetType = "asset",
          labels = Some(labelList),
          dataSetId = Some(dataSetId)
        ))
    )

    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "relationships")
      .load()
      .where(s"externalId = '$externalId'")
      .collect()

    assert(rows.length == 1)
    val relationship = fromRow[RelationshipsReadSchema](rows.head)
    assert(relationship.externalId == externalId)
    assert(relationship.sourceExternalId == assetExtId1)
    assert(relationship.sourceType == "asset")
    assert(relationship.targetExternalId == assetExtId2)
    assert(relationship.targetType == "asset")
    assert(relationship.labels.isDefined && relationship.labels.get.head == labelList.head.externalId)

    writeClient.relationships.deleteByExternalId(externalId)
  }

  it should "be able to write a relationship" taggedAs (WriteTest) in {
    val externalId = s"sparktest-relationship-${shortRandomString()}"

    spark
      .sql(s"""select '$externalId' as externalId,
           |'$assetExtId1' as sourceExternalId,
           |'asset' as sourceType,
           |'$assetExtId2' as targetExternalId,
           |'asset' as targetType,
           | array('scala-sdk-relationships-test-label1') as labels,
           | 0.7 as confidence,
           | cast(from_unixtime(0) as timestamp) as startTime,
           | cast(from_unixtime(1) as timestamp) as endTime""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("type", "relationships")
      .option("apiKey", writeApiKey)
      .save()

    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "relationships")
      .load()
      .where(s"externalId = '$externalId'")
      .collect()

    assert(rows.length == 1)
    writeClient.relationships.deleteByExternalId(externalId)
  }

  it should "create some relationships up before filter tests" taggedAs (ReadTest) in {
    createResources(externalIdPrefix)
  }

  it should "be able to update a relationship" taggedAs ReadTest in forAll(updateAndUpsert) { updateMode =>
    val externalId = s"relationship-update-${shortRandomString()}"
    writeClient.relationships.create(
      Seq(
        RelationshipCreate(
          externalId = externalId,
          sourceExternalId = assetExtId1,
          sourceType = "asset",
          targetExternalId = assetExtId2,
          targetType = "asset",
          labels = Some(labelList),
          dataSetId = Some(dataSetId)
        ))
    )

    spark
      .sql(s"""select '$externalId' as externalId,
           |'$assetExtId1' as sourceExternalId,
           |'asset' as sourceType,
           |'$eventExtId1' as targetExternalId,
           |'event' as targetType,
           | array('scala-sdk-relationships-test-label1') as labels,
           | 0.35 as confidence,
           | cast(from_unixtime(0) as timestamp) as startTime,
           | cast(from_unixtime(1) as timestamp) as endTime""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("type", "relationships")
      .option("apiKey", writeApiKey)
      .option("onconflict", updateMode)
      .option("collectMetrics", "true")
      .save()

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationRelationship where externalId = '$externalId'").collect,
      rows => rows.length < 1
    )

    assert(rows.length == 1)
    val relationship = fromRow[RelationshipsReadSchema](rows.head)
    assert(relationship.sourceExternalId == s"$assetExtId1")
    assert(relationship.targetExternalId == s"$eventExtId1")
    assert(relationship.confidence.get == 0.35)
    assert(relationship.sourceType == "asset")
    assert(relationship.targetType == "event")

    writeClient.relationships.deleteByExternalId(externalId)
  }

  it should "be able to upsert a relationship" taggedAs ReadTest in {
    val externalId = s"$externalIdPrefix-${shortRandomString()}"
    spark
      .sql(s"""select '$externalId' as externalId,
           |'$assetExtId1-123' as sourceExternalId,
           |'asset' as sourceType,
           |'$assetExtId2-123' as targetExternalId,
           |'asset' as targetType,
           | array('scala-sdk-relationships-test-label1') as labels,
           | 0.6 as confidence,
           | cast(from_unixtime(0) as timestamp) as startTime,
           | cast(from_unixtime(1) as timestamp) as endTime""".stripMargin)
      .write
      .format("cognite.spark.v1")
      .option("type", "relationships")
      .option("apiKey", writeApiKey)
      .option("onconflict", "upsert")
      .option("collectMetrics", "true")
      .save()

    val rows = retryWhile[Array[Row]](
      spark.sql(s"select * from destinationRelationship where externalId = '$externalId'").collect,
      rows => rows.length < 1
    )

    assert(rows.length == 1)
    val relationship = fromRow[RelationshipsReadSchema](rows.head)
    assert(relationship.externalId == externalId)
    assert(relationship.sourceExternalId == s"$assetExtId1-123")
    assert(relationship.targetExternalId == s"$assetExtId2-123")
    assert(relationship.confidence.get == 0.6)
    assert(relationship.targetType == "asset")

    writeClient.relationships.deleteByExternalId(externalId)
  }

  it should "support pushdown filters with nulls" taggedAs (ReadTest) in {
    val metricsPrefix = s"pushdown.filter.nulls.${shortRandomString()}"
    val df = getBaseReader(metricsPrefix)
      .where(s"sourceExternalId in('${assetExtId2}', NULL)")
    assert(df.count == 1)
    val relationshipsRead = getNumberOfRowsRead(metricsPrefix, "relationships")
    assert(relationshipsRead == 1)
  }

  it should "support filtering on null" taggedAs (ReadTest) in {
    val countRows = spark
      .sql(
        s"select * from destinationRelationship where confidence is null and dataSetId = ${dataSetId}")
      .count
    assert(countRows == 2)
  }

  it should "get exception on invalid query" taggedAs (ReadTest) in {
    val metricsPrefix = s"pushdown.filter.invalid.${shortRandomString()}"
    val df = getBaseReader(metricsPrefix)
      .where("dataSetId = 0")

    val thrown = the[SparkException] thrownBy df.count()
    thrown.getMessage should include("id got 0, expected more than 0")
  }

  it should "apply a single pushdown filter" taggedAs (ReadTest) in {
    val metricsPrefix = s"single.pushdown.filter.${shortRandomString()}"
    val df = getBaseReader(metricsPrefix)
      .where(s"sourceExternalId = '${assetExtId2}'")

    assert(df.count == 1)
    val relationshipsRead = getNumberOfRowsRead(metricsPrefix, "relationships")
    assert(relationshipsRead == 1)
  }

  it should "support pushdown filters on sourceExternalId" taggedAs (ReadTest) in {
    val countRowsIn = spark.sql(s"""select * from destinationRelationship
         |where sourceExternalId in('${assetExtId1}', 'nonExistingSource')""".stripMargin).count
    assert(countRowsIn == 2)

    val countRows = spark.sql(s"""select * from destinationRelationship
         |where sourceExternalId = '${eventExtId1}'""".stripMargin).count
    assert(countRows == 1)
  }

  it should "support pushdown filters on targetExternalId" taggedAs (ReadTest) in {
    val countRowsIn = spark.sql(s"""select * from destinationRelationship
         |where targetExternalId in('${assetExtId2}', 'nonExistingTarget')""".stripMargin).count
    assert(countRowsIn == 3)

    val countRows = spark.sql(s"""select * from destinationRelationship
         |where targetExternalId = '${assetExtId2}'""".stripMargin).count
    assert(countRows == 3)

    val countRowsZero = spark.sql(s"""select * from destinationRelationship
         |where targetExternalId = 'nonExistingTarget'""".stripMargin).count
    assert(countRowsZero == 0)
  }

  it should "support pushdown filters on sourceType" taggedAs (ReadTest) in {
    val countRows = spark.sql(s"""select * from destinationRelationship
         |where sourceType = 'event' and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countRows == 1)

    val countRowsIn = spark.sql(s"""select * from destinationRelationship
         |where sourceType in('asset', 'timeSeries') and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countRowsIn == 3)
  }

  it should "support pushdown filters on targetType" taggedAs (ReadTest) in {
    val countRows = spark.sql(s"""select * from destinationRelationship
         |where targetType = 'asset' and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countRows == 4)
  }

  it should "handle pushdown filters on startTime" taggedAs (ReadTest) in {
    val countMaxStartTime = spark.sql(s"""select * from destinationRelationship
         |where startTime < cast(from_unixtime(1601565779) as timestamp)
         |and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countMaxStartTime == 1)

    val countMinStartTime = spark.sql(s"""select * from destinationRelationship
         |where startTime > cast(from_unixtime(1601565779) as timestamp)
         |and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countMinStartTime == 1)

    val countNullStartTime = spark.sql(s"""select * from destinationRelationship
         |where startTime is null and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countNullStartTime == 2)
  }

  it should "handle pushdown filters on endTime" taggedAs (ReadTest) in {
    val countMaxStartTime = spark
      .sql(s"""select * from destinationRelationship
         |where endTime <= cast(from_unixtime(1603207379) as timestamp) and dataSetId = ${dataSetId}""".stripMargin)
      .count
    assert(countMaxStartTime == 1)

    val countMinStartTime = spark
      .sql(s"""select * from destinationRelationship
         |where endTime > cast(from_unixtime(1603207379) as timestamp) and dataSetId = ${dataSetId}""".stripMargin)
      .count
    assert(countMinStartTime == 0)

    val countNullStartTime = spark.sql(s"""select * from destinationRelationship
         |where endTime is null and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countNullStartTime == 3)
  }

  it should "handle pushdown filters on confidence" taggedAs (ReadTest) in {
    val countMaxConfidence = spark.sql(s"""select * from destinationRelationship
         |where confidence < 0.4 and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countMaxConfidence == 1)

    val countMinConfidence = spark.sql(s"""select * from destinationRelationship
         |where confidence > 0.4 and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countMinConfidence == 1)

    val countNullConfidence = spark.sql(s"""select * from destinationRelationship
         |where confidence is null and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countNullConfidence == 2)
  }

  it should "handle pushdown filters on labels" taggedAs (ReadTest) in {
    val countLabelsEqual = spark
      .sql(s"""select * from destinationRelationship
         |where labels = array('scala-sdk-relationships-test-label1') and dataSetId = ${dataSetId}""".stripMargin)
      .count
    assert(countLabelsEqual == 2)

    val countLabelsIn = spark.sql(s"""select * from destinationRelationship
         |where labels in(array('scala-sdk-relationships-test-label1'), NULL, array('madeUpLabel', 'someMore'))
         | and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countLabelsIn == 2)

    val countLabelsNull = spark.sql(s"""select * from destinationRelationship
         |where labels is null and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countLabelsNull == 2)
  }

  it should "handle and, or and in() in one query" taggedAs (ReadTest) in {
    val countRows = spark.sql(s"""select * from destinationRelationship
         |where (sourceType = 'asset' or labels in(array('scala-sdk-relationships-test-label1'), NULL))
         | and dataSetId = ${dataSetId}""".stripMargin).count
    assert(countRows == 4)

    val countRows2 = spark.sql(s"""select * from destinationRelationship
         |where (sourceType = 'asset' or size(labels) != 0 or startTime > cast(from_unixtime(1603207369) as timestamp))
         |and endTime is null and  dataSetId = ${dataSetId}""".stripMargin).count
    assert(countRows2 == 3)
  }

  it should "be able to delete relationships" taggedAs (WriteTest) in {
    spark
      .sql(
        s"select externalId from destinationRelationship where externalId in('${externalIdPrefix}-1','${externalIdPrefix}-2','${externalIdPrefix}-3', '${externalIdPrefix}-4')")
      .write
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "relationships")
      .option("onconflict", "delete")
      .save()

    val rows = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", writeApiKey)
      .option("type", "relationships")
      .load()
      .where(
        s"externalId in('${externalIdPrefix}-1','${externalIdPrefix}-2','${externalIdPrefix}-3', '${externalIdPrefix}-4')")
      .collect()
    assert(rows.isEmpty)
  }

}
