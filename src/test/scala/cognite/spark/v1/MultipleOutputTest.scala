package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}
import cognite.spark.v1.CdpConnector.ioRuntime

import scala.util.control.NonFatal

class MultipleOutputTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  it should "be possible to create assets and events simultaneously" taggedAs WriteTest in {
    val assetsTestSource = s"multi-relation-test-create-${shortRandomString()}"
    val assetExternalId = s"multi-relation-test-create-${shortRandomString()}"
    val eventExternalId = s"multi-relation-test-create-${shortRandomString()}"
    val metricsPrefix = s"assetsAndEvents.test.create.${shortRandomString()}"
    try {
      spark
        .sql(
          s"""
             |select struct(
             |'$assetExternalId' as externalId,
             |$testDataSetId as dataSetId,
             |'asset multi name' as name,
             |'asset description' as description,
             |'$assetsTestSource' as source,
             |array('scala-sdk-relationships-test-label1') as labels
             |) as a,
             |struct(
             |'$eventExternalId' as externalId,
             |$testDataSetId as dataSetId,
             |cast(0 as timestamp) as startTime,
             |cast(100 as timestamp) as endTime,
             |'event description' as description,
             |'event multi type' as type,
             |'subtype' as subtype,
             |array() as assetIds
             |) as e
    """.stripMargin)
        .write
        .format("cognite.spark.v1")
        .useOIDCWrite
        .option("type", "a:assets,e:events")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()

      val assetsCreated = getNumberOfRowsCreated(metricsPrefix, "assets")
      assert(assetsCreated == 1)
      val eventsCreated = getNumberOfRowsCreated(metricsPrefix, "events")
      assert(eventsCreated == 1)

      val createdAsset = writeClient.assets.retrieveByExternalId(assetExternalId).unsafeRunSync()
      createdAsset.name shouldBe "asset multi name"

      val createdEvent = writeClient.events.retrieveByExternalId(eventExternalId).unsafeRunSync()
      createdEvent.`type` shouldBe Some("event multi type")


    } finally {
      try {
        writeClient.assets.deleteByExternalId(assetExternalId).unsafeRunSync()
        writeClient.events.deleteByExternalId(eventExternalId).unsafeRunSync()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }
}
