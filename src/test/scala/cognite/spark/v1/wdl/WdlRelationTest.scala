package cognite.spark.v1.wdl

import cognite.spark.v1.SparkTest
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class WdlRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  it should "real all rows fra raw and create nds events" in {
    val clientId = sys.env("SH_COGNITE_CLIENT_ID")
    val clientSecret = sys.env("SH_COGNITE_CLIENT_SECRET")
    val tokenUri = sys.env("SH_COGNITE_TOKEN_URL")

    val rawReadDf = spark.read
      .format("cognite.spark.v1")
      .option("baseUrl", "https://greenfield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "sigurdholsen")
      .option("scopes", "https://greenfield.cognitedata.com/.default")
      .option("collectMetrics", true)
      .option("metricsPrefix", "sh_read_raw")
      .option("type", "raw")
      .option("database", "fdm")
      .option("table", "nds_events")
      .option("partitions", 50)
      .option("inferSchema", true)
      .option("parallelismPerPartition", 2)
      .load()

    rawReadDf.createTempView("nds_events")

    val ndsWriteDf = spark.sql(
      """
        |select
        |  struct(
        |    externalId as eventExternalId,
        |    'raw_fdm' as sourceName
        |  ) as source,
        |  description,
        |  cast(severity as integer) as severity,
        |  cast(probability as integer) as probability,
        |  riskType,
        |  struct('meter' as unit, holeDiameterInMeters as value) as holeDiameter,
        |  wellboreExternalId as wellboreAssetExternalId,
        |  struct(topMeasuredDepthInMeters as value, 'meter' as unit) as topMeasuredDepth,
        |  struct(baseMeasuredDepthInMeters as value, 'meter' as unit) as baseMeasuredDepth
        |from
        |  nds_events
        |""".stripMargin
    )

    println("Start writing nds events")

    val metricPrefix = "sh_created_nds_events"
    ndsWriteDf.write
      .format("cognite.spark.v1")
      .option("baseUrl", "https://greenfield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "sigurdholsen")
      .option("scopes", "https://greenfield.cognitedata.com/.default")
      .option("collectMetrics", value = true)
      .option("onconflict", "upsert")
      .option("metricsPrefix", metricPrefix)
      .option("type", "welldatalayer")
      .option("wdlDataType", "NdsIngestion")
      .option("partitions", 1)
      .option("parallelismPerPartition", 1)
      .save()

    val ndsEventsCreated = getNumberOfRowsCreated(metricPrefix, "welldatalayer")
    println(s"Nds evenst created: $ndsEventsCreated")
  }

}
