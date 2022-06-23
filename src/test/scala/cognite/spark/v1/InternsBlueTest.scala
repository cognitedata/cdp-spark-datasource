package cognite.spark.v1

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class InternsBlueTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with BeforeAndAfterAll {
  import CdpConnector.ioRuntime

  val clientId = "28d0091b-1773-4230-be4f-b7ec9478751b"
  val clientSecret = ""
  val aadTenant = "08201a88-16b3-421c-a291-0f6548092d69"
  val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"

  private def readRows() =
    spark.read
      .format("cognite.spark.v1")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "interns-blue")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("collectMetrics", true)
      .option("metricsPrefix", "assets")
      .option("type", "assets")
      .load()

  def insertRows(df: DataFrame): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", "alphadatamodelinstances")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "interns-blue")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("modelExternalId", "Asset")
      .option("spaceExternalId", "TEST")
      .option("instanceSpaceExternalId", "TEST")
      .option("onconflict", "upsert")
      .option("collectMetrics", true)
      .option("metricsPrefix", "Asset")
      .save

  it should "ingest data" in {
    readRows().createTempView("assets")

    insertRows(
      spark
        .sql(
          s"""select id, string(id) as externalId, name,
             |description, true as inUse,
             |float(1000*rand(100)) as weight,
             |"cdf" as source from assets""".stripMargin),
    )

  }

}
