package cognite.spark.v1

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class NOCAssetHierarchyTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  it should "noc repro" in {
    val apiKey = sys.env("NOC_DEV_API_KEY")

    val sql = """
select
  coalesce(b.`FunctionalLocation`, a.`FunctionalLocation`) as externalId,
  if(coalesce(b.`SuperiorFunctionalLocation`, a.`SuperiorFunctionalLocation`) == 'QA', '', coalesce(b.`SuperiorFunctionalLocation`, a.`SuperiorFunctionalLocation`))  as parentExternalId,
  "sap" as source,
  coalesce(element_at(split(b.`FunctionalLocation`,'\\.'), -1), element_at(split(a.`FunctionalLocation`,'\\.'), -1)) as name,
  coalesce(b.`DescOfFunctionalLocation`) as description,
  map(
    "Functional Loc.", coalesce(b.`FunctionalLocation`, a.`FunctionalLocation`),
    "MaintPlant", coalesce(b.`MaintenancePlant`,a.`MaintenancePlant`),
    "SupFunctLoc.", coalesce(b.`SuperiorFunctionalLocation`, a.`SuperiorFunctionalLocation`),
    "System status", coalesce(b.`SystemStatus`, a.`SystemStatus`),
    "User status", coalesce(b.`SystemStatus`, a.`SystemStatus`),
    "plant", concat("QA_", element_at(split(coalesce(b.`FunctionalLocation`, a.`FunctionalLocation`),'\\.'), 2))
  ) as metadata
from datadump as a -- DUMP RAW table
full outer join extractor as b -- LIVE Extractor table
on a.key == b.key
where a.key != "DB_INIT" and a.`FunctionalLocation` != 'QA'
"""

    def cdf(`type`: String): DataFrameReader =
      spark.read
        .format("cognite.spark.v1")
        .option("apiKey", apiKey)
        .option("type", `type`)

    def rawTable(db: String, table: String): DataFrame =
      cdf("raw")
        .option("database", db)
        .option("table", table)
        .option("batchSize", "1000")
        .option("inferSchema", "true")
        .option("inferSchemaLimit", 1000)
        .option("maxRetries", 10)
        .load()

    val datadump = rawTable("sap_datadump", "IH06")
    datadump.createOrReplaceTempView("datadump")

    val extractor = rawTable("sap_extractor_v3", "ih06")
    extractor.createOrReplaceTempView("extractor")

    val source = spark.sql(sql)

    val destination = {
      source.write
        .format("cognite.spark.v1")
        .option("apiKey", apiKey)
        .option("type", "assethierarchy")
    }

    destination.save()
  }
}
