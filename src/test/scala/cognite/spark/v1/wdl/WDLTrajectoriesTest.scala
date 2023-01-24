package cognite.spark.v1.wdl

import cognite.spark.v1.DataFrameMatcher
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLTrajectoriesTest
    extends FlatSpec
    with WDLSparkTest
    with Inspectors
    with DataFrameMatcher
    with BeforeAndAfter {

  import RowEquality._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  before {
    SQLConf.get.setConfString("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
    client.deleteAll()
    client.miniSetup()
  }

  it should "ingest and read bent trajectory" in {
    val testTrajectoryIngestionRowsDF = spark.read
      .json("src/test/resources/wdl-test-trajectory-rows.jsonl")
    testTrajectoryIngestionRowsDF.createOrReplaceTempView("wdl_test_trajectory_rows")

    val testTrajectoryIngestionsDF = spark.read
      .schema(client.getSchema("TrajectoryIngestion"))
      .json("src/test/resources/wdl-test-trajectories.jsonl")
    testTrajectoryIngestionsDF.createOrReplaceTempView("wdl_test_trajectories")

    testTrajectoryIngestionsDF.printSchema()

    val testIngestionsDF = spark
      .sql(s"""with r as (
           |  select wellboreAssetExternalId,
           |    collect_list(
           |      named_struct(
           |        "azimuth", azimuth,
           |        "inclination", inclination,
           |        "measuredDepth", measuredDepth,
           |        "doglegSeverity", doglegSeverity
           |      )
           |    ) as rows
           |   from wdl_test_trajectory_rows
           |   group by wellboreAssetExternalId
           | )
           | select
           |   t.wellboreAssetExternalId,
           |   t.source,
           |   t.type,
           |   t.measuredDepthUnit,
           |   t.inclinationUnit,
           |   t.azimuthUnit,
           |   t.doglegSeverityUnit,
           |   t.phase,
           |   r.rows as rows
           | from wdl_test_trajectories as t
           | left join r on r.wellboreAssetExternalId = t.wellboreAssetExternalId""".stripMargin)

//      .sql(
//        s"""with r as
//           | (select wellboreAssetExternalId, named_struct(measuredDepth, inclination, azimuth from wdl_test_trajectory_rows) as row)
//           | select *, to_json(collect_list(r.row)) as rows
//           | from wdl_test_trajectories
//           | join r on wellboreAssetExternalId""".stripMargin)

    testIngestionsDF.printSchema()
    testIngestionsDF.show(truncate = false)

    testIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "TrajectoryIngestion")
      .useOIDCWrite
      .save()

    val testTrajectoriesDF = spark.read
      .schema(client.getSchema("Trajectory"))
      .json("src/test/resources/wdl-test-expected-trajectories.jsonl")

    val TrajectoriesDF = sparkReader
      .option("wdlDataType", "Trajectory")
      .load()

    (testTrajectoriesDF.collect() should contain).theSameElementsAs(TrajectoriesDF.collect())
  }
}
