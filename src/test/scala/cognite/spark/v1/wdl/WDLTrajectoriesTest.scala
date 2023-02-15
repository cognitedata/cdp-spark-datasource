package cognite.spark.v1.wdl

import cognite.spark.v1.{DataFrameMatcher, SparkTest}
import org.apache.spark.SparkException
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors}

class WDLTrajectoriesTest
    extends FlatSpec
    with SparkTest
    with Inspectors
    with DataFrameMatcher
    with BeforeAndAfter {

  import RowEquality._

  private val sparkReader = spark.read
    .format("cognite.spark.v1")
    .option("project", "jetfiretest2")
    .option("type", "welldatalayer")
    .useOIDCWrite

  val testClient = new TestWdlClient(writeClient)

  before {
    testClient.deleteAll()
    testClient.miniSetup()
  }

  val rawTrajectoryRowsDF: Unit = spark.read
    .json("src/test/resources/wdl-test-raw-trajectory-rows.jsonl")
    .createOrReplaceTempView("wdl_test_raw_trajectory_rows")

  val rawTrajectoriesDF: Unit = spark.read
    .json("src/test/resources/wdl-test-raw-trajectories.jsonl")
    .createOrReplaceTempView("wdl_test_raw_trajectories")

  val getTrajectoryRowsCTE: String =
    """with r as (
      |  select wellboreAssetExternalId,
      |    collect_list(
      |      struct(
      |        azimuth,
      |        inclination,
      |        measuredDepth,
      |        doglegSeverity
      |      )
      |    ) as rows
      |   from wdl_test_raw_trajectory_rows
      |   group by wellboreAssetExternalId
      | )""".stripMargin

  val getTrajectoriesExpr: String =
    """select
      |   t.wellboreAssetExternalId,
      |   struct(t.sourceName, t.externalId as sequenceExternalId) as source,
      |   t.type,
      |   t.measuredDepthUnit,
      |   t.inclinationUnit,
      |   t.azimuthUnit,
      |   t.doglegSeverityUnit,
      |   t.phase,
      |   r.rows as rows
      | from wdl_test_raw_trajectories as t
      | left join r on r.wellboreAssetExternalId = t.wellboreAssetExternalId
      |""".stripMargin

  it should "ingest and read bent trajectory" in {
    val testIngestionsDF = spark
      .sql(s"""$getTrajectoryRowsCTE
           | $getTrajectoriesExpr
           |""".stripMargin)
      .filter("""wellboreAssetExternalId = "A:wb1"""")

    testIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "TrajectoryIngestion")
      .useOIDCWrite
      .save()

    val expectedTrajectoriesDF = spark.read
      .schema(testClient.getSchema("Trajectory"))
      .json("src/test/resources/wdl-test-expected-trajectories.jsonl")
      .filter("""wellboreAssetExternalId = "A:wb1"""")

    val trajectoriesDF = sparkReader
      .option("wdlDataType", "Trajectory")
      .load()

    (expectedTrajectoriesDF.collect() should contain).theSameElementsAs(trajectoriesDF.collect())
  }

  it should "ingest and read trajectory in feet" in {
    val testIngestionsDF = spark
      .sql(s"""$getTrajectoryRowsCTE
           | $getTrajectoriesExpr
           |""".stripMargin)
      .filter("""wellboreAssetExternalId = "A:wb2"""")

    testIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "TrajectoryIngestion")
      .useOIDCWrite
      .save()

    val expectedTrajectoriesDF = spark.read
      .schema(testClient.getSchema("Trajectory"))
      .json("src/test/resources/wdl-test-expected-trajectories.jsonl")
      .filter("""wellboreAssetExternalId = "A:wb2"""")

    val trajectoriesDF = sparkReader
      .option("wdlDataType", "Trajectory")
      .load()

    (expectedTrajectoriesDF.collect() should contain).theSameElementsAs(trajectoriesDF.collect())
  }

  it should "fail while ingesting trajectory without rows" in {
    val testIngestionsDF = spark
      .sql("""select
          |   t.wellboreAssetExternalId,
          |   struct(t.sourceName, t.externalId as sequenceExternalId) as source,
          |   t.type,
          |   t.measuredDepthUnit,
          |   t.inclinationUnit,
          |   t.azimuthUnit,
          |   t.doglegSeverityUnit,
          |   t.phase,
          |   array() as rows
          | from wdl_test_raw_trajectories as t
          |""".stripMargin)
      .filter("""wellboreAssetExternalId = "A:wb1"""")

    a[SparkException] should be thrownBy testIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "TrajectoryIngestion")
      .useOIDCWrite
      .save()

    val trajectoriesDF = sparkReader
      .option("wdlDataType", "Trajectory")
      .load()

    trajectoriesDF shouldBe empty
  }

  it should "ingest and read multiple trajectories" in {
    val testIngestionsDF = spark
      .sql(s"""$getTrajectoryRowsCTE
           | $getTrajectoriesExpr
           |""".stripMargin)

    testIngestionsDF.write
      .format("cognite.spark.v1")
      .option("project", "jetfiretest2")
      .option("type", "welldatalayer")
      .option("wdlDataType", "TrajectoryIngestion")
      .useOIDCWrite
      .save()

    val expectedTrajectoriesDF = spark.read
      .schema(testClient.getSchema("Trajectory"))
      .json("src/test/resources/wdl-test-expected-trajectories.jsonl")

    val trajectoriesDF = sparkReader
      .option("wdlDataType", "Trajectory")
      .load()

    (expectedTrajectoriesDF.collect() should contain).theSameElementsAs(trajectoriesDF.collect())
  }
}
