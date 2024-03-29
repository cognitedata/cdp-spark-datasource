package cognite.spark.v1

import cognite.spark.v1.CdpConnector.ioRuntime
import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelRevisionMappingsRelationTest
    extends FlatSpec
    with ParallelTestExecution
    with SparkTest {
  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val model = writeClient.threeDModels.list().compile.toList.unsafeRunSync().head
    val revision = writeClient.threeDRevisions(model.id).list().compile.toList.unsafeRunSync().head

    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "3dmodelrevisionmappings")
      .option("modelid", model.id)
      .option("revisionid", revision.id)
      .load()
    assert(df.count() == 1)
  }
}
