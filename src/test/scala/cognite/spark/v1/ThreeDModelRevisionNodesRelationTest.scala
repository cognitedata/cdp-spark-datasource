package cognite.spark.v1

import cognite.spark.v1.CdpConnector.ioRuntime
import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelRevisionNodesRelationTest extends FlatSpec with ParallelTestExecution with SparkTest {
  "ThreeDModelRevisionNodesRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val model = writeClient.threeDModels.list().compile.toList.unsafeRunSync().head
    val revision = writeClient.threeDRevisions(model.id).list().compile.toList.unsafeRunSync().head

    val df = spark.read
      .format("cognite.spark.v1")
      .useOIDCWrite
      .option("type", "3dmodelrevisionnodes")
      .option("modelid", model.id)
      .option("revisionid", revision.id)
      .option("limitPerPartition", "10")
      .load()
    assert(df.count() == 10)

  }
}
