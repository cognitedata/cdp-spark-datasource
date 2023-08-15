package cognite.spark.v1

import cognite.spark.v1.CdpConnector.ioRuntime
import com.cognite.sdk.scala.v1.{ThreeDModel, ThreeDRevision}
import natchez.noop.NoopSpan
import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelRevisionNodesRelationTest extends FlatSpec with ParallelTestExecution with SparkTest {
  private def getSomeModelRevision: TracedIO[(ThreeDModel, ThreeDRevision)] = {
    writeClient.threeDModels.list()
      .flatMap(model =>
        writeClient.threeDRevisions(model.id).list()
          .filter(_.status == "Done")
          .map((model, _))
      )
      .head
      .compile
      .toList
      .map(_.head)
  }

  "ThreeDModelRevisionNodesRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val (model, revision) = getSomeModelRevision.run(NoopSpan()).unsafeRunSync()

    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "3dmodelrevisionnodes")
      .option("modelid", model.id)
      .option("revisionid", revision.id)
      .option("limitPerPartition", "10")
      .load()
    assert(df.count() > 0)

  }
}
