package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.CdpConnector.ioRuntime
import natchez.noop.NoopEntrypoint
import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelRevisionMappingsRelationTest
    extends FlatSpec
    with ParallelTestExecution
    with SparkTest {
  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {

    val model = NoopEntrypoint[IO]()
      .root("list")
      .use(writeClient.threeDModels.list().compile.toList.run)
      .unsafeRunSync()
      .head
    val revision = {
      NoopEntrypoint[IO]()
        .root("list")
        .use(writeClient.threeDRevisions(model.id).list().compile.toList.run)
        .unsafeRunSync()
        .head
    }

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
