package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.CdpConnector.ioRuntime
import natchez.noop.NoopEntrypoint
import org.scalatest.{FlatSpec, ParallelTestExecution}

class ThreeDModelRevisionsRelationTest extends FlatSpec with ParallelTestExecution with SparkTest {
  "ThreeDModelRevisionsRelationTest" should "pass a smoke test" taggedAs WriteTest in {
    val model = {
      NoopEntrypoint[IO]()
        .root("list")
        .use(writeClient.threeDModels.list().compile.toList.run)
        .unsafeRunSync()
        .head
    }

    val df = spark.read
      .format(DefaultSource.sparkFormatString)
      .useOIDCWrite
      .option("type", "3dmodelrevisions")
      .option("modelid", model.id)
      .load()
    assert(df.count() == 1)
  }
}
