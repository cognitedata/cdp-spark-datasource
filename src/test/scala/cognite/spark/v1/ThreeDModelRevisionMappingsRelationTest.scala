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
      .format("cognite.spark.v1")
      .option("tokenUri", OIDCWrite.tokenUri)
      .option("clientId", OIDCWrite.clientId)
      .option("clientSecret", OIDCWrite.clientSecret)
      .option("project", OIDCWrite.project)
      .option("scopes", OIDCWrite.scopes)
      .option("type", "3dmodelrevisionmappings")
      .option("modelid", model.id)
      .option("revisionid", revision.id)
      .load()
    assert(df.count() == 1)
  }
}
