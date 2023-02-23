package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers}

class FlexibleDataModelConnectionRelationTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with FlexibleDataModelsTestBase {

  //  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  it should "pass" in {
    1 shouldBe 1
  }
}
