package cognite.spark.v1

import cognite.spark.v1.AlphaDataModelInstancesHelper.getDirectRelationIdentifierProperty
import com.cognite.sdk.scala.v1.DirectRelationIdentifier
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class AlphaDataModelInstancesHelperTest extends FlatSpec with Matchers {
  it should "use first colon as the separator for spaceExternalId and externalId when get Direct Relation Identifier" in {
    val externalId = "instance-1"
    val row = Row.fromSeq(
      Seq(
        "test-space1:externalId1",
        "test-space2:externalId2:withColon2",
        "test-space3:externalId3:withColon3a:withColon3b::withColon3c:::withColon3d"))
    val dri1 = getDirectRelationIdentifierProperty(externalId, row, "", 0)
    dri1 shouldBe DirectRelationIdentifier(Some("test-space1"), "externalId1")

    val dri2 = getDirectRelationIdentifierProperty(externalId, row, "", 1)
    dri2 shouldBe DirectRelationIdentifier(Some("test-space2"), "externalId2:withColon2")

    val dri3 = getDirectRelationIdentifierProperty(externalId, row, "", 2)
    dri3 shouldBe DirectRelationIdentifier(
      Some("test-space3"),
      "externalId3:withColon3a:withColon3b::withColon3c:::withColon3d")
  }

  it should "throw CdfSparkException if there is no colon in the row of Direct Relation Identifier" in {
    val row = Row.fromSeq(Seq("test-space1externalId1"))
    val exception = the[CdfSparkException] thrownBy getDirectRelationIdentifierProperty(
      "instance-1",
      row,
      "startNode",
      0)
    exception.getMessage shouldBe "Invalid data model instance row with external id 'instance-1'. The values " +
      "in the startNode column should have the format `spaceExternalId:nodeExternalId`, but was: test-space1externalId1"
  }
}
