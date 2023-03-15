package cognite.spark.v1.wdl

import cognite.spark.v1.SparkTest
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors, Matchers}

class WDLTestUtilsTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with Inspectors
    with BeforeAndAfter {

  val testClient: TestWdlClient = new TestWdlClient(writeClient)

  before {
    testClient.deleteAll()
  }

  it should "setup wells and wellbores with miniSetup" in {
    import cognite.spark.v1.CdpConnector._
    val miniSetup: testClient.MiniSetup = testClient.miniSetup()
    val wellSources = writeClient.wdl.wellSources.list().unsafeRunSync()
    wellSources.map(_.source) shouldBe miniSetup.well.sources

    val wellboreSources = writeClient.wdl.wellboreSources.list().unsafeRunSync()
    wellboreSources.map(_.source) shouldBe miniSetup.wellbores.flatMap(_.sources)
  }
}
