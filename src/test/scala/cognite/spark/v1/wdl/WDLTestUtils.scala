package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfSparkAuth, WDLSparkTest}
import com.cognite.sdk.scala.common.ApiKeyAuth
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors, Matchers}

class WDLTestUtilsTest
    extends FlatSpec
    with Matchers
    with WDLSparkTest
    with Inspectors
    with BeforeAndAfter {
  private val config = getDefaultConfig(CdfSparkAuth.Static(ApiKeyAuth(writeApiKey)))
  private val client = new TestWdlClient(config)

  before {
    client.deleteAll()
  }

  it should "setup wells and wellbores with miniSetup" in {
    client.miniSetup()
  }

  it should "ingest and delete sources" in {
    // Create new source
    client.ingestSources(Seq(Source(name = "my-test")))
    val sources2 = client.getSources
    assert(sources2.map(_.name) == Seq("my-test"))

    client.setMergeRules(Seq("my-test"))

    // Delete the source
    client.deleteSources(Seq("my-test"))
    val sources3 = client.getSources
    assert(sources3 == Seq())
  }
}
