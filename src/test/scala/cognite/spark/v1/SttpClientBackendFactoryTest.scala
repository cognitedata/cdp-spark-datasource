package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers}
import sttp.client3.asynchttpclient.SttpClientBackendFactory

class SttpClientBackendFactoryTest extends FlatSpec with Matchers {


  class DummyTestThread extends Runnable {
    def run(): Unit = ()
  }

  "SttpClientBackendFactory" should "create AsyncHttpClient with default prefix Cdf-Spark for threads" in {
    val asyncHttpClient = SttpClientBackendFactory.create()
    val thread = asyncHttpClient.getConfig.getThreadFactory.newThread { new DummyTestThread }
    thread.getName should startWith("Cdf-Spark-AsyncHttpClient")
    asyncHttpClient.close()
    asyncHttpClient.isClosed shouldBe true
  }
}
