package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.GenericClient
import fs2.Stream
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import com.softwaremill.sttp._

class SdkV1RddTest extends FlatSpec with Matchers with SparkTest {
  it should "throw an error when passed streams that return an error" in {

    val errorMessage = "Some exception"

    def getStreams(
        client: GenericClient[IO, Nothing],
        limit: Option[Int],
        numPartitions: Int): Seq[Stream[IO, String]] =
      Seq(
        Stream.eval(
          throw com.cognite.sdk.scala.common.CdpApiException(
            uri"https://api.cognitedata.com/v1/",
            400,
            errorMessage,
            None,
            None,
            None,
            None)))

    def toRow(s: String): Row = Row.empty
    def uniqueId(s: String): String = "1"

    val sdkRdd = SdkV1Rdd(spark.sparkContext, getDefaultConfig(readApiKeyAuth), toRow, uniqueId, getStreams)

    val e = intercept[CdpApiException] {
      sdkRdd.compute(CdfPartition(0), TaskContext.get())
    }
    assert(e.message == errorMessage)
    assert(e.code == 400)
  }
}
