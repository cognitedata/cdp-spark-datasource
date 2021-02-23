package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{Event, GenericClient}
import fs2.{Chunk, Stream}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}
import com.softwaremill.sttp._
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow, structType}
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.concurrent.duration._

class SdkV1RddTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  it should "throw an error when passed streams that return an error" in {

    val errorMessage = "Some exception"

    def getStreams(
        client: GenericClient[IO],
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

    def toRow(s: String, partitionIndex: Option[Int]): Row = Row.empty
    def uniqueId(s: String): String = "1"

    val sdkRdd =
      SdkV1Rdd(spark.sparkContext, getDefaultConfig(CdfSparkAuth.Static(readApiKeyAuth)), toRow, uniqueId, getStreams)

    val e = intercept[CdpApiException] {
      sdkRdd.compute(CdfPartition(0), TaskContext.get())
    }
    assert(e.message == errorMessage)
    assert(e.code == 400)
  }

  it should "convert multiple streams to one Iterator" in {
    import CdpConnector._

    val nStreams = 50
    val nItemsPerStream = 1000
    val rdd = new SdkV1Rdd[Event, Long](
      spark.sparkContext,
      DefaultSource
        .parseRelationConfig(Map("apiKey" -> writeApiKey), spark.sqlContext)
        .copy(parallelismPerPartition = nStreams),
      (e: Event, partitionIndex: Option[Int]) => asRow(e),
      (e: Event) => e.id,
      (_: GenericClient[IO], _: Option[Int], _: Int) => {
        val allStreams = 0.until(nStreams).map { i =>
          Stream.evalUnChunk {
            IO.sleep((scala.math.random * 300).millis)(cdpConnectorTimer) *> IO(
              Chunk.seq(1.to(nItemsPerStream).map(j => Event(id = i * nItemsPerStream + j))))
          }
        }
        // Duplicates should be filtered out, so appending streams shouldn't make any difference.
        allStreams ++ allStreams ++ allStreams
      }
    )

    assert(rdd.compute(CdfPartition(0), TaskContext.get()).size == nStreams * nItemsPerStream)
  }
}
