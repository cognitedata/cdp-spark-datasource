package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1.{Event, GenericClient}
import fs2.{Chunk, Stream}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}
import cognite.spark.compiletime.macros.SparkSchemaHelper.asRow
import sttp.client3._

import scala.concurrent.duration._

class SdkV1RddTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  it should "throw an error when passed streams that return an error" in {

    val errorMessage = "Some exception"

    val getStreams = (_: GenericClient[IO]) =>
      Seq(Stream.eval(IO.raiseError(com.cognite.sdk.scala.common.CdpApiException(
        uri"https://api.cognitedata.com/v1/",
        400,
        errorMessage,
        None,
        None,
        None,
        None,
        None))))

    val toRow = (_: String, _: Option[Int]) => Row.empty
    val uniqueId = (_: String) => "1"

    val sdkRdd = {
      val relationConfig = getDefaultConfig(
        CdfSparkAuth.OAuth2ClientCredentials(readOidcCredentials),
        readProject
      )
      SdkV1Rdd[String, String](
        spark.sparkContext,
        relationConfig,
        toRow,
        uniqueId,
        getStreams)
    }

    val e = intercept[CdpApiException] {
      sdkRdd.compute(CdfPartition(0), TaskContext.get()).size
    }
    assert(e.message == errorMessage)
    assert(e.code == 400)
  }

  private def generateStreams(nStreams: Int, nItemsPerStream: Int) =
    0.until(nStreams).map { i =>
      Stream.evalUnChunk {
        IO.sleep((scala.math.random() * 300).millis) *> IO(Chunk.from(1.to(nItemsPerStream).map { j =>
          val id = (i * nItemsPerStream + j).toLong
          Event(id = id)
        }))
      }
    }

  it should "convert multiple streams to one Iterator" in {
    val nStreams = 50
    val nItemsPerStream = 1000
    val rdd = new SdkV1Rdd[Event, Long](
      spark.sparkContext,
      DefaultSource
        .parseRelationConfig(Map(
          "tokenUri" -> OIDCWrite.tokenUri,
          "clientId" -> OIDCWrite.clientId,
          "clientSecret" -> OIDCWrite.clientSecret,
          "project" -> OIDCWrite.project,
          "scopes" -> OIDCWrite.scopes
        ), spark.sqlContext)
        .copy(parallelismPerPartition = nStreams * 3),
      (e: Event, _: Option[Int]) => asRow(e),
      (e: Event) => e.id,
      (_: GenericClient[IO]) => {
        val allStreams = generateStreams(nStreams, nItemsPerStream)
        // Duplicates should be filtered out, so appending streams shouldn't make any difference.
        allStreams ++ allStreams ++ allStreams
      }
    )

    assert(rdd.partitions.length == 1)
    assert(rdd.compute(CdfPartition(0), TaskContext.get()).size == nStreams * nItemsPerStream)
  }

  it should "convert multiple streams and keep duplicate" in {
    val nStreams = 50
    val nItemsPerStream = 1000
    val rdd = new SdkV1Rdd[Event, Long](
      spark.sparkContext,
      DefaultSource
        .parseRelationConfig(Map(
          "tokenUri" -> OIDCWrite.tokenUri,
          "clientId" -> OIDCWrite.clientId,
          "clientSecret" -> OIDCWrite.clientSecret,
          "project" -> OIDCWrite.project,
          "scopes" -> OIDCWrite.scopes
        ), spark.sqlContext)
        .copy(parallelismPerPartition = nStreams * 3),
      (e: Event, _: Option[Int]) => asRow(e),
      (e: Event) => e.id,
      (_: GenericClient[IO]) => {
        val allStreams = generateStreams(nStreams, nItemsPerStream)
        allStreams ++ allStreams ++ allStreams
      },
      deduplicateRows = false
    )

    assert(rdd.partitions.length == 1)
    assert(rdd.compute(CdfPartition(0), TaskContext.get()).size == nStreams * nItemsPerStream * 3) // * 3 because we duplicate the allStreams 3 times
  }

  it should "parse additional flags from relation config" in {
    DefaultSource.parseRelationConfig(
      Map(
        "tokenUri" -> OIDCWrite.tokenUri,
        "clientId" -> OIDCWrite.clientId,
        "clientSecret" -> OIDCWrite.clientSecret,
        "project" -> OIDCWrite.project,
        "scopes" -> OIDCWrite.scopes,
        DefaultSource.toAdditionalFlagKey("parseThisToTrue") -> "true",
        DefaultSource.toAdditionalFlagKey("parseThisToFalse") -> "false",
      ),
      spark.sqlContext
    ).additionalFlags shouldBe(
      Map(
        "parseThisToTrue" -> true,
        "parseThisToFalse" -> false
      )
    )
  }
}
