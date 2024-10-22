package cognite.spark.v1

import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.{FileDownloadExternalId, GenericClient}
import fs2.Stream
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{Partition, TaskContext}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.net.URL
import scala.io.Source

trait WithSizeLimit {
  val sizeLimit: Long
}

class FileContentRelation(config: RelationConfig, fileExternalId: String)(
    override val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedFilteredScan
    with Serializable
    with WithSizeLimit {

  override val sizeLimit: Long = 5 * FileUtils.ONE_GB

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  override def schema: StructType =
    createDataFrame(sqlContext.sparkSession).schema

  def createDataFrame(sparkSession: SparkSession): DataFrame = {
    val rowsRdd: RDD[String] = new RDD[String](sparkSession.sparkContext, Nil) with Serializable {

      import cognite.spark.v1.CdpConnector.ioRuntime

      val maxParallelism = 1
      override def compute(split: Partition, context: TaskContext): Iterator[String] = {

        val validUrl = for {
          mimeType <- client.files.retrieveByExternalId(fileExternalId).map(_.mimeType)
          downloadLink <- client.files
            .downloadLink(FileDownloadExternalId(fileExternalId))
            .map(_.downloadUrl)
          isFileWithinLimits <- isFileWithinLimits(downloadLink)
        } yield {
          if (mimeType.isDefined && Seq("application/jsonlines", "application/x-ndjson", "application/jsonl").contains(mimeType.get))
            throw new CdfSparkException("Wrong mimetype. Expects application/jsonlines")
          if (!isFileWithinLimits)
            throw new CdfSparkException(f"File size above size limit")
          downloadLink
        }

        StreamIterator(
          Stream
            .eval(validUrl)
            .flatMap(readUrlContent),
          maxParallelism,
          None
        )

      }

      override protected def getPartitions: Array[Partition] =
        Array(CdfPartition(0))
    }
    import sparkSession.implicits._
    sparkSession.read.json(rowsRdd.toDS())
  }

  private def readUrlContent(url: String): Stream[IO, String] =
    Stream
      .bracket(IO(new URL(url).openStream())) { inStream =>
        IO(inStream.close()) // Ensure the InputStream is closed properly
      }
      .flatMap { inStream =>
        val source = Source.fromInputStream(inStream)
        Stream.fromIterator[IO](source.getLines(), chunkSize = 4096)
      }

  private def isFileWithinLimits(downloadUrl: String): IO[Boolean] = {
    val backend = Resource.make(IO(HttpURLConnectionBackend()))(b => IO(b.close()))
    val request = basicRequest.head(uri"$downloadUrl")

    backend.use { backend =>
      IO {
        val response = request.send(backend)
        response.header("Content-Length").exists { sizeStr =>
          sizeStr.toLong < sizeLimit
        }
      }
    }
  }

  override def buildScan(): RDD[Row] =
    createDataFrame(sqlContext.sparkSession).rdd

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    createDataFrame(sqlContext.sparkSession).select(requiredColumns.map(col).toIndexedSeq: _*).rdd

}
