package cognite.spark.v1

import cats.effect.std.Dispatcher
import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.FileDownloadExternalId
import fs2.{Pipe, Stream}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}
import sttp.capabilities.WebSockets
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.asynchttpclient.SttpClientBackendFactory
import sttp.client3.asynchttpclient.fs2.AsyncHttpClientFs2Backend
import sttp.client3.{SttpBackend, UriContext, asStreamUnsafe, basicRequest}
import sttp.model.Uri

import scala.collection.immutable._
import scala.concurrent.duration.Duration

//The trait exist for testing purposes
trait WithSizeLimit {
  val fileSizeLimitBytes: Long
  val lineSizeLimitCharacters: Int
}

class FileContentRelation(config: RelationConfig, fileExternalId: String, inferSchema: Boolean)(
    override val sqlContext: SQLContext)
    extends CdfRelation(config, FileContentRelation.name)
    with TableScan
    with PrunedFilteredScan
    with Serializable
    with WithSizeLimit {

  override val fileSizeLimitBytes: Long = 5 * FileUtils.ONE_GB
  //We enforce an arbitrarily chosen 2.5MB JVM mem limit per line. In jvm each character takes 2bytes so we divide by two to get the limit in characters.
  private val lineSizeLimitBytes: Int = (2.5 * FileUtils.ONE_MB).toInt
  override val lineSizeLimitCharacters: Int = lineSizeLimitBytes / 2

  @transient private lazy val sttpFileContentStreamingBackendResource
    : Resource[IO, SttpBackend[IO, Fs2Streams[IO] with WebSockets]] =
    for {
      dispatcher <- Dispatcher.parallel[IO]
      backend <- Resource.make(
        IO(
          AsyncHttpClientFs2Backend
            .usingClient[IO](
              SttpClientBackendFactory.create("file content download", Some(3600000)),
              dispatcher))
      )(backend => backend.close())
    } yield backend

  @transient private lazy val dataFrame: DataFrame = createDataFrame

  override def schema: StructType =
    dataFrame.schema

  @transient lazy val createDataFrame: DataFrame = {
    val rdd: RDD[String] = new RDD[String](sqlContext.sparkContext, Nil) with Serializable {

      import cognite.spark.v1.CdpConnector.ioRuntime

      val maxParallelism = 1
      override def compute(split: Partition, context: TaskContext): Iterator[String] = {

        val validUrl = for {
          file <- client.files.retrieveByExternalId(fileExternalId)
          _ <- IO.raiseWhen(!file.uploaded)(
            new CdfSparkException(
              f"Could not read file because no file was uploaded for externalId: $fileExternalId")
          )
          downloadLink <- client.files
            .downloadLink(FileDownloadExternalId(fileExternalId))
          uri <- IO.pure(uri"${downloadLink.downloadUrl}")

          _ <- IO.raiseWhen(!uri.scheme.contains("https"))(
            new CdfSparkException("Invalid download uri, it should be a valid url using https")
          )

        } yield uri

        StreamIterator(
          Stream
            .eval(validUrl)
            .flatMap(readUrlContentLines)
            .evalTap(_ => IO.delay { itemsRead.inc() }),
          maxParallelism * 2,
          None
        )

      }

      override protected def getPartitions: Array[Partition] =
        Array(CdfPartition(0))
    }
    import sqlContext.sparkSession.implicits._
    val dataset = rdd.cache().toDS()

    if (inferSchema) {
      sqlContext.sparkSession.read.json(dataset)
    } else {
      dataset.toDF()
    }
  }

  private def readUrlContentLines(link: Uri): Stream[IO, String] =
    Stream.resource(sttpFileContentStreamingBackendResource).flatMap { backend =>
      val request = basicRequest
        .get(link)
        .response(asStreamUnsafe(Fs2Streams[IO]))
        .readTimeout(Duration.Inf)

      Stream.eval(backend.send(request)).flatMap { response =>
        response.body match {
          case Right(byteStream) =>
            byteStream
              .through(enforceSizeLimit)
              .through(fs2.text.utf8.decode)
              .through(fs2.text.linesLimited(lineSizeLimitCharacters))
              .handleErrorWith {
                case e: fs2.text.LineTooLongException =>
                  throw new CdfSparkException(
                    s"""Line too long in file with external id: "$fileExternalId" SizeLimit in characters: ${e.max}, but ${e.length} characters accumulated""",
                    e)
                case other =>
                  throw other
              }
          case Left(error) =>
            Stream.raiseError[IO](new Exception(s"Error while requesting underlying file: $error"))
        }
      }
    }

  override def buildScan(): RDD[Row] =
    dataFrame.rdd

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    dataFrame.select(requiredColumns.map(col).toIndexedSeq: _*).rdd

  private val enforceSizeLimit: Pipe[IO, Byte, Byte] =
    in =>
      in.scanChunks(0L) { (acc, chunk) =>
        val newSize = acc + chunk.size
        if (newSize > fileSizeLimitBytes)
          throw new CdfSparkException(
            s"""File with external id: "$fileExternalId" size too big. SizeLimit in bytes: $fileSizeLimitBytes""")
        else
          (newSize, chunk)
    }

}

object FileContentRelation extends NamedRelation {
  override val name: String = "filecontent"
}
