package cognite.spark.v1

import cats.effect.std.Dispatcher
import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.{FileDownloadExternalId, GenericClient}
import fs2.Stream
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}
import sttp.capabilities.WebSockets
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.asynchttpclient.SttpClientBackendFactory
import sttp.client3.asynchttpclient.fs2.AsyncHttpClientFs2Backend
import sttp.client3.{SttpBackend, UriContext, asStreamUnsafe, basicRequest}
import sttp.model.{Header, Uri}
import scala.collection.immutable.Seq

//The trait exist for testing purposes
trait WithSizeLimit {
  val sizeLimit: Long
}

class FileContentRelation(config: RelationConfig, fileExternalId: String, inferSchema: Boolean)(
    override val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedFilteredScan
    with Serializable
    with WithSizeLimit {

  override val sizeLimit: Long = 5 * FileUtils.ONE_GB

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  @transient private lazy val sttpFileContentStreamingBackendResource
    : Resource[IO, SttpBackend[IO, Fs2Streams[IO] with WebSockets]] =
    for {
      dispatcher <- Dispatcher.parallel[IO]
      backend <- Resource.make(
        IO(
          AsyncHttpClientFs2Backend
            .usingClient[IO](SttpClientBackendFactory.create("file content download"), dispatcher))
      )(backend => backend.close())
    } yield backend

  val acceptedMimeTypes: Seq[String] =
    Seq("application/jsonlines", "application/x-ndjson", "application/jsonl")

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
          mimeType <- IO.pure(file.mimeType)
          _ <- IO.raiseWhen(!file.uploaded)(
            new CdfSparkException(
              f"Could not read file because no file was uploaded for externalId: $fileExternalId")
          )
          _ <- IO.raiseWhen(mimeType.exists(!acceptedMimeTypes.contains(_)))(
            new CdfSparkException("Wrong mimetype. Expects application/jsonlines")
          )

          downloadLink <- client.files
            .downloadLink(FileDownloadExternalId(fileExternalId))
          uri <- IO.pure(uri"${downloadLink.downloadUrl}")

          _ <- IO.raiseWhen(!uri.scheme.contains("https"))(
            new CdfSparkException("Invalid download uri, it should be a valid url using https")
          )

          _ <- isFileWithinLimits(uri).flatMap(IO.fromEither)
        } yield uri

        StreamIterator(
          Stream
            .eval(validUrl)
            .flatMap(readUrlContentLines),
          maxParallelism * 2,
          None
        )

      }

      override protected def getPartitions: Array[Partition] =
        Array(CdfPartition(0))
    }
    import sqlContext.sparkSession.implicits._
    val dataset = rdd.toDS()

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

      Stream.eval(backend.send(request)).flatMap { response =>
        response.body match {
          case Right(byteStream) =>
            byteStream
              .through(fs2.text.utf8.decode)
              .through(fs2.text.lines)
          case Left(error) =>
            Stream.raiseError[IO](new Exception(s"Error while requesting underlying file: $error"))
        }
      }
    }

  private def isFileWithinLimits(downloadUrl: Uri): IO[Either[CdfSparkException, Long]] =
    for {
      headers <- client.requestSession.head(downloadUrl, Seq(Header("Accept-Encoding", "")))
    } yield {
      val sizeHeader = headers.find(_.name.equalsIgnoreCase("content-length"))
      sizeHeader match {
        case None => Left(FileSizeUnknown())
        case Some(header: Header) =>
          if (header.value.toLong > sizeLimit)
            Left(FileSizeTooBig(header.value.toLong, sizeLimit))
          else
            Right(header.value.toLong)
      }
    }

  override def buildScan(): RDD[Row] =
    dataFrame.rdd

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    dataFrame.select(requiredColumns.map(col).toIndexedSeq: _*).rdd

}

final case class FileSizeTooBig(size: Long, sizeLimit: Long)
    extends CdfSparkException(s"File size too big. Size: $size SizeLimit: $sizeLimit")
final case class FileSizeUnknown()
    extends CdfSparkException("Unknown file size: No content-length header on the download endpoint")
