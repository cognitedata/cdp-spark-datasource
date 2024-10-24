package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1.{FileDownloadExternalId, GenericClient}
import fs2.Stream
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{Partition, TaskContext}
import sttp.client3.{UriContext, asByteArray}
import sttp.model.Header



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

  private lazy val dataFrame: DataFrame = createDataFrame(sparkSession = sqlContext.sparkSession)


  override def schema: StructType = {
    dataFrame.schema
  }


  def createDataFrame(sparkSession: SparkSession): DataFrame = {
    val rdd: RDD[String] = new RDD[String](sparkSession.sparkContext, Nil) with Serializable {

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
          if (mimeType.isDefined && !Seq("application/jsonlines", "application/x-ndjson", "application/jsonl").contains(mimeType.get))
            throw new CdfSparkException("Wrong mimetype. Expects application/jsonlines")
          if (!isFileWithinLimits)
            throw new CdfSparkException(f"File size above size limit, or file size header absent from head request")
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
    val dataset = rdd.toDS()

    inferSchema match {
      case true =>  sqlContext.sparkSession.read.json(dataset)
      case false => dataset.toDF()
    }
  }

  private def readUrlContent(link: String): Stream[IO, String] = {
    val request = client.requestSession.send { request =>
      request
        .get(uri"$link")
        .response(asByteArray)
    }
    Stream.eval(request).flatMap { response =>
      response.body match {
        case Right(byteArray) => Stream.emits(byteArray)
          .through(fs2.text.utf8.decode)
          .through(fs2.text.lines)
        case Left(error) => throw new CdfSparkException(f"Error while requesting underlying file: $error")
        case _ => throw new CdfSparkException("Error while requesting underlying file")
      }
    }
  }

  private def isFileWithinLimits(downloadUrl: String): IO[Boolean] = {
    val headers: IO[Seq[Header]] = client.requestSession.head(uri"$downloadUrl")()

    headers.map { headerSeq =>
      val sizeHeader = headerSeq.find(_.name.equalsIgnoreCase("content-length"))
      sizeHeader.exists(_.value.toLong < sizeLimit)
    }
  }

  override def buildScan(): RDD[Row] = {
    dataFrame.rdd
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    dataFrame.select(requiredColumns.map(col).toIndexedSeq: _*).rdd

}
