package cognite.spark.v1

import cats.effect.IO
import cats.effect.kernel.Resource
import com.cognite.sdk.scala.v1.{FileDownloadExternalId, GenericClient}
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{Partition, TaskContext}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}
class FileContentRelation(config: RelationConfig, fileId: String)(override val sqlContext: SQLContext) extends BaseRelation
  with TableScan
  with PrunedFilteredScan
  with Serializable
{

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  override def schema: StructType = {
    createDataFrame(sqlContext.sparkSession).schema
  }
  def createDataFrame(sparkSession: SparkSession): DataFrame = {
    val rowsRdd: RDD[String] = new RDD[String](sparkSession.sparkContext, Nil)
      with Serializable {

      import cognite.spark.v1.CdpConnector.ioRuntime

      val maxParallelism = 1
      override def compute(split: Partition, context: TaskContext): Iterator[String] = {

        val validUrl = for {
          downloadLink <- client
            .files.downloadLink(FileDownloadExternalId(fileId))
            .map(_.downloadUrl)
          isFileWithinLimits <- isFileWithinLimits(downloadLink)
        } yield {
          if(isFileWithinLimits)
            downloadLink
          else
            throw new CdfSparkException("File size too big")
        }

        StreamIterator(
          Stream
            .eval(validUrl)
            .flatMap(url => Files[IO].readUtf8Lines(Path(url))),
          maxParallelism,
          None
        )

      }

      override protected def getPartitions: Array[Partition] =
        Array(CdfPartition(0))
    }
    import sparkSession.implicits._
    rowsRdd.toDS().map(x => {
      print(x)
      x
    }): Unit
    sparkSession.read.json(rowsRdd.toDS())
  }

  private def isFileWithinLimits(downloadUrl: String): IO[Boolean] = {
    val backend = Resource.make(IO(HttpURLConnectionBackend()))(b => IO(b.close()))
    val request = basicRequest.head(uri"$downloadUrl")

    backend.use { backend =>
      IO {
        val response = request.send(backend)
        response.header("Content-Length").exists { sizeStr =>
          sizeStr.toLong < FileUtils.ONE_GB * 5
        }
      }
    }
  }
  override def buildScan(): RDD[Row] =
    createDataFrame(sqlContext.sparkSession).rdd

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    //createDataFrame(sqlContext.sparkSession).select(requiredColumns.map(col): _*).rdd
    createDataFrame(sqlContext.sparkSession).rdd

}
