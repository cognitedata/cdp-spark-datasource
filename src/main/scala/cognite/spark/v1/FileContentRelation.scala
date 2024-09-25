package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1.{FileDownloadExternalId, FileDownloadLink}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.apache.spark.sql.functions.col

class FileContentRelation(config: RelationConfig, fileId: String,
                          inferSchemaLimit: Option[Int],
                          collectSchemaInferenceMetrics: Boolean)(override val sqlContext: SQLContext)     extends BaseRelation
  with TableScan
  with PrunedFilteredScan
  with Serializable
{

  override def schema: StructType = {
    createDataFrame(sqlContext.sparkSession).schema
  }

  def createDataFrame(sparkSession: SparkSession): DataFrame = {
    val rowsRdd = new RDD[String](sparkSession.sparkContext, Nil)
      with Serializable {

      import cognite.spark.v1.CdpConnector.ioRuntime

      val maxParallelism = 1
      override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        val url: IO[String] = CdpConnector.clientFromConfig(config)
          .files.downloadLink(FileDownloadExternalId(fileId)).map(_.downloadUrl)
        StreamIterator(
          Stream
            .eval(url)
            .flatMap(url => Files[IO].readUtf8Lines(Path(url))),
          maxParallelism,
          None
        )

      }

      override protected def getPartitions: Array[Partition] =
        Array(CdfPartition(0))
    }

    sparkSession.read.json(rowsRdd)
  }

  override def buildScan(): RDD[Row] =
    createDataFrame(sqlContext.sparkSession).rdd

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    createDataFrame(sqlContext.sparkSession).select(requiredColumns.map(col): _*).rdd
}
