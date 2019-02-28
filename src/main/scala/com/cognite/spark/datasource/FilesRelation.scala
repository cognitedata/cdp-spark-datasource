package com.cognite.spark.datasource

import com.codahale.metrics.Counter
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.datasource.MetricsSource

case class FileItem(
    id: Option[Long],
    fileName: String,
    directory: Option[String],
    source: Option[String],
    sourceId: Option[String],
    fileType: Option[String],
    metadata: Option[Map[String, Option[String]]],
    assetIds: Option[Seq[Long]],
    uploaded: Option[Boolean],
    uploadedAt: Option[Long],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val metricsSource = new MetricsSource(config.metricsPrefix)
  @transient lazy private val filesRead = metricsSource.getOrCreateCounter("files.read")

  override def schema: StructType = structType[FileItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = baseFilesUrl(config.project)
    CdpRdd[FileItem](
      sqlContext.sparkContext,
      (e: FileItem) => {
        if (config.collectMetrics) {
          filesRead.inc()
        }
        asRow(e)
      },
      baseUrl,
      baseUrl,
      config.apiKey,
      config.project,
      batchSize,
      maxRetries,
      config.limit
    )
  }

  def baseFilesUrl(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/files"
}
