package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class FileItem(id: Option[Long],
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

class FilesRelation(apiKey: String,
                    project: String,
                    limit: Option[Int],
                    batchSizeOption: Option[Int],
                    maxRetriesOption: Option[Int],
                    metricsPrefix: String,
                    collectMetrics: Boolean)
                   (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = batchSizeOption.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = maxRetriesOption.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val filesRead = UserMetricsSystem.counter(s"${metricsPrefix}files.read")

  override def schema: StructType = structType[FileItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = baseFilesUrl(project)
    CdpRdd[FileItem](sqlContext.sparkContext,
      (e: FileItem) => {
        if (collectMetrics) {
          filesRead.inc()
        }
        asRow(e)
      },
      baseUrl, baseUrl, apiKey, project, batchSize, maxRetries, limit)
  }

  def baseFilesUrl(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/files"
  }
}
