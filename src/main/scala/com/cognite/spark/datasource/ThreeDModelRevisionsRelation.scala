package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionItem(id: Long,
                             fileId: Long,
                             published: Boolean,
                             rotation: Option[Seq[Double]],
                             camera: Map[String, Seq[Double]],
                             status: String,
                             thumbnailThreedFileId: Option[Long],
                             thumbnailUrl: Option[String],
                             sceneThreedFiles: Option[Seq[Map[String, Long]]],
                             sceneThreedFileId: Option[Long],
                             assetMappingCount: Long,
                             createdTime: Long)

class ThreeDModelRevisionsRelation(apiKey: String,
                                   project: String,
                                   modelId: Long,
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

  @transient lazy private val modelRevisionsRead = UserMetricsSystem.counter(s"${metricsPrefix}3dmodelrevisions.read")

  override def schema: StructType = structType[ModelRevisionItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = base3dModelRevisionsUrl(project)
    CdpRdd[ModelRevisionItem](sqlContext.sparkContext,
      (e: ModelRevisionItem) => {
        if (collectMetrics) {
          modelRevisionsRead.inc()
        }
        asRow(e)
      },
      baseUrl, baseUrl, apiKey, project, batchSize, maxRetries, limit)
  }

  def base3dModelRevisionsUrl(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/3d/models/$modelId/revisions"
  }
}
