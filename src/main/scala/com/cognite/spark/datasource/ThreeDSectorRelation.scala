package com.cognite.spark.datasource

import com.softwaremill.sttp._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

case class SectorItem(id: Int,
                      parentId: Int,
                      path: String,
                      depth: Int,
                      boundingBox: Map[String, (Int,Int,Int)],
                      threedFileId: Int,
                      threedFile: Map[String, Int])

class ThreeDSectorRelation(apiKey: String,
                           project: String,
                           modelId: Long,
                           revisionId: Long,
                           limit: Option[Int],
                           batchSizeOption: Option[Int],
                           maxRetriesOption: Option[Int],
                           metricsPrefix: String,
                           collectMetrics: Boolean)
                          (val sqlContext: SQLContext)
  extends BaseRelation
    with CdpConnector
    with TableScan
    with Serializable {
  @transient lazy private val batchSize = batchSizeOption.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = maxRetriesOption.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy val assetsRead = UserMetricsSystem.counter(s"${metricsPrefix}assets.read")

  import SparkSchemaHelper._

  override val schema: StructType = structType[SectorItem]

  override def buildScan(): RDD[Row] = {
    val url = baseThreeDSectorURL(project)
    val getUrl = uri"$url/models/$modelId/revisions/$revisionId/sectors"
    CdpRdd[ThreeDSectorRelation](sqlContext.sparkContext,
      (tds: ThreeDSectorRelation) => {
        if (collectMetrics) {
          assetsRead.inc()
        }
        asRow(tds)},
      getUrl, getUrl, apiKey, project, batchSize, maxRetries, limit)
  }

  def baseThreeDSectorURL(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/3d/"
  }
}
