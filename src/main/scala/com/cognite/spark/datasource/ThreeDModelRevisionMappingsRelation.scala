package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionMappingItem(nodeId: Long,
                                    assetId: Long,
                                    treeIndex: Option[Long],
                                    subtreeSize: Option[Long])

class ThreeDModelRevisionMappingsRelation(config: RelationConfig,
                                          modelId: Long,
                                          revisionId: Long)
                                         (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val modelRevisionMappingsRead =
    UserMetricsSystem.counter(s"${config.metricsPrefix}3dmodelrevisionmappings.read")

  override def schema: StructType = structType[ModelRevisionMappingItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = base3dModelRevisionMappingsUrl(config.project)
    CdpRdd[ModelRevisionMappingItem](sqlContext.sparkContext,
      (e: ModelRevisionMappingItem) => {
        if (config.collectMetrics) {
          modelRevisionMappingsRead.inc()
        }
        asRow(e)
      },
      baseUrl, baseUrl, config.apiKey, config.project, batchSize, maxRetries, config.limit)
  }

  def base3dModelRevisionMappingsUrl(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/3d/models/$modelId/revisions/$revisionId/mappings"
  }
}
