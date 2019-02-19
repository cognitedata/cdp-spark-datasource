package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelItem(id: Long,
                     name: String,
                     createdTime: Long)

class ThreeDModelsRelation(config: RelationConfig)
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val modelsRead = UserMetricsSystem.counter(s"${config.metricsPrefix}3dmodels.read")

  override def schema: StructType = structType[ModelItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = base3dModelsUrl(config.project)
    CdpRdd[ModelItem](sqlContext.sparkContext,
      (e: ModelItem) => {
        if (config.collectMetrics) {
          modelsRead.inc()
        }
        asRow(e)
      },
      baseUrl, baseUrl, config.apiKey, config.project, batchSize, maxRetries, config.limit)
  }

  def base3dModelsUrl(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/3d/models"
  }
}
