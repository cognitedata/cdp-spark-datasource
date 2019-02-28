package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionNodeItem(
    id: Long,
    treeIndex: Long,
    parentId: Option[Long],
    depth: Long,
    name: String,
    subtreeSize: Long,
    metadata: Map[String, String],
    boundingBox: Option[Map[String, Seq[Double]]],
    sectorId: Option[Long])

class ThreeDModelRevisionNodesRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val metricsSource = new MetricsSource(config.metricsPrefix)
  @transient lazy private val modelRevisionNodesRead =
    metricsSource.getOrCreateCounter(s"3dmodelrevisionnodes.read")

  override def schema: StructType = structType[ModelRevisionNodeItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = base3dModelRevisionMappingsUrl(config.project)
    CdpRdd[ModelRevisionNodeItem](
      sqlContext.sparkContext,
      (e: ModelRevisionNodeItem) => {
        if (config.collectMetrics) {
          modelRevisionNodesRead.inc()
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

  def base3dModelRevisionMappingsUrl(project: String, version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/$project/3d/models/$modelId/revisions/$revisionId/nodes"
}
