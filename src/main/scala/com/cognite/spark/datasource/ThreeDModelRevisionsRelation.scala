package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.{Uri, _}
import io.circe.generic.auto._
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class ModelRevisionItem(
    id: Long,
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

class ThreeDModelRevisionsRelation(config: RelationConfig, modelId: Long)(
    val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with CdpConnector
    with Serializable {
  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = config.maxRetries.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val metricsSource = new MetricsSource(config.metricsPrefix)
  @transient lazy private val modelRevisionsRead =
    metricsSource.getOrCreateCounter(s"3dmodelrevisions.read")

  override def schema: StructType = structType[ModelRevisionItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = base3dModelRevisionsUrl(config.project)
    CdpRdd[ModelRevisionItem](
      sqlContext.sparkContext,
      (e: ModelRevisionItem) => {
        if (config.collectMetrics) {
          modelRevisionsRead.inc()
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

  def base3dModelRevisionsUrl(project: String, version: String = "0.6"): Uri =
    uri"https://api.cognitedata.com/api/$version/projects/$project/3d/models/$modelId/revisions"
}
