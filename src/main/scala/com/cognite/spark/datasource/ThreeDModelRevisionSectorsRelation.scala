package com.cognite.spark.datasource

import com.softwaremill.sttp._
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructType}
import io.circe.generic.auto._

case class ThreeDModelRevisionSectorsItem(id: Int,
                                          parentId: Option[Int],
                                          path: String,
                                          depth: Int,
                                          boundingBox: Map[String, Seq[Double]],
                                          threedFileId: Long,
                                          threedFiles: Seq[Map[String, Long]])

class ThreeDModelRevisionSectorsRelation(apiKey: String,
                                         project: String,
                                         modelId: Long,
                                         revisionId: Long,
                                         limit: Option[Int],
                                         batchSizeOption: Option[Int],
                                         maxRetriesOption: Option[Int],
                                         metricsPrefix: String,
                                         collectMetrics: Boolean)
                                        (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with CdpConnector
    with TableScan
    with Serializable {
  @transient lazy private val batchSize = batchSizeOption.getOrElse(Constants.DefaultBatchSize)
  @transient lazy private val maxRetries = maxRetriesOption.getOrElse(Constants.DefaultMaxRetries)

  @transient lazy private val threeDModelRevisionSectorsRead = UserMetricsSystem.counter(s"${metricsPrefix}assets.read")

  import SparkSchemaHelper._

  override val schema: StructType = structType[ThreeDModelRevisionSectorsItem]

  override def buildScan(): RDD[Row] = {
    val baseUrl = baseThreeDModelReviewSectorsUrl(project)
    CdpRdd[ThreeDModelRevisionSectorsItem](sqlContext.sparkContext,
      (tds: ThreeDModelRevisionSectorsItem) => {
        if (collectMetrics) {
          threeDModelRevisionSectorsRead.inc()
        }
        asRow(tds)
      },
      baseUrl, baseUrl, apiKey, project, batchSize, maxRetries, limit)
  }

  def baseThreeDModelReviewSectorsUrl(project: String, version: String = "0.6"): Uri = {
    uri"https://api.cognitedata.com/api/$version/projects/$project/3d/models/$modelId/revisions/$revisionId/sectors"
  }
}
