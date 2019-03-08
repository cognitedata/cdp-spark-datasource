package com.cognite.spark.datasource
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import io.circe.generic.auto._
import io.circe.generic.decoding.DerivedDecoder
import com.softwaremill.sttp.Uri
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.reflect._
import scala.reflect.runtime.universe._

abstract class CdpRelation[T: DerivedDecoder: TypeTag: ClassTag](
    config: RelationConfig,
    shortName: String)
    extends BaseRelation
    with TableScan
    with Serializable {
  @transient lazy val metricsSource = new MetricsSource(config.metricsPrefix)
  @transient lazy private val itemsRead = metricsSource.getOrCreateCounter(s"$shortName.read")

  val sqlContext: SQLContext

  override def buildScan(): RDD[Row] =
    CdpRdd[T](
      sqlContext.sparkContext,
      (e: T) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(e)
      },
      cursorsUrl(config),
      listUrl(config),
      config
    )

  def toRow(t: T): Row

  def listUrl(relationConfig: RelationConfig): Uri

  def cursorsUrl(relationConfig: RelationConfig): Uri = listUrl(relationConfig)
}
