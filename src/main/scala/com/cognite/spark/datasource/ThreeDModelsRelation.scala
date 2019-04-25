package com.cognite.spark.datasource

import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import io.circe.generic.auto._

case class ModelItem(id: Long, name: String, createdTime: Long)

class ThreeDModelsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[ModelItem](config, "3dmodels") {
  override def schema: StructType = structType[ModelItem]

  override def toRow(t: ModelItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.6/projects/${config.project}/3d/models"
}
