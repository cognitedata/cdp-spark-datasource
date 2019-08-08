package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.resources.ThreeDModels
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDModel}
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import io.circe.generic.auto._
case class ModelItem(id: Long, name: String, createdTime: Long)

class ThreeDModelsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDModel, ThreeDModels[IO], ModelItem](config, "threeDModels.read") {

  override def schema: StructType = structType[ThreeDModel]

  override def toRow(t: ThreeDModel): Row = asRow(t)

  override def clientToResource(client: GenericClient[IO, Nothing]): ThreeDModels[IO] =
    client.threeDModels

  def listUrl(version: String = "v1"): Uri =
    uri"${config.baseUrl}/api/$version/projects/${config.project}/3d/models"
}
