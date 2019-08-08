package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import io.circe.generic.auto._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDRevision}
import com.cognite.sdk.scala.v1.resources.ThreeDRevisions

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
    extends SdkV1Relation[ThreeDRevision, ThreeDRevisions[IO], ModelRevisionItem](
      config,
      "3dmodelrevision") {
  override def schema: StructType = structType[ThreeDRevision]

  override def toRow(t: ThreeDRevision): Row = asRow(t)

  override def clientToResource(client: GenericClient[IO, Nothing]): ThreeDRevisions[IO] =
    client.threeDRevisions(modelId)

  override def listUrl(version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/${config.project}/3d/models/$modelId/revisions"
}
