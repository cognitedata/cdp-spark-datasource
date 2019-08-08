package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import io.circe.generic.auto._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDAssetMapping}
import com.cognite.sdk.scala.v1.resources.ThreeDAssetMappings

case class ModelRevisionMappingItem(
    nodeId: Long,
    assetId: Long,
    treeIndex: Option[Long],
    subtreeSize: Option[Long])

class ThreeDModelRevisionMappingsRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDAssetMapping, ThreeDAssetMappings[IO], ModelRevisionMappingItem](
      config,
      "3dmodelrevisionmappings") {
  override def schema: StructType = structType[ThreeDAssetMapping]

  override def toRow(t: ThreeDAssetMapping): Row = asRow(t)

  override def listUrl(version: String = "0.6"): Uri =
    uri"${config.baseUrl}/api/$version/projects/${config.project}/3d/models/$modelId/revisions/$revisionId/mappings"

  override def clientToResource(client: GenericClient[IO, Nothing]): ThreeDAssetMappings[IO] =
    client.threeDAssetMappings(modelId, revisionId)
}
