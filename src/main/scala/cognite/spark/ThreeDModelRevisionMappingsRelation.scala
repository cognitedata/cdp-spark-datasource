package cognite.spark

import cats.effect.IO
import cognite.spark.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDAssetMapping}
import org.apache.spark.sql.sources.Filter

case class ModelRevisionMappingItem(
    nodeId: Long,
    assetId: Long,
    treeIndex: Option[Long],
    subtreeSize: Option[Long])

class ThreeDModelRevisionMappingsRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDAssetMapping](config, "3dmodelrevisionmappings") {
  override def schema: StructType = structType[ThreeDAssetMapping]

  override def toRow(t: ThreeDAssetMapping): Row = asRow(t)

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: StatusCode): Seq[fs2.Stream[IO, ThreeDAssetMapping]] =
    Seq(client.threeDAssetMappings(modelId, revisionId).list(limit))
}
