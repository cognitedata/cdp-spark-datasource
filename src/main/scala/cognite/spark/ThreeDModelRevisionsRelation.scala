package cognite.spark

import cats.effect.IO
import cognite.spark.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDRevision}
import org.apache.spark.sql.sources.Filter

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

class ThreeDModelRevisionsRelation(config: RelationConfig, modelId: Long)(val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDRevision](config, "3dmodelrevision") {
  override def schema: StructType = structType[ThreeDRevision]

  override def toRow(t: ThreeDRevision): Row = asRow(t)

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: StatusCode): Seq[fs2.Stream[IO, ThreeDRevision]] =
    Seq(client.threeDRevisions(modelId).list(limit))
}
