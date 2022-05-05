package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDRevision}
import fs2.Stream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

final case class ModelRevisionItem(
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
    extends SdkV1Relation[ThreeDRevision, Long](config, "3dmodelrevision") {
  override def schema: StructType = structType[ThreeDRevision]

  override def toRow(t: ThreeDRevision): Row = asRow(t)

  override def uniqueId(a: ThreeDRevision): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ThreeDRevision]] =
    Seq(client.threeDRevisions(modelId).list(limit))
}
