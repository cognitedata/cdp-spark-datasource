package cognite.spark.v1

import cognite.spark.compiletime.macros.SparkSchemaHelper._
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
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override def schema: StructType = structType[ThreeDRevision]()

  override def toRow(t: ThreeDRevision): Row = asRow(t)

  override def uniqueId(a: ThreeDRevision): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[TracedIO]): Seq[Stream[TracedIO, ThreeDRevision]] =
    Seq(client.threeDRevisions(modelId).list(config.limitPerPartition))
}
