package cognite.spark.v1

import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDAssetMapping}
import fs2.Stream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ThreeDModelRevisionMappingsRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDAssetMapping, String](config, "3dmodelrevisionmappings") {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override def schema: StructType = structType[ThreeDAssetMapping]()

  override def toRow(t: ThreeDAssetMapping): Row = asRow(t)

  override def uniqueId(a: ThreeDAssetMapping): String = a.nodeId.toString + a.assetId.toString

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[TracedIO]): Seq[Stream[TracedIO, ThreeDAssetMapping]] =
    Seq(client.threeDAssetMappings(modelId, revisionId).list(config.limitPerPartition))
}
