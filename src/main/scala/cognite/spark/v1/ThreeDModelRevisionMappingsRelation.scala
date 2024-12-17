package cognite.spark.v1

import cats.effect.IO
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDAssetMapping}
import fs2.Stream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ThreeDModelRevisionMappingsRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDAssetMapping, String](config, ThreeDModelRevisionMappingsRelation.name) {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override def schema: StructType = structType[ThreeDAssetMapping]()

  override def toRow(t: ThreeDAssetMapping): Row = asRow(t)

  override def uniqueId(a: ThreeDAssetMapping): String = a.nodeId.toString + a.assetId.toString

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, ThreeDAssetMapping]] =
    Seq(client.threeDAssetMappings(modelId, revisionId).list(config.limitPerPartition))
}

object ThreeDModelRevisionMappingsRelation extends NamedRelation {
  override val name: String = "3dmodelrevisionmappings"
}
