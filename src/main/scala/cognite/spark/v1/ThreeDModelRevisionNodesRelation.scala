package cognite.spark.v1

import cats.effect.IO
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDNode}
import fs2.Stream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ThreeDModelRevisionNodesRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDNode, Long](config, "3dmodelrevisionnodes") {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override def schema: StructType = structType[ThreeDNode]()

  override def toRow(t: ThreeDNode): Row = asRow(t)

  override def uniqueId(a: ThreeDNode): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, ThreeDNode]] =
    Seq(client.threeDNodes(modelId, revisionId).list(config.limitPerPartition))
}

object ThreeDModelRevisionNodesRelation extends NamedRelation {
  override val name: String = "3dmodelrevisionnodes"
}
