package cognite.spark

import cats.effect.IO
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDNode}
import cognite.spark.SparkSchemaHelper._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import fs2.Stream

class ThreeDModelRevisionNodesRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDNode](config, "3dmodelrevisionnodes") {
  override def schema: StructType = structType[ThreeDNode]

  override def toRow(t: ThreeDNode): Row = asRow(t)

  override def uniqueId(a: ThreeDNode): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ThreeDNode]] =
    Seq(client.threeDNodes(modelId, revisionId).list(limit))
}
