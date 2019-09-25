package cognite.spark

import cats.effect.IO
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDNode}
import cognite.spark.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ThreeDModelRevisionNodesRelation(config: RelationConfig, modelId: Long, revisionId: Long)(
    val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDNode](config, "3dmodelrevisionnodes") {
  override def schema: StructType = structType[ThreeDNode]

  override def toRow(t: ThreeDNode): Row = asRow(t)

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: StatusCode): Seq[fs2.Stream[IO, ThreeDNode]] =
    Seq(client.threeDNodes(modelId, revisionId).list(limit))
}
