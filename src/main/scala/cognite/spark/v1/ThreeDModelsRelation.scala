package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDModel}
import cognite.spark.v1.SparkSchemaHelper._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.Filter
import fs2.Stream

case class ModelItem(id: Long, name: String, createdTime: Long)

class ThreeDModelsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDModel, Long](config, "threeDModels.read") {

  override def schema: StructType = structType[ThreeDModel]

  override def toRow(t: ThreeDModel): Row = asRow(t)

  override def uniqueId(a: ThreeDModel): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ThreeDModel]] =
    Seq(client.threeDModels.list(limit))
}
