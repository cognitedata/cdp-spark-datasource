package cognite.spark.v1

import cats.effect.IO
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDModel}
import fs2.Stream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

final case class ModelItem(id: Long, name: String, createdTime: Long)

class ThreeDModelsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDModel, Long](config, ThreeDModelsRelation.name) {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  override def schema: StructType = structType[ThreeDModel]()

  override def toRow(t: ThreeDModel): Row = asRow(t)

  override def uniqueId(a: ThreeDModel): Long = a.id

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, ThreeDModel]] =
    Seq(client.threeDModels.list(config.limitPerPartition))
}

object ThreeDModelsRelation extends NamedRelation {
  override val name: String = "3dmodels"
}
