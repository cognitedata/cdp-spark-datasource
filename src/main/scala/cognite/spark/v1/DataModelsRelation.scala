package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities.{
  executeFilterOnePartition,
  pushdownToFilters,
  pushdownToParameters,
  toPushdownFilterExpression
}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class DataModelMappingsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[DataModelMappingReadSchema, String](config, "models")
    with InsertableRelation
    with WritableRelation {
  import CdpConnector._

  override def schema: StructType = ???

  def toRow(a: DataModelMappingReadSchema): Row = ???

  def uniqueId(a: DataModelMappingReadSchema): String = a.externalId

//  override def getStreams(filters: Array[Filter])(
//      client: GenericClient[IO],
//      limit: Option[Int],
//      numPartitions: Int): Seq[Stream[IO, DataModelMappingReadSchema]] = {
//      alphaClient.dataModelMappings.list().map(_.map{
//        mapping: DataModelMapping =>
//          val pushdownFilterExpression = toPushdownFilterExpression(filters)
//          val filtersAsMaps = pushdownToParameters(pushdownFilterExpression).toVector
//          DataModelMappingReadSchema(mapping.externalId, )
//
//      })
//  }

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, DataModelMappingReadSchema]] = ???

  def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert not supported for data model mappings.")

  def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update not supported for data model mappings.")

  override def delete(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Delete not supported for data model mappings.")

  override def upsert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Upsert not supported for data model mappings.")

}
final case class DataModelMappingReadSchema(
    externalId: String,
    `type`: Map[String, String],
    nullable: Map[String, Option[Boolean]]
)

object DataModelMappingsRelation {}
