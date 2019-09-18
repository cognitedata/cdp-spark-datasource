package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.{GenericClient, ThreeDModel}
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.Filter
case class ModelItem(id: Long, name: String, createdTime: Long)

class ThreeDModelsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[ThreeDModel](config, "threeDModels.read") {

  override def schema: StructType = structType[ThreeDModel]

  override def toRow(t: ThreeDModel): Row = asRow(t)

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Long],
      numPartitions: StatusCode): Seq[fs2.Stream[IO, ThreeDModel]] =
    Seq(
      config.limit.map(client.threeDModels.listWithLimit(_)).getOrElse(client.threeDModels.list)
    )
}
