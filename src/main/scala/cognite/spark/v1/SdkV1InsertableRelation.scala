package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.InsertableRelation

import scala.annotation.unused

abstract class SdkV1InsertableRelation[A <: Product, I](config: RelationConfig, shortName: String)
    extends SdkV1Relation[A, I](config, shortName)
    with Serializable
    with InsertableRelation {
  def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit]

  override def insert(data: DataFrame, @unused overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      import CdpConnector._
      val batches =
        rows.grouped(config.batchSize.getOrElse(cognite.spark.v1.Constants.DefaultBatchSize)).toVector
      batches
        .parTraverse_(getFromRowsAndCreate(_))
        .unsafeRunSync()
    })

}
