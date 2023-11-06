package cognite.spark.v1

import cats.implicits._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.InsertableRelation

import scala.annotation.unused

abstract class SdkV1InsertableRelation[A <: Product, I](config: RelationConfig, shortName: String)
    extends SdkV1Relation[A, I](config, shortName)
    with Serializable
    with InsertableRelation {
  def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): TracedIO[Unit]

  override def insert(data: DataFrame, @unused overwrite: Boolean): Unit = {
    import CdpConnector.ioRuntime
    config
      .trace("insert")(SparkF(data).foreachPartition((rows: Iterator[Row]) => {
        val batches =
          rows.grouped(config.batchSize.getOrElse(cognite.spark.v1.Constants.DefaultBatchSize)).toVector
        batches
          .parTraverse_(getFromRowsAndCreate(_))
      }))
      .unsafeRunSync()
  }

}
