package cognite.spark.v1

import cats.effect.IO
import cats.implicits.catsSyntaxParallelTraverse_
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StructField, StructType}
class MultiRelation(config: RelationConfig, relations: Map[String, BaseRelation])(
    val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation
    with Serializable {
  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      import CdpConnector._
      val batches =
        rows.grouped(config.batchSize.getOrElse(cognite.spark.v1.Constants.DefaultBatchSize)).toVector
      batches.parTraverse_(getFromRowsAndCreate).unsafeRunSync()
    })

  private def getFromRowsAndCreate(rows: Seq[Row]): IO[Unit] = relations.toVector.parTraverse_ {
    case (name, relation: WritableRelation) =>
      val rs = rows.map(row => {
        row.getAs[Row](name)
      })
      relation.insert(rs)
    case _ => throw new CdfSparkException("Relation is not writable")
  }

  override val schema: StructType = StructType(relations.map {
    case (name, rel) =>
      StructField(name, rel.schema, nullable = false)
  }.toSeq)
}
