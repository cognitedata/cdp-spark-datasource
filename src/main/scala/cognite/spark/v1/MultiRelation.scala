package cognite.spark.v1

import cats.effect.IO
import cats.implicits.catsSyntaxParallelTraverse_
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StructField, StructType}
class MultiRelation(
    config: RelationConfig,
    relations: Map[String, BaseRelation with WritableRelation with Serializable])(
    val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation
    with WritableRelation
    with Serializable {
  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      import CdpConnector._
      val batches =
        rows.grouped(config.batchSize.getOrElse(cognite.spark.v1.Constants.DefaultBatchSize)).toVector
      batches.parTraverse_(insert).unsafeRunSync()
    })

  override val schema: StructType = StructType(relations.map {
    case (name, rel) =>
      StructField(name, rel.schema, nullable = false)
  }.toSeq)

  private def eachStructField(
      rows: Seq[Row],
      action: (WritableRelation, Seq[Row]) => IO[Unit]): IO[Unit] =
    relations.toVector.parTraverse_ {
      case (name, relation) =>
        val rs = rows.map(row => {
          row.getAs[Row](name)
        })
        action(relation, rs)
    }

  override def insert(rows: Seq[Row]): IO[Unit] = eachStructField(rows, _.insert(_))
  override def upsert(rows: Seq[Row]): IO[Unit] = eachStructField(rows, _.upsert(_))
  override def update(rows: Seq[Row]): IO[Unit] = eachStructField(rows, _.update(_))
  override def delete(rows: Seq[Row]): IO[Unit] = eachStructField(rows, _.delete(_))
}
