package com.cognite.spark.datasource
import cats.effect.IO
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.Decoder
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

case class Setter[A](set: A, setNull: Boolean)
object Setter {
  def apply[A](set: Option[A]): Option[Setter[A]] =
    set match {
      case None => None
      case _ => Some(new Setter(set.get, false))
    }
}
case class NonNullableSetter[A](set: A)

case class DeleteItem(
    id: Long
)

abstract class CdpRelation[T: Decoder](config: RelationConfig, shortName: String)
    extends BaseRelation
    with TableScan
    with Serializable
    with CdpConnector {
  @transient lazy private val itemsRead =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")

  val sqlContext: SQLContext
  override def buildScan(): RDD[Row] =
    CdpRdd[T](
      sqlContext.sparkContext,
      (e: T) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(e)
      },
      listUrl(),
      config,
      cursors()
    )

  def toRow(t: T): Row

  def listUrl(): Uri

  def cursors(): Iterator[(Option[String], Option[Int])] =
    NextCursorIterator(listUrl(), config)

  def deleteItems(config: RelationConfig, baseUrl: Uri, rows: Seq[Row]): IO[Unit] = {
    val deleteItems: Seq[Long] = rows.map(r => fromRow[DeleteItem](r).id)
    post(
      config,
      uri"$baseUrl/delete",
      deleteItems
    )
  }

  def insert(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "abort".""")

  def upsert(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "upsert".""")

  def update(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "update".""")

  def delete(rows: Seq[Row]): IO[Unit] =
    throw new IllegalArgumentException(
      s"""$shortName does not support the "onconflict" option "delete".""")

}
