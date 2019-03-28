package com.cognite.spark.datasource
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import io.circe.generic.auto._
import io.circe.generic.decoding.DerivedDecoder
import com.softwaremill.sttp.Uri
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.reflect._
import scala.reflect.runtime.universe._

case class Setter[A](set: A, setNull: Boolean)
object Setter {
  def apply[A](set: Option[A]): Option[Setter[A]] =
    set match {
      case None => None
      case _ => Some(new Setter(set.get, false))
    }
}

abstract class CdpRelation[T: DerivedDecoder: TypeTag: ClassTag](
    config: RelationConfig,
    shortName: String)
    extends BaseRelation
    with TableScan
    with Serializable {
  @transient lazy private val itemsRead =
    config.metricsSource.getOrCreateCounter(s"$shortName.read")

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

  def cursors(): Iterator[(Option[String], Option[Int])] = NextCursorIterator(listUrl(), config)
}
