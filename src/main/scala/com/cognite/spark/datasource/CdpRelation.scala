package com.cognite.spark.datasource
import cats.effect.IO
import com.codahale.metrics.Counter
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.Decoder
import org.apache.spark.sql.sources.{
  And,
  BaseRelation,
  EqualNullSafe,
  EqualTo,
  Filter,
  In,
  IsNotNull,
  Or,
  PrunedFilteredScan,
  TableScan
}
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

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

case class PushdownFilter(fieldName: String, value: String)

abstract class CdpRelation[T <: Product: Decoder](config: RelationConfig, shortName: String)
    extends BaseRelation
    with TableScan
    with Serializable
    with PrunedFilteredScan
    with CdpConnector {
  @transient lazy protected val itemsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"$shortName.read")

  val fieldsWithPushdownFilter: Seq[String] = Seq[String]()

  val sqlContext: SQLContext
  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val urlsFilters = urlsWithFilters(filters, listUrl())
    val urls = if (urlsFilters.isEmpty) {
      Seq(listUrl())
    } else {
      urlsFilters
    }

    CdpRdd[T](
      sqlContext.sparkContext,
      (e: T) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(e, requiredColumns)
      },
      listUrl(),
      config,
      urls,
      cursors()
    )
  }

  def urlsWithFilters(filters: Array[Filter], uri: Uri): Seq[Uri] =
    fieldsWithPushdownFilter
      .map(f => (f, filters.flatMap(getFilter(_, f)))) // map from fieldname to its filtering values
      .flatMap(pdf => pdf._2.map(v => PushdownFilter(pdf._1, v)))
      .distinct
      .map(f => uri.param(f.fieldName, f.value))

  def toRow(t: T): Row

  def toRow(item: T, requiredColumns: Array[String]): Row =
    if (requiredColumns.isEmpty) {
      toRow(item)
    } else {
      val values = item.productIterator
      val itemMap = item.getClass.getDeclaredFields.map(_.getName -> values.next).toMap
      Row.fromSeq(requiredColumns.map(itemMap(_)).toSeq)
    }

  // Spark will still filter the result after pushdown filters are applied, see source code for
  // PrunedFilteredScan, hence it's ok that our pushdown filter reads some data that should ideally
  // be filtered out
  def getFilter(filter: Filter, colName: String): Seq[String] =
    filter match {
      case IsNotNull(`colName`) => Seq()
      case EqualTo(`colName`, value) => Seq(value.toString)
      case EqualNullSafe(`colName`, value) => Seq(value.toString)
      case In(`colName`, values) => values.map(v => v.toString)
      case And(f1, f2) => getFilter(f1, colName) ++ getFilter(f2, colName)
      case Or(f1, f2) => getFilter(f1, colName) ++ getFilter(f2, colName)
      case _ => Seq()
    }

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

trait InsertSchema {
  val insertSchema: StructType
}

trait UpsertSchema {
  val upsertSchema: StructType
}

trait UpdateSchema {
  val updateSchema: StructType
}

abstract class DeleteSchema {
  val deleteSchema: StructType = StructType(Seq(StructField("id", DataTypes.LongType)))
}
