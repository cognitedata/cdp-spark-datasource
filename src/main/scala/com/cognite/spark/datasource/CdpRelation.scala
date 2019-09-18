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
  GreaterThan,
  GreaterThanOrEqual,
  In,
  IsNotNull,
  LessThan,
  LessThanOrEqual,
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

case class DeleteItem(id: Long)

sealed trait PushdownExpression
final case class PushdownFilter(fieldName: String, value: String) extends PushdownExpression
final case class PushdownAnd(left: PushdownExpression, right: PushdownExpression)
    extends PushdownExpression
final case class PushdownFilters(filters: Seq[PushdownExpression]) extends PushdownExpression
final case class NoPushdown() extends PushdownExpression

object PushdownUtilities {
  def pushdownToUri(parameters: Seq[Map[String, String]], uri: Uri): Seq[Uri] =
    parameters.map(params => uri.params(params))

  def pushdownToParameters(p: PushdownExpression): Seq[Map[String, String]] =
    p match {
      case PushdownAnd(left, right) =>
        handleAnd(pushdownToParameters(left), pushdownToParameters(right))
      case PushdownFilter(field, value) => Seq(Map[String, String](field -> value))
      case PushdownFilters(filters) => filters.flatMap(pushdownToParameters)
      case NoPushdown() => Seq()
    }

  def handleAnd(
      left: Seq[Map[String, String]],
      right: Seq[Map[String, String]]): Seq[Map[String, String]] =
    if (left.isEmpty) {
      right
    } else if (right.isEmpty) {
      left
    } else {
      for {
        l <- left
        r <- right
      } yield l ++ r
    }

  def toPushdownFilterExpression(filters: Array[Filter]): PushdownExpression =
    if (filters.isEmpty) {
      NoPushdown()
    } else {
      filters
        .map(getFilter)
        .reduce(PushdownAnd(_, _))
    }

  // Spark will still filter the result after pushdown filters are applied, see source code for
  // PrunedFilteredScan, hence it's ok that our pushdown filter reads some data that should ideally
  // be filtered out
  // scalastyle:off
  def getFilter(filter: Filter): PushdownExpression =
    filter match {
      case IsNotNull(colName) => NoPushdown()
      case EqualTo(colName, value) => PushdownFilter(colName, value.toString)
      case EqualNullSafe(colName, value) => PushdownFilter(colName, value.toString)
      case GreaterThan(colName, value) =>
        PushdownFilter("min" + colName.capitalize, value.toString)
      case GreaterThanOrEqual(colName, value) =>
        PushdownFilter("min" + colName.capitalize, value.toString)
      case LessThan(colName, value) =>
        PushdownFilter("max" + colName.capitalize, value.toString)
      case LessThanOrEqual(colName, value) =>
        PushdownFilter("max" + colName.capitalize, value.toString)
      case In(colName, values) =>
        PushdownFilters(values.map(v => PushdownFilter(colName, v.toString)))
      case And(f1, f2) => PushdownAnd(getFilter(f1), getFilter(f2))
      case Or(f1, f2) => PushdownFilters(Seq(getFilter(f1), getFilter(f2)))
      case _ => NoPushdown()
    }

  def shouldGetAll(
      pushdownExpression: PushdownExpression,
      fieldsWithPushdownFilter: Seq[String]): Boolean =
    pushdownExpression match {
      case PushdownAnd(left, right) =>
        shouldGetAll(left, fieldsWithPushdownFilter) || shouldGetAll(
          right,
          fieldsWithPushdownFilter)
      case PushdownFilter(field, _) => !fieldsWithPushdownFilter.contains(field)
      case PushdownFilters(filters) =>
        filters
          .map(shouldGetAll(_, fieldsWithPushdownFilter))
          .exists(identity)
      case NoPushdown() => false
    }
}

abstract class CdpRelation[T <: Product: Decoder](config: RelationConfig, shortName: String)
    extends BaseRelation
    with TableScan
    with Serializable
    with PrunedFilteredScan
    with CdpConnector {
  import PushdownUtilities._
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

  def urlsWithFilters(filters: Array[Filter], uri: Uri): Seq[Uri] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val getAll = shouldGetAll(pushdownFilterExpression, fieldsWithPushdownFilter)
    val params = pushdownToParameters(pushdownFilterExpression)
    val urlsWithFilter = pushdownToUri(params, uri).distinct

    if (urlsWithFilter.isEmpty || getAll) {
      Seq(uri)
    } else {
      urlsWithFilter
    }
  }

  def toRow(t: T): Row

  def toRow(item: T, requiredColumns: Array[String]): Row =
    if (requiredColumns.isEmpty) {
      toRow(item)
    } else {
      val values = item.productIterator
      val itemMap = item.getClass.getDeclaredFields.map(_.getName -> values.next).toMap
      Row.fromSeq(requiredColumns.map(itemMap(_)).toSeq)
    }

  def listUrl(): Uri

  def cursors(): Iterator[(Option[String], Option[Int])] =
    NextCursorIterator(listUrl(), config, true)

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
