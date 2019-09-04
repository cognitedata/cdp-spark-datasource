package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.concurrent.ExecutionContext

case class PostTimeSeriesDataItems[A](items: Seq[A])
case class TimeSeriesConflict(duplicated: Seq[LegacyName])
case class LegacyName(legacyName: String)
case class TimeSeriesNotFound(notFound: Seq[String])

case class TimeSeriesItem(
    name: String,
    isString: Boolean,
    metadata: Option[Map[String, String]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Option[Boolean],
    description: Option[String],
    // Change this to Option[Vector[Long]] if we start seeing this exception:
    // java.io.NotSerializableException: scala.Array$$anon$2
    securityCategories: Option[Seq[Long]],
    id: Option[Long],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

case class PostTimeSeriesItemBase(
    name: String,
    isString: Boolean,
    metadata: Option[Map[String, String]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Option[Boolean],
    description: Option[String],
    securityCategories: Option[Seq[Long]])

case class PostTimeSeriesItem(
    name: String,
    legacyName: String,
    isString: Boolean,
    metadata: Option[Map[String, String]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Option[Boolean],
    description: Option[String],
    securityCategories: Option[Seq[Long]])
object PostTimeSeriesItem {
  def apply(timeSeriesItem: TimeSeriesItem): PostTimeSeriesItem =
    new PostTimeSeriesItem(
      timeSeriesItem.name,
      timeSeriesItem.name, // Use name as legacyName for now
      timeSeriesItem.isString,
      timeSeriesItem.metadata,
      timeSeriesItem.unit,
      timeSeriesItem.assetId,
      timeSeriesItem.isStep,
      timeSeriesItem.description,
      timeSeriesItem.securityCategories
    )
  def apply(postTimeSeriesItemBase: PostTimeSeriesItemBase): PostTimeSeriesItem =
    new PostTimeSeriesItem(
      postTimeSeriesItemBase.name,
      postTimeSeriesItemBase.name,
      postTimeSeriesItemBase.isString,
      postTimeSeriesItemBase.metadata,
      postTimeSeriesItemBase.unit,
      postTimeSeriesItemBase.assetId,
      postTimeSeriesItemBase.isStep,
      postTimeSeriesItemBase.description,
      postTimeSeriesItemBase.securityCategories
    )
}

// This class is needed to enable partial updates
// since TimeSeriesItem has values that are not optional
case class UpdateTimeSeriesBase(
    name: Option[String],
    isString: Option[Boolean],
    metadata: Option[Map[String, String]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Option[Boolean],
    description: Option[String],
    securityCategories: Option[Seq[Long]],
    id: Option[Long])

case class UpdateTimeSeriesItem(
    id: Long,
    name: Option[Setter[String]],
    metadata: Option[Setter[Map[String, String]]],
    unit: Option[Setter[String]],
    assetId: Option[Setter[Long]],
    description: Option[Setter[String]],
    securityCategories: Option[Map[String, Option[Seq[Long]]]],
    isString: Option[Setter[Boolean]],
    isStep: Option[Setter[Boolean]]
)

object UpdateTimeSeriesItem {
  def apply(timeSeriesItem: TimeSeriesItem): UpdateTimeSeriesItem =
    new UpdateTimeSeriesItem(
      timeSeriesItem.id.get,
      Setter[String](Some(timeSeriesItem.name)),
      Setter[Map[String, String]](timeSeriesItem.metadata),
      Setter[String](timeSeriesItem.unit),
      Setter[Long](timeSeriesItem.assetId),
      Setter[String](timeSeriesItem.description),
      securityCategories = timeSeriesItem.securityCategories.map(a => Map("set" -> Option(a))),
      Setter[Boolean](Some(timeSeriesItem.isString)),
      Setter[Boolean](timeSeriesItem.isStep)
    )
  def apply(timeSeriesBase: UpdateTimeSeriesBase): UpdateTimeSeriesItem =
    new UpdateTimeSeriesItem(
      timeSeriesBase.id.get,
      Setter[String](timeSeriesBase.name),
      Setter[Map[String, String]](timeSeriesBase.metadata),
      Setter[String](timeSeriesBase.unit),
      Setter[Long](timeSeriesBase.assetId),
      Setter[String](timeSeriesBase.description),
      securityCategories = timeSeriesBase.securityCategories.map(a => Map("set" -> Option(a))),
      Setter[Boolean](timeSeriesBase.isString),
      Setter[Boolean](timeSeriesBase.isStep)
    )
}

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[TimeSeriesItem](config, "timeseries")
    with InsertableRelation {

  @transient lazy private val timeSeriesCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"timeseries.created")

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val urlsFilters = urlsWithFilters(filters, listUrl())
    val urls = if (urlsFilters.isEmpty) {
      Seq(listUrl())
    } else {
      urlsFilters
    }

    TimeSeriesRdd[TimeSeriesItem](
      sqlContext.sparkContext,
      (e: TimeSeriesItem) => {
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

  override def cursors(): Iterator[(Option[String], Option[Int])] =
    NextCursorIterator[TimeSeriesItem](listUrl(), config, false)

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val postTimeSeriesItems = rows.map { r =>
      val postTimeSeriesItem = PostTimeSeriesItem(fromRow[PostTimeSeriesItemBase](r))
      postTimeSeriesItem.copy(metadata = filterMetadata(postTimeSeriesItem.metadata))
    }
    post(config, baseTimeSeriesUrl(config.project, "v1"), postTimeSeriesItems)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map { r =>
      val timeSeriesItem = fromRow[TimeSeriesItem](r)
      timeSeriesItem.copy(metadata = filterMetadata(timeSeriesItem.metadata))
    }
    createOrUpdate(timeSeriesItems)
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val updateTimeSeriesBaseItems =
      rows.map { r =>
        val timeSeriesItem = fromRow[UpdateTimeSeriesBase](r)
        timeSeriesItem.copy(metadata = filterMetadata(timeSeriesItem.metadata))
      }

    // Time series must have an id when using update
    if (updateTimeSeriesBaseItems.exists(_.id.isEmpty)) {
      throw new IllegalArgumentException("Time series must have an id when using update")
    }

    val updateTimeSeriesItems = updateTimeSeriesBaseItems.map(UpdateTimeSeriesItem(_))

    post(
      config,
      uri"${baseTimeSeriesUrl(config.project, "0.6")}/update",
      updateTimeSeriesItems
    )
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val timeSeriesItems = rows.map { r =>
        val timeSeriesItem = fromRow[TimeSeriesItem](r)
        timeSeriesItem.copy(metadata = filterMetadata(timeSeriesItem.metadata))
      }
      val batches =
        timeSeriesItems.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup.parTraverse(createOrUpdate).unsafeRunSync()
      }
      ()
    })

  override def delete(rows: Seq[Row]): IO[Unit] =
    deleteItems(config, baseTimeSeriesUrl(config.project, "0.6"), rows)

  private val updateTimeSeriesUrl =
    uri"${baseUrl(config.project, "0.6", config.baseUrl)}/timeseries/update"

  private def createOrUpdate(timeSeriesItems: Seq[TimeSeriesItem]): IO[Unit] = {

    val postTimeSeriesItems = timeSeriesItems.map(t => PostTimeSeriesItem(t))

    postOr(config, baseTimeSeriesUrl(config.project), postTimeSeriesItems) {
      case response @ Response(Right(body), StatusCodes.Conflict, _, _, _) => {
        val b = body
        val x = decode[Error[TimeSeriesConflict]](body)
        decode[Error[TimeSeriesConflict]](body) match {
          case Right(conflict) =>
            resolveConflict(timeSeriesItems, conflict.error)
          case Left(_) => IO.raiseError(onError(baseTimeSeriesUrl(config.project), response))
        }
      }
    }.flatTap { _ =>
      IO {
        if (config.collectMetrics) {
          timeSeriesCreated.inc(timeSeriesItems.length)
        }
      }
    }
  }

  def resolveConflict(
      timeSeriesItems: Seq[TimeSeriesItem],
      timeSeriesConflict: TimeSeriesConflict): IO[Unit] = {
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

    val conflictingTimeSeriesNames = timeSeriesConflict.duplicated.map(_.legacyName)

    val (timeSeriesToUpdate, timeSeriesToCreate) =
      timeSeriesItems.partition(ts => conflictingTimeSeriesNames.contains(ts.name))

    // Time series must have an id when using update
    val updatesWithNoId = timeSeriesToUpdate.filter(_.id.isEmpty)
    if (updatesWithNoId.nonEmpty) {
      throw new IllegalArgumentException(
        s"The following existing time series do not have an ID: ${updatesWithNoId
          .map(_.name)}")
    }
    val updateItems = if (timeSeriesToUpdate.isEmpty) {
      IO.unit
    } else {
      post(
        config,
        updateTimeSeriesUrl,
        timeSeriesToUpdate.map(t => UpdateTimeSeriesItem(t))
      )
    }

    val createItems = if (timeSeriesToCreate.isEmpty) {
      IO.unit
    } else {
      post(
        config,
        baseTimeSeriesUrl(config.project),
        timeSeriesToCreate.map(t => PostTimeSeriesItem(t))
      ).flatTap { _ =>
        IO {
          if (config.collectMetrics) {
            timeSeriesCreated.inc(timeSeriesToCreate.length)
          }
        }
      }
    }

    (updateItems, createItems).parMapN((_, _) => ())
  }

  def baseTimeSeriesUrl(project: String, version: String = "v1"): Uri =
    uri"${baseUrl(project, version, config.baseUrl)}/timeseries"

  override def schema: StructType = structType[TimeSeriesItem]

  override def toRow(t: TimeSeriesItem): Row = asRow(t)

  override def listUrl(): Uri =
    baseTimeSeriesUrl(config.project, "v1")
}

object TimeSeriesRelation
    extends DeleteSchema
    with UpsertSchema
    with InsertSchema
    with UpdateSchema {
  val insertSchema = structType[PostTimeSeriesItem]
  val upsertSchema = StructType(structType[TimeSeriesItem].filterNot(field =>
    Seq("createdTime", "lastUpdatedTime").contains(field.name)))
  val updateSchema = StructType(structType[UpdateTimeSeriesBase])
}
