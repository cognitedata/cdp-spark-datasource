package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.concurrent.ExecutionContext

case class PostTimeSeriesDataItems[A](items: Seq[A])
case class TimeSeriesConflict(notFound: Seq[Long])
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

case class PostTimeSeriesItem(
    name: String,
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
      timeSeriesItem.isString,
      timeSeriesItem.metadata,
      timeSeriesItem.unit,
      timeSeriesItem.assetId,
      timeSeriesItem.isStep,
      timeSeriesItem.description,
      timeSeriesItem.securityCategories
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
    id: Option[Long],
    createdTime: Option[Long],
    lastUpdatedTime: Option[Long])

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

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val postTimeSeriesItems = rows.map { r =>
      val postTimeSeriesItem = fromRow[PostTimeSeriesItem](r)
      postTimeSeriesItem.copy(metadata = filterMetadata(postTimeSeriesItem.metadata))
    }
    post(config, baseTimeSeriesUrl(config.project), postTimeSeriesItems)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map(r => fromRow[TimeSeriesItem](r))
    updateOrPostTimeSeries(timeSeriesItems)
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val updateTimeSeriesBaseItems =
      rows.map(r => fromRow[UpdateTimeSeriesBase](r))

    // Time series must have an id when using update
    if (updateTimeSeriesBaseItems.exists(_.id.isEmpty)) {
      throw new IllegalArgumentException("Time series must have an id when using update")
    }

    val updateTimeSeriesItems = updateTimeSeriesBaseItems.map(UpdateTimeSeriesItem(_))

    post(
      config,
      uri"${baseTimeSeriesUrl(config.project)}/update",
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
        batchGroup.parTraverse(updateOrPostTimeSeries).unsafeRunSync()
      }
      ()
    })

  override def delete(rows: Seq[Row]): IO[Unit] =
    deleteItems(config, baseTimeSeriesUrl(config.project, "0.6"), rows)

  private val updateTimeSeriesUrl =
    uri"${baseUrl(config.project, "0.6", config.baseUrl)}/timeseries/update"

  private def updateOrPostTimeSeries(timeSeriesItems: Seq[TimeSeriesItem]): IO[Unit] = {
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

    val updateTimeSeriesItems =
      timeSeriesItems.filter(_.id.nonEmpty).map(i => UpdateTimeSeriesItem(i))
    val postTimeSeriesToCreate =
      timeSeriesItems.filter(_.id.isEmpty).map(i => PostTimeSeriesItem(i))
    val postItems = if (postTimeSeriesToCreate.isEmpty) {
      IO.unit
    } else {
      post(
        config,
        baseTimeSeriesUrl(config.project),
        postTimeSeriesToCreate
      ).flatTap { _ =>
        IO {
          if (config.collectMetrics) {
            timeSeriesCreated.inc(postTimeSeriesToCreate.length)
          }
        }
      }
    }
    val updateItems = if (updateTimeSeriesItems.isEmpty) {
      IO.unit
    } else {
      postOr(
        config,
        updateTimeSeriesUrl,
        updateTimeSeriesItems
      ) {
        case response @ Response(Right(body), StatusCodes.BadRequest, _, _, _) =>
          decode[Error[TimeSeriesConflict]](body) match {
            case Right(conflict) =>
              resolveConflict(
                timeSeriesItems,
                baseTimeSeriesUrl(config.project),
                updateTimeSeriesItems,
                updateTimeSeriesUrl,
                conflict.error)
            case Left(_) => IO.raiseError(onError(updateTimeSeriesUrl, response))
          }
      }
    }
    (updateItems, postItems).parMapN((_, _) => ())
  }

  def resolveConflict(
      timeSeriesItems: Seq[TimeSeriesItem],
      timeSeriesUrl: Uri,
      updateTimeSeriesItems: Seq[UpdateTimeSeriesItem],
      updateTimeSeriesUrl: Uri,
      duplicateIds: TimeSeriesConflict): IO[Unit] = {
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

    val notFound = duplicateIds.notFound
    val timeSeriesToUpdate = updateTimeSeriesItems.filter(p => !notFound.contains(p.id))
    val putItems = if (timeSeriesToUpdate.isEmpty) {
      IO.unit
    } else {
      post(
        config,
        updateTimeSeriesUrl,
        timeSeriesToUpdate
      )
    }

    val timeSeriesToCreate = timeSeriesItems.filter(p => p.id.exists(notFound.contains(_)))
    val postTimeSeriesToCreate = timeSeriesToCreate.map(t => PostTimeSeriesItem(t))
    val postItems = if (timeSeriesToCreate.isEmpty) {
      IO.unit
    } else {
      post(
        config,
        timeSeriesUrl,
        postTimeSeriesToCreate
      ).flatTap { _ =>
        IO {
          if (config.collectMetrics) {
            timeSeriesCreated.inc(postTimeSeriesToCreate.length)
          }
        }
      }
    }

    (putItems, postItems).parMapN((_, _) => ())
  }

  def baseTimeSeriesUrl(project: String, version: String = "0.5"): Uri =
    uri"${baseUrl(project, version, config.baseUrl)}/timeseries"

  override def schema: StructType = structType[TimeSeriesItem]

  override def toRow(t: TimeSeriesItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.5/projects/${config.project}/timeseries"
}
