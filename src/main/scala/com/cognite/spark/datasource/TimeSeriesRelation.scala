package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp._
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import SparkSchemaHelper._
import org.apache.spark.datasource.MetricsSource

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
    isStep: Boolean,
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
    isStep: Boolean,
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
      Setter[Boolean](Some(timeSeriesItem.isStep))
    )
}

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[TimeSeriesItem](config, "timeseries")
    with InsertableRelation
    with CdpConnector {

  @transient lazy private val timeSeriesCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"timeseries.created")

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val postTimeSeriesItems = rows.map { r =>
      val postTimeSeriesItem = fromRow[PostTimeSeriesItem](r)
      postTimeSeriesItem.copy(metadata = filterMetadata(postTimeSeriesItem.metadata))
    }
    post(config.auth, baseTimeSeriesUrl(config.project), postTimeSeriesItems, config.maxRetries)
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map(r => fromRow[TimeSeriesItem](r))
    updateOrPostTimeSeries(timeSeriesItems)
  }

  override def update(rows: Seq[Row]): IO[Unit] = {
    val timeSeriesItems = rows.map(r => fromRow[TimeSeriesItem](r))
    val updateTimeSeriesItems = timeSeriesItems.map(t => UpdateTimeSeriesItem(t))

    post(
      config.auth,
      uri"${baseTimeSeriesUrl(config.project)}/update",
      updateTimeSeriesItems,
      config.maxRetries)
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
      batches.parTraverse(updateOrPostTimeSeries).unsafeRunSync()
      ()
    })

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
        config.auth,
        baseTimeSeriesUrl(config.project),
        postTimeSeriesToCreate,
        config.maxRetries)
        .flatTap { _ =>
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
      postOr(config.auth, updateTimeSeriesUrl, updateTimeSeriesItems, config.maxRetries) {
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
      post(config.auth, updateTimeSeriesUrl, timeSeriesToUpdate, config.maxRetries)
    }

    val timeSeriesToCreate = timeSeriesItems.filter(p => p.id.exists(notFound.contains(_)))
    val postTimeSeriesToCreate = timeSeriesToCreate.map(t => PostTimeSeriesItem(t))
    val postItems = if (timeSeriesToCreate.isEmpty) {
      IO.unit
    } else {
      post(config.auth, timeSeriesUrl, postTimeSeriesToCreate, config.maxRetries)
        .flatTap { _ =>
          IO {
            if (config.collectMetrics) {
              timeSeriesCreated.inc(postTimeSeriesToCreate.length)
            }
          }
        }
    }

    (putItems, postItems).parMapN((_, _) => ())
  }

  def baseTimeSeriesUrl(project: String): Uri =
    uri"${baseUrl(project, "0.5", config.baseUrl)}/timeseries"

  override def schema: StructType = structType[TimeSeriesItem]

  override def toRow(t: TimeSeriesItem): Row = asRow(t)

  override def listUrl(): Uri =
    uri"${config.baseUrl}/api/0.5/projects/${config.project}/timeseries"
}
