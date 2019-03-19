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
    // need to use Vector to avoid this error:
    // Caused by: java.io.NotSerializableException: scala.Array$$anon$2
    securityCategories: Option[Vector[Long]],
    id: Long,
    createdTime: Long,
    lastUpdatedTime: Long)

case class PostTimeSeriesItem(
    name: String,
    isString: Boolean,
    metadata: Option[Map[String, String]],
    unit: Option[String],
    assetId: Option[Long],
    isStep: Boolean,
    description: Option[String],
    securityCategories: Option[Vector[Long]],
    id: Long)

case class UpdateTimeSeriesItem(
    id: Long,
    name: Option[Setter[String]],
    metadata: Option[Setter[Map[String, String]]],
    unit: Option[Setter[String]],
    assetId: Option[Setter[Long]],
    description: Option[Setter[String]],
    securityCategories: Option[Map[String, Option[Vector[Long]]]],
    isString: Option[Setter[Boolean]],
    isStep: Option[Setter[Boolean]]
)
object UpdateTimeSeriesItem {
  def apply(postTimeSeriesItem: PostTimeSeriesItem): UpdateTimeSeriesItem =
    new UpdateTimeSeriesItem(
      postTimeSeriesItem.id,
      Setter[String](Some(postTimeSeriesItem.name)),
      Setter[Map[String, String]](postTimeSeriesItem.metadata),
      Setter[String](postTimeSeriesItem.unit),
      Setter[Long](postTimeSeriesItem.assetId),
      Setter[String](postTimeSeriesItem.description),
      securityCategories = postTimeSeriesItem.securityCategories.map(a => Map("set" -> Option(a))),
      Setter[Boolean](Some(postTimeSeriesItem.isString)),
      Setter[Boolean](Some(postTimeSeriesItem.isStep))
    )
}

class TimeSeriesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends CdpRelation[TimeSeriesItem](config, "3dmodelrevisionnodes")
    with InsertableRelation
    with CdpConnector {

  @transient lazy private val timeSeriesCreated =
    metricsSource.getOrCreateCounter(s"timeseries.created")

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val timeSeriesItems = rows.map { r =>
        val postTimeSeriesItem = fromRow[PostTimeSeriesItem](r)
        // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
        // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
        // Since null keys don't make sense to CDP either, remove them as well.
        val filteredMetadata = postTimeSeriesItem.metadata.map(_.filter { case (k, v) => k != null && v != null })
        postTimeSeriesItem.copy(metadata = filteredMetadata)
      }
      val batches = timeSeriesItems.grouped(config.batchSize.getOrElse(Constants.DefaultBatchSize)).toVector
      batches.parTraverse(updateOrPostTimeSeries).unsafeRunSync()
    })

  private def updateOrPostTimeSeries(timeSeriesItems: Seq[PostTimeSeriesItem]): IO[Unit] = {
    val updateTimeSeriesItems =
      timeSeriesItems.map(i => UpdateTimeSeriesItem(i))
    val updateTimeSeriesUrl =
      uri"${baseUrl(config.project, "0.6", config.baseUrl)}/timeseries/update"
    postOr(config.apiKey, updateTimeSeriesUrl, updateTimeSeriesItems, config.maxRetries) {
      case Response(Right(body), StatusCodes.BadRequest, _, _, _) =>
        decode[Error[TimeSeriesConflict]](body) match {
          case Right(conflict) =>
            resolveConflict(
              timeSeriesItems,
              baseTimeSeriesUrl(config.project),
              updateTimeSeriesItems,
              updateTimeSeriesUrl,
              conflict.error)
          case Left(error) => IO.raiseError(error)
        }
    }
  }

  def resolveConflict(
      timeSeriesItems: Seq[PostTimeSeriesItem],
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
      put(config.apiKey, updateTimeSeriesUrl, timeSeriesToUpdate, config.maxRetries)
    }

    val timeSeriesToCreate = timeSeriesItems.filter(p => notFound.contains(p.id))
    val postItems = if (timeSeriesToCreate.isEmpty) {
      IO.unit
    } else {
      post(config.apiKey, timeSeriesUrl, timeSeriesToCreate, config.maxRetries)
        .flatTap { _ =>
          IO {
            if (config.collectMetrics) {
              timeSeriesCreated.inc(timeSeriesToCreate.length)
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

  override def listUrl(relationConfig: RelationConfig): Uri =
    uri"${config.baseUrl}/api/0.5/projects/${config.project}/timeseries"
}
