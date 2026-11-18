package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceDefinition, TypePropertyDefinition}
import fs2.{Chunk, Stream}
import io.circe.{Decoder, Encoder}
import natchez.Trace.Implicits.noop
import org.apache.spark.TaskContext
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.PrunedScan
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.log4s.getLogger
import sttp.client3._
import sttp.client3.circe._

import java.util.UUID
import scala.concurrent.duration._

/**
  * Read-only relation over the batch API. Polls the batch until it is ready, fetches its read
  * cursors, then pages each cursor to exhaustion through `/models/swamp/read`.
  *
  * @param config common relation configs
  * @param statusId id of the batch, returned when it was begun
  * @param typing property typing returned when the batch was begun; needed to decode instance properties
  * @param sqlContext sql context
  */
class BatchApiRelation(
    config: RelationConfig,
    statusId: UUID,
    typing: Option[Map[String, Map[String, Map[String, TypePropertyDefinition]]]],
    pollInterval: FiniteDuration = BatchApiRelation.defaultPollInterval
)(val sqlContext: SQLContext)
    extends CdfRelation(config, BatchApiRelation.name)
    with PrunedScan {
  @transient private lazy val logger = getLogger

  override def schema: StructType = BatchApiRelation.schema

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    SdkV1Rdd[InstanceDefinition, (String, String)](
      sqlContext.sparkContext,
      config,
      (instance: InstanceDefinition, _) => toRow(instance, requiredColumns),
      instance => (instance.space, instance.externalId),
      getStreams,
      // the batch API hands out disjoint cursors, so there is nothing to deduplicate
      deduplicateRows = false
    )

  // The read cursors are only known once the batch is ready, so polling happens inside the stream.
  // A single stream also means SdkV1Rdd creates exactly one Spark partition: only one task reads.
  private[v1] def getStreams(client: GenericClient[IO]): Seq[Stream[IO, InstanceDefinition]] =
    Seq(
      Stream
        .eval(pollUntilReady(client).flatMap(fetchReadCursors(client, _)))
        .flatMap(cursors => Stream.emits(cursors).flatMap(readAll(client, _))))

  // HACK: RESOURCE_TYPE is sealed in the SDK, so tag with a plain string: the metrics backend only calls toString on it.
  // TODO: while the SDK does not have this resource, do this hack
  private def requestSession(client: GenericClient[IO]) = {
    val session = client.requestSession
    session.copy(tags = session.tags + (GenericClient.RESOURCE_TYPE_TAG -> BatchApiRelation.name))
  }

  private def pollUntilReady(client: GenericClient[IO]): IO[UUID] = {
    val session = requestSession(client)
    session
      .get[PollResponse, PollResponse](
        uri"${session.baseUrl}/models/swamp/poll/$statusId",
        identity
      )
      .flatTap { response =>
        IO(logger.info(s"Batch $statusId poll status: ${response.status.value}")) *>
          incPollMetrics(response.status)
      }
      .flatMap {
        case PollResponse.Ready(readyId) => IO.pure(readyId)
        case PollResponse.Failed =>
          IO.raiseError(new CdfSparkException(s"Batch API batch $statusId failed"))
        case _ => IO.sleep(pollInterval) *> pollUntilReady(client)
      }
  }

  private def incPollMetrics(status: SyncStatus): IO[Unit] =
    IO(
      if (config.collectMetrics) {
        MetricsSource
          .getOrCreateAttemptTrackingCounter(
            config.metricsTrackAttempts,
            config.metricsPrefix,
            s"$name.poll.${status.value}",
            Option(TaskContext.get()))
          .inc()
      }
    )

  private def fetchReadCursors(client: GenericClient[IO], readyId: UUID): IO[Seq[String]] = {
    val session = requestSession(client)
    session.get[Seq[String], ReadCursorsResponse](
      uri"${session.baseUrl}/models/swamp/cursors/$readyId",
      _.readCursors
    )
  }

  private def readAll(client: GenericClient[IO], readCursor: String): Stream[IO, InstanceDefinition] =
    Stream.unfoldChunkEval[IO, Option[String], InstanceDefinition](Some(readCursor)) {
      case None => IO.pure(None)
      case Some(cursor) =>
        readPage(client, cursor).map(page => Some((Chunk.from(page.items), page.nextReadCursor)))
    }

  private def readPage(client: GenericClient[IO], readCursor: String): IO[ReadResponse] = {
    implicit val readResponseDecoder: Decoder[ReadResponse] = ReadResponse.decoder(typing)
    val session = requestSession(client)
    session.post[ReadResponse, ReadResponse, ReadRequest](
      ReadRequest(readCursor),
      uri"${session.baseUrl}/models/swamp/read",
      identity
    )
  }

  private def toRow(instance: InstanceDefinition, requiredColumns: Array[String]): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    val values = Map[String, Any](
      "instanceType" -> instance.instanceType.productPrefix.toLowerCase,
      "space" -> instance.space,
      "externalId" -> instance.externalId,
      "version" -> instance.version.orNull,
      "createdTime" -> instance.createdTime,
      "lastUpdatedTime" -> instance.lastUpdatedTime,
      "deletedTime" -> instance.deletedTime.orNull
    )
    val columns = if (requiredColumns.isEmpty) schema.fieldNames else requiredColumns
    Row.fromSeq(columns.toSeq.map(values))
  }
}

object BatchApiRelation extends NamedRelation {
  override val name: String = "BATCHAPI"

  private val defaultPollInterval = 15.seconds

  val schema: StructType = DataTypes.createStructType(
    Array(
      DataTypes.createStructField("instanceType", DataTypes.StringType, false),
      DataTypes.createStructField("space", DataTypes.StringType, false),
      DataTypes.createStructField("externalId", DataTypes.StringType, false),
      DataTypes.createStructField("version", DataTypes.LongType, true),
      DataTypes.createStructField("createdTime", DataTypes.LongType, false),
      DataTypes.createStructField("lastUpdatedTime", DataTypes.LongType, false),
      DataTypes.createStructField("deletedTime", DataTypes.LongType, true)
    ))
}

sealed trait SyncStatus { def value: String }

object SyncStatus {
  case object Pending extends SyncStatus { val value = "pending" }
  case object Backfilling extends SyncStatus { val value = "backfilling" }
  case object Progressing extends SyncStatus { val value = "progressing" }
  case object Ready extends SyncStatus { val value = "ready" }
  case object Failed extends SyncStatus { val value = "failed" }

  implicit val decoder: Decoder[SyncStatus] = Decoder.decodeString.emap {
    case "pending" => Right(Pending)
    case "backfilling" => Right(Backfilling)
    case "progressing" => Right(Progressing)
    case "ready" => Right(Ready)
    case "failed" => Right(Failed)
    case other => Left(s"Unknown SyncStatus: $other")
  }
}

sealed trait PollResponse { def status: SyncStatus }

object PollResponse {
  case object Pending extends PollResponse { val status: SyncStatus = SyncStatus.Pending }
  case object Backfilling extends PollResponse { val status: SyncStatus = SyncStatus.Backfilling }
  case object Progressing extends PollResponse { val status: SyncStatus = SyncStatus.Progressing }
  case object Failed extends PollResponse { val status: SyncStatus = SyncStatus.Failed }
  final case class Ready(readyId: UUID) extends PollResponse {
    val status: SyncStatus = SyncStatus.Ready
  }

  implicit val decoder: Decoder[PollResponse] = Decoder.instance { cursor =>
    cursor.downField("status").as[SyncStatus].flatMap {
      case SyncStatus.Pending => Right(Pending)
      case SyncStatus.Backfilling => Right(Backfilling)
      case SyncStatus.Progressing => Right(Progressing)
      case SyncStatus.Failed => Right(Failed)
      case SyncStatus.Ready => cursor.downField("readyId").as[UUID].map(Ready)
      case other =>
        Left(io.circe.DecodingFailure(s"Unknown PollResponse status: $other", cursor.history))
    }
  }
}

final case class ReadCursorsResponse(readCursors: Seq[String])

object ReadCursorsResponse {
  implicit val decoder: Decoder[ReadCursorsResponse] =
    Decoder.forProduct1("readCursors")(ReadCursorsResponse.apply)
}

final case class ReadRequest(readCursor: String)

object ReadRequest {
  implicit val encoder: Encoder[ReadRequest] = Encoder.forProduct1("readCursor")(_.readCursor)
}

final case class ReadResponse(
    items: Seq[InstanceDefinition],
    nextReadCursor: Option[String]
)

object ReadResponse {
  // Read responses carry no typing of their own; instance properties are decoded using the typing from begin.
  def decoder(
      typing: Option[Map[String, Map[String, Map[String, TypePropertyDefinition]]]]
  ): Decoder[ReadResponse] = {
    implicit val instanceDecoder: Decoder[InstanceDefinition] =
      InstanceDefinition.instancePropertyDefinitionBasedInstanceDecoder(typing)
    Decoder.forProduct2("items", "nextReadCursor")(ReadResponse.apply)
  }
}
