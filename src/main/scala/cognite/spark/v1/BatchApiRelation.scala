package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceDefinition, InstanceType, TypePropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import fs2.{Chunk, Stream}
import io.circe.{Decoder, Encoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import sttp.client3._
import sttp.client3.circe._

import java.util.UUID

/**
  * Read-only relation over the batch API. Each read cursor is paged to exhaustion through
  * `/models/instances/swap/read`, one stream per cursor.
  *
  * @param config common relation configs
  * @param readCursors cursors obtained from the batch API read-cursors endpoint
  * @param typing property typing returned when the batch was begun; needed to decode instance properties
  * @param sqlContext sql context
  */
class BatchApiRelation(
    config: RelationConfig,
    readCursors: Seq[String],
    typing: Option[Map[String, Map[String, Map[String, TypePropertyDefinition]]]]
)(val sqlContext: SQLContext)
    extends CdfRelation(config, BatchApiRelation.name)
    with PrunedFilteredScan {

  override def schema: StructType = BatchApiRelation.schema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[InstanceDefinition, (String, String)](
      sqlContext.sparkContext,
      config,
      (instance: InstanceDefinition, _) => toRow(instance, requiredColumns),
      instance => (instance.space, instance.externalId),
      getStreams,
      // the batch API hands out disjoint cursors, so there is nothing to deduplicate
      deduplicateRows = false
    )

  private def getStreams(client: GenericClient[IO]): Seq[Stream[IO, InstanceDefinition]] =
    readCursors.map(readAll(client, _))

  private def readAll(client: GenericClient[IO], readCursor: String): Stream[IO, InstanceDefinition] =
    Stream.unfoldChunkEval[IO, Option[String], InstanceDefinition](Some(readCursor)) {
      case None => IO.pure(None)
      case Some(cursor) =>
        readPage(client, cursor).map(page => Some((Chunk.from(page.items), page.nextReadCursor)))
    }

  private def readPage(client: GenericClient[IO], readCursor: String): IO[ReadResponse] = {
    implicit val readResponseDecoder: Decoder[ReadResponse] = ReadResponse.decoder(typing)
    val requestSession = client.requestSession.withResourceType(GenericClient.DATAMODELS)
    requestSession.post[ReadResponse, ReadResponse, ReadRequest](
      ReadRequest(readCursor),
      uri"${requestSession.baseUrl}/models/swamp/read",
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
  override val name: String = "batchapi"

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

final case class BeginRequest(
    includeTyping: Boolean,
    sources: Seq[SourceSelectorWithoutTargetUnits],
    instanceType: InstanceType,
    filter: Option[FilterDefinition] = None,
)

final case class SourceSelectorWithoutTargetUnits(
    source: ViewReference,
)

final case class BeginResponse(
    typing: Option[Map[String, Map[String, Map[String, TypePropertyDefinition]]]] = None,
    statusId: UUID
)

sealed trait SyncStatus { def value: String }

object SyncStatus {
  case object Pending extends SyncStatus { val value = "pending" }
  case object Backfilling extends SyncStatus { val value = "backfilling" }
  case object Progressing extends SyncStatus { val value = "progressing" }
  case object Ready extends SyncStatus { val value = "ready" }
  case object Failed extends SyncStatus { val value = "failed" }

  implicit val encoder: Encoder[SyncStatus] = Encoder.encodeString.contramap(_.value)
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

final case class ReadCursorsRequest(readyId: UUID)
final case class ReadCursorsResponse(readCursors: Seq[String])

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
