package cognite.spark.v1

import java.time.Instant
import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.StringDataPoint
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteId, CogniteInternalId, GenericClient}
import PushdownUtilities.{
  filtersToTimestampLimits,
  getIdFromMap,
  pushdownToParameters,
  toPushdownFilterExpression
}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import cognite.spark.v1.SparkSchemaHelper.structType
import fs2.Stream

final case class StringDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: String
)

final case class StringDataPointsInsertItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: String
) extends RowWithCogniteId("inserting string data points")

class StringDataPointsRelationV1(config: RelationConfig)(override val sqlContext: SQLContext)
    extends DataPointsRelationV1[StringDataPointsItem](config, "stringdatapoints")(sqlContext)
    with WritableRelation {
  import CdpConnector._
  override def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert not supported for stringdatapoints. Please use upsert instead.")

  override def upsert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Use insert(DataFrame) instead")

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update not supported for stringdatapoints. Please use upsert instead.")

  def toRow(a: StringDataPointsItem): Row = asRow(a)

  override def schema: StructType =
    StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

  def insertRowIterator(rows: Iterator[Row]): IO[Unit] = {
    // we basically use Stream.fromIterator instead of Seq.grouped, because it's significantly more efficient
    val dataPoints = Stream.fromIterator(
      rows.map(r => fromRow[StringDataPointsInsertItem](r)),
      chunkSize = Constants.CreateDataPointsLimit
    )

    dataPoints.chunks
      .flatMap(c => Stream.emits(c.toVector.groupBy(_.getCogniteId).toVector))
      .parEvalMapUnordered(config.parallelismPerPartition) {
        case (id, dataPoints) =>
          client.dataPoints
            .insertStrings(id, dataPoints.map(dp => StringDataPoint(dp.timestamp, dp.value)))
            .flatTap(_ => incMetrics(itemsCreated, dataPoints.size))
      }
      .compile
      .drain
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    StringDataPointsRdd(
      sqlContext.sparkContext,
      config,
      getIOs(filters),
      (item: StringDataPointsItem) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        toRow(requiredColumns)(item)
      })

  def getIOs(filters: Array[Filter])(
      client: GenericClient[IO]): Seq[(CogniteId, IO[Seq[StringDataPoint]])] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val ids = filtersAsMaps.flatMap(getIdFromMap).distinct

    // Notify users that they need to supply one or more ids/externalIds when reading data points
    if (ids.isEmpty) {
      throw new CdfSparkIllegalArgumentException(
        "Please filter by one or more ids or externalIds when reading string data points.")
    }

    val (lowerTimeLimit, upperTimeLimit) = filtersToTimestampLimits(filters, "timestamp")

    ids.zip(ids.map { id =>
      DataPointsRelationV1.getAllDataPoints[StringDataPoint](
        queryStrings,
        config.batchSize.getOrElse(Constants.DefaultDataPointsLimit),
        id,
        lowerTimeLimit,
        upperTimeLimit.plusMillis(1),
        config.limitPerPartition)
    })
  }

  private def queryStrings(id: CogniteId, lowerLimit: Instant, upperLimit: Instant, limit: Int) =
    client.dataPoints
      .queryStrings(Seq(id), lowerLimit, upperLimit, Some(limit), ignoreUnknownIds = true)
      .map { response =>
        val dataPoints = response.headOption
          .map { ts =>
            WrongDatapointTypeException.check(ts.isString, ts.id, ts.externalId, shouldBeString = true)
            ts.datapoints
          }
          .getOrElse(Seq.empty)
        val lastTimestamp = dataPoints.lastOption.map(_.timestamp)
        (lastTimestamp, dataPoints)
      }

  override def toRow(requiredColumns: Array[String])(item: StringDataPointsItem): Row = {
    val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
    val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf(f))
    val rowOfAllFields = toRow(item)
    Row.fromSeq(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
  }
}

object StringDataPointsRelation extends UpsertSchema {
  // We should use StringDataPointsItem here, but doing that gives the error: "constructor Timestamp encapsulates
  // multiple overloaded alternatives and cannot be treated as a method. Consider invoking
  // `<offending symbol>.asTerm.alternatives` and manually picking the required method" in StructTypeEncoder, probably
  // because TimeStamp has multiple constructors. Issue in backlog for investigating this.
  val upsertSchema = structType[StringDataPointsInsertItem]
  val readSchema = structType[StringDataPointsItem]
  val insertSchema = structType[StringDataPointsInsertItem]
  val deleteSchema = structType[DeleteDataPointsItem]
}
