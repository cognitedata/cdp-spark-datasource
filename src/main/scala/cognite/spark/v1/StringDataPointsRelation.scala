package cognite.spark.v1

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.StringDataPoint
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteId, CogniteInternalId, GenericClient}
import PushdownUtilities.{pushdownToParameters, toPushdownFilterExpression}
import cognite.spark.v1.SparkSchemaHelper.{asRow, fromRow}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import PushdownUtilities.filtersToTimestampLimits
import cognite.spark.v1.SparkSchemaHelper.structType

case class StringDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: java.sql.Timestamp,
    value: String
)

case class StringDataPointsInsertItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: Instant,
    value: String
)

case class StringDataPointsFilter(
    id: Option[Long],
    externalId: Option[String]
)

class StringDataPointsRelationV1(config: RelationConfig)(override val sqlContext: SQLContext)
    extends DataPointsRelationV1[StringDataPointsItem](config)(sqlContext)
    with WritableRelation {

  override def insert(rows: Seq[Row]): IO[Unit] =
    throw new RuntimeException("Insert not supported for stringdatapoints. Please use upsert instead.")

  override def upsert(rows: Seq[Row]): IO[Unit] = insertSeqOfRows(rows)

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new RuntimeException("Update not supported for stringdatapoints. Please use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] =
    throw new RuntimeException("Delete not supported for stringdatapoints.")

  def toRow(a: StringDataPointsItem): Row = asRow(a)

  override def schema: StructType =
    StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

  override def insertSeqOfRows(rows: Seq[Row]): IO[Unit] = {
    val (dataPointsWithId, dataPointsWithExternalId) =
      rows.map(r => fromRow[StringDataPointsInsertItem](r)).partition(p => p.id.isDefined)
    IO {
      dataPointsWithId.groupBy(_.id).map {
        case (id, datapoint) =>
          client.dataPoints
            .insertStringsById(id.get, datapoint.map(dp => StringDataPoint(dp.timestamp, dp.value)))
            .unsafeRunSync()
      }
    } *>
      IO {
        dataPointsWithExternalId.groupBy(_.externalId).map {
          case (extId, datapoint) =>
            client.dataPoints
              .insertStringsByExternalId(
                extId.get,
                datapoint.map(dp => StringDataPoint(dp.timestamp, dp.value)))
              .unsafeRunSync()
        }
      }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    StringDataPointsRdd(sqlContext.sparkContext, config, getIOs(filters), toRow(requiredColumns))

  def getIOs(filters: Array[Filter])(
      client: GenericClient[IO, Nothing]): Seq[(StringDataPointsFilter, IO[Seq[StringDataPoint]])] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToParameters(pushdownFilterExpression)

    val ids = filtersAsMaps.flatMap(m => m.get("id")).map(_.toLong).distinct
    val externalIds = filtersAsMaps.flatMap(m => m.get("externalId")).distinct

    val (lowerTimeLimit, upperTimeLimit) = filtersToTimestampLimits(filters, "timestamp")

    val itemsIOFromId = ids.map { id =>
      (
        StringDataPointsFilter(Some(id), None),
        DataPointsRelationV1.getAllDataPoints[StringDataPoint](
          queryStrings,
          config.batchSize,
          CogniteInternalId(id),
          lowerTimeLimit,
          upperTimeLimit.plusMillis(1),
          config.limitPerPartition)
      )
    }

    val itemsIOFromExternalId = externalIds.map { extId =>
      (
        StringDataPointsFilter(None, Some(extId)),
        DataPointsRelationV1.getAllDataPoints[StringDataPoint](
          queryStrings,
          config.batchSize,
          CogniteExternalId(extId),
          lowerTimeLimit,
          upperTimeLimit.plusMillis(1),
          config.limitPerPartition)
      )
    }

    itemsIOFromId ++ itemsIOFromExternalId
  }

  private def queryStrings(
      id: CogniteId,
      lowerLimit: Instant,
      upperLimit: Instant,
      nPointsRemaining: Option[Int]) = {
    val responses = id match {
      case CogniteInternalId(internalId) =>
        client.dataPoints.queryStringsById(
          internalId,
          lowerLimit,
          upperLimit,
          DataPointsRelationV1.limitForCall(nPointsRemaining, config.batchSize))
      case CogniteExternalId(externalId) =>
        client.dataPoints.queryStringsByExternalId(
          externalId,
          lowerLimit,
          upperLimit,
          DataPointsRelationV1.limitForCall(nPointsRemaining, config.batchSize))
    }
    responses.map { queryResponses =>
      val dataPoints = queryResponses.flatMap(_.datapoints)
      val lastTimestamp = dataPoints.lastOption.map(_.timestamp)
      (lastTimestamp, dataPoints)
    }
  }

  override def toRow(requiredColumns: Array[String])(item: StringDataPointsItem): Row = {
    val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
    val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf[String](f))
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
}
