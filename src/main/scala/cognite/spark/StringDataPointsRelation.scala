package cognite.spark

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.StringDataPoint
import com.cognite.sdk.scala.v1.GenericClient
import PushdownUtilities.{pushdownToParameters, toPushdownFilterExpression}
import cognite.spark.SparkSchemaHelper.{asRow, fromRow}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

case class StringDataPointsItem(
    id: Option[Long],
    externalId: Option[String],
    timestamp: java.sql.Timestamp,
    value: String
)

case class StringDataPointsFilter(
    id: Option[Long],
    externalId: Option[String]
)

class StringDataPointsRelationV1(config: RelationConfig)(override val sqlContext: SQLContext)
    extends DataPointsRelationV1[StringDataPointsItem](config)(sqlContext) {

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
      rows.map(r => fromRow[StringDataPointsItem](r)).partition(p => p.id.isDefined)
    IO {
      dataPointsWithId.groupBy(_.id).map {
        case (id, datapoint) =>
          client.dataPoints
            .insertStringsById(
              id.get,
              datapoint.map(dp => StringDataPoint(dp.timestamp.toInstant, dp.value)))
            .unsafeRunSync()
      }
    } *>
      IO {
        dataPointsWithExternalId.groupBy(_.externalId).map {
          case (extId, datapoint) =>
            client.dataPoints
              .insertStringsByExternalId(
                extId.get,
                datapoint.map(dp => StringDataPoint(dp.timestamp.toInstant, dp.value)))
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

    val (lowerTimeLimit, upperTimeLimit) = filtersToTimestampLimits(filters)

    val itemsIOFromId = ids.map { id =>
      (
        StringDataPointsFilter(Some(id), None),
        client.dataPoints
          .queryStringsById(id, lowerTimeLimit, upperTimeLimit, config.limit))
    }

    val itemsIOFromExternalId = externalIds.map { extId =>
      (
        StringDataPointsFilter(None, Some(extId)),
        client.dataPoints
          .queryStringsByExternalId(extId, lowerTimeLimit, upperTimeLimit, config.limit))

    }

    itemsIOFromId ++ itemsIOFromExternalId
  }

  override def toRow(requiredColumns: Array[String])(item: StringDataPointsItem): Row = {
    val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
    val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf[String](f))
    val rowOfAllFields = toRow(item)
    Row.fromSeq(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
  }
}
