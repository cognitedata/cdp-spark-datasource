package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities.{
  getIdFromAndFilter,
  pushdownToSimpleOr,
  toPushdownFilterExpression
}
import cognite.spark.compiletime.macros.SparkSchemaHelper.{asRow, fromRow, structType}
import com.cognite.sdk.scala.common.StringDataPoint
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

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
    extends DataPointsRelationV1[StringDataPointsItem](config, StringDataPointsRelation.name)(sqlContext)
    with WritableRelation {
  override def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert not supported for stringdatapoints. Please use upsert instead.")

  override def upsert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Use insert(DataFrame) instead")

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update not supported for stringdatapoints. Please use upsert instead.")

  override def toRow(a: StringDataPointsItem): Row = asRow(a)

  override def schema: StructType =
    StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

  override def insertRowIterator(rows: Iterator[Row]): IO[Unit] = {
    // we basically use Stream.fromIterator instead of Seq.grouped, because it's significantly more efficient
    val dataPoints = Stream.fromIterator[IO](
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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushdownFilterExpression = toPushdownFilterExpression(filters)
    val filtersAsMaps = pushdownToSimpleOr(pushdownFilterExpression).filters
    val ids = filtersAsMaps.flatMap(getIdFromAndFilter).distinct

    // Notify users that they need to supply one or more ids/externalIds when reading data points
    if (ids.isEmpty) {
      throw new CdfSparkIllegalArgumentException(
        "Please filter by one or more ids or externalIds when reading string data points.")
    }
    StringDataPointsRdd(sqlContext.sparkContext, config, filters, ids, (item: StringDataPointsItem) => {
      if (config.collectMetrics) {
        itemsRead.inc()
      }
      toRow(requiredColumns)(item)
    })
  }

  override def toRow(requiredColumns: Array[String])(item: StringDataPointsItem): Row = {
    val fieldNamesInOrder = item.getClass.getDeclaredFields.map(_.getName)
    val indicesOfRequiredFields = requiredColumns.map(f => fieldNamesInOrder.indexOf(f))
    val rowOfAllFields = toRow(item)
    new GenericRow(indicesOfRequiredFields.map(idx => rowOfAllFields.get(idx)))
  }
}

object StringDataPointsRelation
    extends UpsertSchema
    with ReadSchema
    with DeleteSchema
    with InsertSchema
    with NamedRelation {
  override val name: String = "stringdatapoints"
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  // We should use StringDataPointsItem here, but doing that gives the error: "constructor Timestamp encapsulates
  // multiple overloaded alternatives and cannot be treated as a method. Consider invoking
  // `<offending symbol>.asTerm.alternatives` and manually picking the required method" in StructTypeEncoder, probably
  // because TimeStamp has multiple constructors. Issue in backlog for investigating this.
  override val upsertSchema: StructType = structType[StringDataPointsInsertItem]()
  override val readSchema: StructType = structType[StringDataPointsItem]()
  override val insertSchema: StructType = structType[StringDataPointsInsertItem]()
  override val deleteSchema: StructType = structType[DeleteDataPointsItem]()

}
