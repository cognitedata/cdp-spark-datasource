package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.CdpConnector.ExtensionMethods
import cognite.spark.v1.PushdownUtilities.filtersToTimestampLimits
import com.cognite.sdk.scala.common.StringDataPoint
import com.cognite.sdk.scala.v1._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.time.Instant

final case class StringDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    config: RelationConfig,
    filters: Array[Filter],
    ids: Seq[CogniteId],
    toRow: StringDataPointsItem => Row
) extends RDD[Row](sparkContext, Nil) {
  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  override def getPartitions: Array[Partition] = {
    val numberOfIOs = ids.length
    0.until(numberOfIOs).toArray.map(CdfPartition)
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

  override def compute(_split: Partition, context: TaskContext): Iterator[Row] = {
    val split = _split.asInstanceOf[CdfPartition] // scalafix:ok
    val id = ids(split.index)
    val internalId = id match {
      case CogniteInternalId(id) => Some(id)
      case _ => None
    }
    val externalId = id match {
      case CogniteExternalId(externalId) => Some(externalId)
      case _ => None
    }
    val (lowerTimeLimit, upperTimeLimit) = filtersToTimestampLimits(filters, "timestamp")

    new InterruptibleIterator(
      context,
      DataPointsRelationV1
        .getAllDataPoints[StringDataPoint](
          queryStrings,
          config.batchSize.getOrElse(Constants.DefaultDataPointsLimit),
          id,
          lowerTimeLimit,
          upperTimeLimit.plusMillis(1),
          config.limitPerPartition)
        .stream
        .map { stringDataPoint =>
          toRow(
            StringDataPointsItem(
              internalId,
              externalId,
              stringDataPoint.timestamp,
              stringDataPoint.value
            ))
        }
        .compile
        .to(Seq)
        .unsafeRunBlocking()
        .iterator
    )
  }
}
