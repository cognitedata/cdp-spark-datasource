package cognite.spark.v1

import java.time.Instant

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.concurrent.ExecutionContext
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.common.Auth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

abstract class Limit extends Ordered[Limit] with Serializable {
  def value: Instant

  override def compare(that: Limit): Int = this.value.compareTo(that.value)
}

sealed case class Min(value: Instant) extends Limit

sealed case class Max(value: Instant) extends Limit

case class AggregationFilter(aggregation: String)

abstract class DataPointsRelationV1[A](config: RelationConfig)(override val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedFilteredScan
    with Serializable
    with InsertableRelation {
  @transient implicit lazy val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client = new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)
  def toRow(a: A): Row

  def toRow(requiredColumns: Array[String])(item: A): Row

  override def schema: StructType

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    data.foreachPartition((rows: Iterator[Row]) => {
      val batches = rows.grouped(Constants.DefaultBatchSize).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup.parTraverse(insertSeqOfRows).unsafeRunSync()
      }
    })

  def insertSeqOfRows(rows: Seq[Row]): IO[Unit]

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  def getAggregationSettings(filters: Array[Filter]): (Array[AggregationFilter], Seq[String]) = {
    val aggregations = filters.flatMap(getAggregation).distinct
    val granularities = filters.flatMap(getGranularity).distinct

    if (aggregations.nonEmpty && granularities.isEmpty) {
      sys.error(s"Aggregations requested but granularity is not specified")
    }

    if (aggregations.isEmpty && granularities.nonEmpty) {
      sys.error(s"Granularity specified but no aggregation requested")
    }

    (aggregations, granularities)
  }

  // scalastyle:off cyclomatic.complexity
  def toAggregationFilter(aggregation: String): AggregationFilter = {
    val allowedAggregations = Seq(
      "average",
      "max",
      "min",
      "count",
      "sum",
      "interpolation",
      "stepinterpolation",
      "totalvariation",
      "continuousvariance",
      "discretevariance")
    aggregation match {
      case agg: String if allowedAggregations.contains(agg) => AggregationFilter(agg)
      case _ => sys.error(s"Invalid aggregation $aggregation")
    }
  }

  def getAggregation(filter: Filter): Seq[AggregationFilter] =
    filter match {
      case IsNotNull("aggregation") => Seq()
      case EqualTo("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case EqualNullSafe("aggregation", value) => Seq(toAggregationFilter(value.toString))
      case In("aggregation", values) =>
        values.map(v => toAggregationFilter(v.toString))
      case And(_, _) => sys.error("AND is not allowed for aggregations")
      case Or(f1, f2) => getAggregation(f1) ++ getAggregation(f2)
      case StringStartsWith("aggregation", value) =>
        sys.error(
          s"Choosing aggregation using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("aggregation", value) =>
        sys.error(
          s"Choosing aggregation using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("aggregation", value) =>
        sys.error(
          s"Choosing aggregation using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
    }

  def toGranularityFilter(granularity: String): Seq[String] =
    granularity.split('|')

  def getGranularity(filter: Filter): Seq[String] =
    filter match {
      case IsNotNull("granularity") => Seq()
      case EqualTo("granularity", value) => toGranularityFilter(value.toString)
      case EqualNullSafe("granularity", value) => toGranularityFilter(value.toString)
      case In("granularity", values) =>
        values.flatMap(v => toGranularityFilter(v.toString))
      case And(_, _) => sys.error("AND is not allowed for granularity")
      case Or(f1, f2) => getGranularity(f1) ++ getGranularity(f2)
      case StringStartsWith("granularity", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringEndsWith("granularity", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case StringContains("granularity", value) =>
        sys.error(
          s"Choosing granularity using 'string starts with' not allowed for data points, attempted for ${value.toString}")
      case _ => Seq()
    }
}
