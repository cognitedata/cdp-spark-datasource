package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.cognite.data.api.v2.DataPoints._
import com.softwaremill.sttp._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.concurrent.ExecutionContext

class StringDataPointsRelation(
    config: RelationConfig,
    numPartitions: Int,
    suppliedSchema: Option[StructType])(override val sqlContext: SQLContext)
    extends DataPointsRelation(config, numPartitions, suppliedSchema)(sqlContext) {
  @transient override val datapointsCreated =
    metricsSource.getOrCreateCounter(s"stringdatapoints.created")
  @transient override val datapointsRead =
    metricsSource.getOrCreateCounter(s"stringdatapoints.read")

  private val batchSize = config.batchSize.getOrElse(Constants.DefaultDataPointsBatchSize)
  override def schema: StructType =
    suppliedSchema.getOrElse(
      StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("timestamp", LongType, nullable = false),
          StructField("value", StringType, nullable = false)
        )))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (timestampLowerLimit, timestampUpperLimit) = getTimestampLimits(filters)
    val names = filters.flatMap(getNameFilters).map(_.name).distinct
    val rdds = names.map { name =>
      val maxTimestamp = timestampUpperLimit match {
        case Some(i) => i + 1
        case None =>
          getLatestDataPoint(name)
            .map(_.timestamp + 1)
            .getOrElse(System.currentTimeMillis())
      }
      new StringDataPointsRdd(
        sqlContext.sparkContext,
        toRow(name, requiredColumns),
        numPartitions,
        timestampLowerLimit.map(lowerLimit => scala.math.max(lowerLimit - 1, 0)).getOrElse(0),
        maxTimestamp,
        uri"${baseDataPointsUrl(config.project)}/$name",
        config.copy(batchSize = Some(batchSize))
      )
    }
    rdds.foldLeft(sqlContext.sparkContext.emptyRDD[Row])((a, b) => a.union(b))
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit =
    df.foreachPartition(rows => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches =
        rows.grouped(config.batchSize.getOrElse(Constants.DefaultDataPointsBatchSize)).toVector
      batches
        .parTraverse(batch => {
          val timeSeriesData = MultiNamedTimeseriesData()
          val namedTimeseriesData = batch
            .groupBy(r => r.getAs[String](0))
            .map {
              case (name, timeseriesRows) =>
                val d = timeseriesRows.foldLeft(StringTimeseriesData())((builder, row) =>
                  builder.addPoints(StringDatapoint(row.getLong(1), row.getString(2))))
                NamedTimeseriesData(name, NamedTimeseriesData.Data.StringData(d))
            }
          postTimeSeries(timeSeriesData.addAllNamedTimeseriesData(namedTimeseriesData))
        })
        .unsafeRunSync
    })

  private val requiredColumnToIndex =
    Map("name" -> 0, "timestamp" -> 1, "value" -> 2)
  private def toColumns(
      name: String,
      requiredColumns: Array[String],
      dataPoint: StringDatapoint): Seq[Option[Any]] = {
    val requiredColumnIndexes = requiredColumns.map(requiredColumnToIndex)
    for (index <- requiredColumnIndexes)
      yield
        index match {
          case 0 => Some(name)
          case 1 => Some(dataPoint.timestamp)
          case 2 => Some(dataPoint.value)
          case _ =>
            sys.error("Invalid required column index " + index.toString)
            None
        }
  }

  private def toRow(name: String, requiredColumns: Array[String])(
      dataPoint: StringDatapoint): Row = {
    if (config.collectMetrics) {
      datapointsRead.inc()
    }
    Row.fromSeq(toColumns(name, requiredColumns, dataPoint))
  }

}
