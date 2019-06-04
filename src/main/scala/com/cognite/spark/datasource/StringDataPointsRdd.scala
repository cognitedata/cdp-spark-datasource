package com.cognite.spark.datasource

import cats.implicits._
import com.cognite.data.api.v2.DataPoints.{StringDatapoint, TimeseriesData}
import com.softwaremill.sttp.{Response, Uri}
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.sql.Row

class StringDataPointsRdd(
    @transient override val sparkContext: SparkContext,
    timestampLimits: Map[String, (Long, Long)],
    toRow: (String, StringDatapoint) => Row,
    numPartitions: Int,
    getSinglePartitionBaseUri: Uri,
    config: RelationConfig)
    extends DataPointsRdd(sparkContext, getSinglePartitionBaseUri, config) {

  override def getDataPointRows(name: String, uri: Uri, start: Long): (Seq[Row], Option[Long]) = {
    val dataPoints =
      getProtobuf[Seq[StringDatapoint]](
        config,
        uri,
        parseResult
      ).unsafeRunSync()
    if (dataPoints.lastOption.fold(true)(_.timestamp < start)) {
      (Seq.empty, None)
    } else {
      (dataPoints.map(toRow(name, _)), dataPoints.lastOption.map(_.timestamp + 1))
    }
  }

  def parseResult(response: Response[Array[Byte]]): Response[Seq[StringDatapoint]] = {
    val r = Either.catchNonFatal {
      val timeSeriesData = TimeseriesData.parseFrom(response.unsafeBody)
      if (timeSeriesData.data.isStringData) {
        timeSeriesData.getStringData.points
      } else {
        Seq.empty
      }
    }
    val rr = r.left.map(throwable => throwable.getMessage.getBytes)
    Response(rr, response.code, response.statusText, response.headers, response.history)
  }

  override def getPartitions: Array[Partition] =
    timestampLimits
      .flatMap {
        case (name, (lowerLimit, upperLimit)) =>
          DataPointsRdd
            .intervalPartitions(
              name,
              scala.math.max(lowerLimit - 1, 0),
              upperLimit + 1,
              1,
              numPartitions
            )
      }
      .zipWithIndex
      .map { case (p, idx) => p.copy(index = idx) }
      .toArray
}
