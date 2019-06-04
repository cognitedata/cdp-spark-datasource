package com.cognite.spark.datasource

import com.softwaremill.sttp.Uri
import io.circe.generic.auto._
import io.circe.Decoder

case class NextCursorIterator[A: Decoder](
    url: Uri,
    config: RelationConfig
) extends Iterator[(Option[String], Option[Int])]
    with CdpConnector {
  private val batchSize = config.batchSize.getOrElse(Constants.DefaultBatchSize)

  private var nItemsRead = 0
  private var nextCursor = Option.empty[String]
  private var isFirst = true

  override def hasNext: Boolean =
    isFirst || nextCursor.isDefined && config.limit.fold(true)(_ > nItemsRead)

  override def next(): (Option[String], Option[Int]) = {
    isFirst = false
    val next = nextCursor
    val thisBatchSize = math.min(
      batchSize,
      config.limit
        .map(_ - nItemsRead)
        .getOrElse(batchSize))
    val urlWithLimit = url.param("limit", thisBatchSize.toString)
    val getUrl = nextCursor.fold(urlWithLimit)(urlWithLimit.param("cursor", _))
    val dataWithCursor =
      getJson[CdpConnector.DataItemsWithCursor[A]](
        config,
        getUrl
      ).unsafeRunSync().data
    nextCursor = dataWithCursor.nextCursor
    nItemsRead += thisBatchSize
    (next, nextCursor.map(_ => thisBatchSize))
  }
}
