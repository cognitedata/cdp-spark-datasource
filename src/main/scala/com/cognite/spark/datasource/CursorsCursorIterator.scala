package com.cognite.spark.datasource

import com.softwaremill.sttp.Uri

import scala.collection.mutable

case class CursorsCursorIterator(url: Uri, config: RelationConfig)
    extends Iterator[(Option[String], Option[Int])]
    with CdpConnector {
  private val cursors = {
    val cursors =
      get[String](config.copy(limit = Some(100)), url).toSeq
    mutable.Queue(cursors: _*)
  }

  override def hasNext: Boolean = cursors.nonEmpty

  override def next(): (Option[String], Option[Int]) = (Some(cursors.dequeue()), None)
}
