package com.cognite.spark.connector

object Batch {
  def withCursor[A,B](chunkSize: Int, limit: Option[Int])(processChunk: (Int, Option[B]) => (Seq[A], Option[B])): Iterator[A] = {
    // We want to do a do / while, but lack construct for that in Iterator
    // That's why lastChunk has to be cached and merged in the end
    var lastChunk = Seq.empty[A]
    Iterator.iterate((limit, true, Seq.empty[A], Option.empty[B])) {
      case (nRowsRemaining, _, _, cursor) =>
          val thisBatchSize = scala.math.min(nRowsRemaining.getOrElse(chunkSize), chunkSize)
          val (chunk, newCursor) = processChunk(thisBatchSize, cursor)
          val rowsRemaining = nRowsRemaining.map(_ - chunk.length)
          val continue = chunk.nonEmpty && rowsRemaining.forall(_ > 0) && newCursor.isDefined
          lastChunk = chunk
          (rowsRemaining, continue, chunk, newCursor)
    }.takeWhile(_._2)
      .flatMap(_._3) ++ lastChunk
  }
}
