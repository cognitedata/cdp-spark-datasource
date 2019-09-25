package cognite.spark

case class Chunk[A, B](chunk: Seq[A], cursor: Option[B])

object Batch {
  def chunksWithCursor[A, B](chunkSize: Int, limit: Option[Int], initialCursor: Option[B] = None)(
      processChunk: (Int, Option[B]) => (Seq[A], Option[B])): Iterator[Chunk[A, B]] = {
    // We want to do a do / while, but lack construct for that in Iterator
    // That's why lastChunk has to be cached and merged in the end
    var lastChunk: Iterator[Chunk[A, B]] = Iterator.empty
    var lastCursor: Option[B] = None
    Iterator
      .iterate((limit, true, Seq.empty[A], initialCursor)) {
        case (nRowsRemaining, _, _, cursor) =>
          val thisBatchSize = scala.math.min(nRowsRemaining.getOrElse(chunkSize), chunkSize)
          val (chunk, newCursor) = processChunk(thisBatchSize, cursor)
          val rowsRemaining = nRowsRemaining.map(_ - chunk.length)
          val continue = chunk.nonEmpty && rowsRemaining.forall(_ > 0) && newCursor.isDefined
          lastChunk = Iterator.single(Chunk(chunk, cursor))
          lastCursor = cursor
          (rowsRemaining, continue, chunk, newCursor)
      }
      .takeWhile(_._2)
      .drop(1) // Drop the initial item, which is only used to start the do / while loop
      .map(chunk => Chunk(chunk._3, lastCursor)) ++ lastChunk
  }

  def withCursor[A, B](chunkSize: Int, limit: Option[Int], initialCursor: Option[B] = None)(
      processChunk: (Int, Option[B]) => (Seq[A], Option[B])): Iterator[A] = {
    val items = chunksWithCursor(chunkSize, limit, initialCursor)(processChunk)
      .flatMap(_.chunk)
    limit match {
      case Some(value) => items.take(value)
      case None => items
    }
  }
}
