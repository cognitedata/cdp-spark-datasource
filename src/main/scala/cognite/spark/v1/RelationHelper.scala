package cognite.spark.v1

import cats.effect.IO
import fs2.Stream

object RelationHelper {
  def getFromIdOrExternalId[T](
      ids: Seq[String],
      clientToGetByIdOrByExternalId: String => IO[T]): Seq[Stream[IO, T]] =
    ids.map(clientToGetByIdOrByExternalId).map(fs2.Stream.eval)
}
