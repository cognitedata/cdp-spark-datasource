package cognite.spark.v1

import cats.effect.IO
import fs2.Stream

object RelationHelper {
  def getFromIdOrExternalId[T](
      idOrExternalId: String,
      filtersAsMaps: Seq[Map[String, String]],
      clientToGetByIdOrByExternalId: String => IO[T]): Seq[Stream[IO, T]] =
    filtersAsMaps
      .flatMap(_.get(idOrExternalId))
      .distinct
      .map { id =>
        clientToGetByIdOrByExternalId(id)
      }
      .map(fs2.Stream.eval)

}
