package cognite.spark.v1

import cats.effect.IO
import fs2.Stream

object RelationHelper {
  def getFromIdsOrExternalIds[T](
      ids: Seq[String],
      clientToGetByIdsOrByExternalIds: Seq[String] => IO[Seq[T]]): Stream[IO, T] =
    if (ids.isEmpty) {
      fs2.Stream.emits(Seq[T]())
    } else {
      fs2.Stream.evalSeq(clientToGetByIdsOrByExternalIds(ids))
    }

}
