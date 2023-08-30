package cognite.spark.v1

import cats.effect.IO
import org.apache.spark.sql.Row

trait WritableRelation {
  def insert(rows: Seq[Row]): IO[Unit]

  def upsert(rows: Seq[Row]): IO[Unit]

  def update(rows: Seq[Row]): IO[Unit]

  def delete(rows: Seq[Row]): IO[Unit]
}
