package cognite.spark.v1

import org.apache.spark.sql.Row

trait WritableRelation {
  def insert(rows: Seq[Row]): TracedIO[Unit]

  def upsert(rows: Seq[Row]): TracedIO[Unit]

  def update(rows: Seq[Row]): TracedIO[Unit]

  def delete(rows: Seq[Row]): TracedIO[Unit]
}
