package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.{File, GenericClient}
import com.cognite.spark.datasource.SparkSchemaHelper._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import cats.implicits._

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[File](config, "files")
    with InsertableRelation {

  override def getFromRowAndCreate(rows: Seq[Row]): IO[Unit] = {
    val files = rows.map { r =>
      val file = fromRow[File](r)
      file.copy(metadata = filterMetadata(file.metadata))
    }
    client.files.updateFromRead(files) *> IO.unit
  }

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Long],
      numPartitions: Int): Seq[fs2.Stream[IO, File]] =
    Seq(config.limit.map(client.files.listWithLimit(_)).getOrElse(client.files.list))

  override def schema: StructType = structType[File]

  override def toRow(t: File): Row = asRow(t)
}
