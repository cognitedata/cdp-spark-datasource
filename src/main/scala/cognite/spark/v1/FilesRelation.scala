package cognite.spark.v1

import cats.effect.IO
import com.cognite.sdk.scala.v1.{File, GenericClient}
import cognite.spark.v1.SparkSchemaHelper._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import cats.implicits._
import fs2.Stream

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[File, Long](config, "files")
    with InsertableRelation {

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val files = rows.map { r =>
      val file = fromRow[File](r)
      file.copy(metadata = filterMetadata(file.metadata))
    }
    client.files.updateFromRead(files) *> IO.unit
  }

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, File]] =
    Seq(client.files.list(limit))

  override def schema: StructType = structType[File]

  override def toRow(t: File): Row = asRow(t)

  override def uniqueId(a: File): Long = a.id
}
