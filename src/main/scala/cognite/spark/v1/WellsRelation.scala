package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper._
import cognite.spark.v1.wdl.{WellIngestionInsertSchema, WellsReadSchema}
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.dsl._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

// Simplest relation based on TableScan
class WellsRelation(
    config: RelationConfig)(override val sqlContext: SQLContext)
  extends CdfRelation(config, "wdl")
    with WritableRelation
    with TableScan {

  private var wells = Vector[WellsReadSchema]()

  override def schema: StructType = structType[WellsReadSchema]()

  override def upsert(rows: Seq[Row]): IO[Unit] = IO {
    if (rows.nonEmpty) {
        val insertions = rows.map(fromRow[WellIngestionInsertSchema](_))

        val newWells = insertions.map(
          _.into[WellsReadSchema]
            .withFieldComputed(_.name, u => u.name)
            .transform)
      }
  }

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def delete(rows: Seq[Row]): IO[Unit] = IO {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceDeleteSchema](r))
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  private def toRow(a: WellsReadSchema): Row = asRow(a)

  private def uniqueId(a: WellsReadSchema): String = a.matchingId

  override def getStreams(filters: Array[Filter])(
    client: GenericClient[IO],
    limit: Option[Int],
    numPartitions: Int): Seq[Stream[IO, WellsReadSchema]] = {
      Seq(
        wells
          .toStream
          .map(r =>(IO.unit, r))
      )
  }

  def getOptionalIndex(
    rSchema: StructType,
    keyString: String): Option[Int] = {
      val index = rSchema.fieldNames.indexOf(keyString)
      if (index < 0) {
        None
      } else {
        Some(index)
      }
  }

  def getRequiredIndex(
    rSchema: StructType,
    keyString: String): Int = {
      getOptionalIndex(rSchema, keyString).getOrElse(
        throw new CdfSparkException(s"Can't row convert to Well, `$keyString` is missing.")
      )
  }

}
