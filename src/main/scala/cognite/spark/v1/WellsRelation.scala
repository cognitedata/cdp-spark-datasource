package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.SparkSchemaHelper._
import cognite.spark.v1.wdl.{AssetSource, WellIngestionInsertSchema, Wellheads, WellsReadSchema}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.dsl.TransformerOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.util.UUID
import scala.annotation.nowarn

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
            .withFieldComputed(_.matchingId, u => u.matchingId.getOrElse(UUID.randomUUID().toString))
            .withFieldComputed(_.wellhead, u => u.wellhead.getOrElse(Wellheads(0.0, 0.0, "CRS")))
            .withFieldComputed(_.sources, u => Seq(u.source))
            .transform)

        wells = wells ++ newWells
      }
  }

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def delete(rows: Seq[Row]): IO[Unit] = IO {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[AssetSource](r))
    wells = wells.filter(w =>
      w.sources.forall(s => !deletes.contains(s))
    )
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  private def toRow(a: WellsReadSchema): Row = asRow(a)

  private def uniqueId(a: WellsReadSchema): String = a.matchingId

  override def buildScan(): RDD[Row] =
    SdkV1Rdd[WellsReadSchema, String](
      sqlContext.sparkContext,
      config,
      (a: WellsReadSchema, _) => toRow(a),
      uniqueId,
      getStreams()
    )


  private def getStreams()(
    @nowarn client: GenericClient[IO],
    @nowarn limit: Option[Int],
    @nowarn numPartitions: Int): Seq[Stream[IO, WellsReadSchema]] = {
      Seq(
        Stream[IO, WellsReadSchema]()
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
