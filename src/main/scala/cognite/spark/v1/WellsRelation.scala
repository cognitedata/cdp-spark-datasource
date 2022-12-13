package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.SparkSchemaHelper._
import cognite.spark.v1.wdl.{AssetSource, Well, WellIngestion, Wellhead}
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
  extends CdfRelation(config, "wells")
    with WritableRelation
    with TableScan {

  override def schema: StructType = structType[Well]()

  override def upsert(rows: Seq[Row]): IO[Unit] = IO {
    if (rows.nonEmpty) {
        val insertions = rows.map(fromRow[WellIngestion](_))

        val newWells = insertions.map(
          _.into[Well]
            .withFieldComputed(_.matchingId, u => u.matchingId.getOrElse(UUID.randomUUID().toString))
            .withFieldComputed(_.wellhead, u => u.wellhead.getOrElse(Wellhead(0.0, 0.0, "CRS")))
            .withFieldComputed(_.sources, u => Seq(u.source))
            .transform)

      WellsRelation.wells = WellsRelation.wells ++ newWells
      }
  }

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def delete(rows: Seq[Row]): IO[Unit] = IO {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[AssetSource](r))
    WellsRelation.wells = WellsRelation.wells.filter(w =>
      w.sources.forall(s => !deletes.contains(s))
    )
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  private def toRow(a: Well): Row = asRow(a)

  private def uniqueId(a: Well) = (a.sources.head.sourceName, a.sources.head.assetExternalId)

  override def buildScan(): RDD[Row] = {
    SdkV1Rdd(
      sqlContext.sparkContext,
      config,
      (a: Well, _) => toRow(a),
      uniqueId,
      getStreams()
    )
  }


  private def getStreams()(
    @nowarn client: GenericClient[IO],
    @nowarn limit: Option[Int],
    @nowarn numPartitions: Int): Seq[Stream[IO, Well]] = {
      Seq(
        Stream.emits(WellsRelation.wells)
      )
  }
}

object WellsRelation {
  private var wells = Vector[Well](
    Well(
      matchingId = "my matching id",
      name = "my name",
      wellhead = Wellhead(0.1, 10.1, "CRS"),
      sources = Seq(AssetSource("EDM:well-1", "EDM"))
    )
  )
}
