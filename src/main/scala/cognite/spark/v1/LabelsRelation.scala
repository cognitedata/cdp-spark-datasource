package cognite.spark.v1

import cats.effect.IO
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.v1.{GenericClient, Label, LabelCreate, LabelsFilter}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class LabelsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Label, String](config, LabelsRelation.name)
    with WritableRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  override def schema: StructType = structType[Label]()

  override def toRow(a: Label): Row = asRow(a)

  override def uniqueId(a: Label): String = a.externalId

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO]): Seq[fs2.Stream[IO, Label]] =
    Seq(client.labels.filter(LabelsFilter()))

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val labels = rows.map(fromRow[LabelCreate](_))
    client.labels
      .create(labels)
      .flatTap(_ => incMetrics(itemsCreated, labels.length)) *> IO.unit
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val labelIds = rows.map(fromRow[DeleteSchemaWithExternalId](_)).map(_.externalId)
    client.labels
      .deleteByExternalIds(labelIds)
      .flatTap(_ => incMetrics(itemsDeleted, labelIds.length))
  }

  override def upsert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Upsert is not supported for labels.")

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for labels.")
}

object LabelsRelation
    extends ReadSchema
    with DeleteWithExternalIdSchema
    with AbortSchema
    with NamedRelation {
  override val name: String = "labels"
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  val abortSchema: StructType = structType[LabelInsertSchema]()
  val readSchema: StructType = structType[LabelReadSchema]()
}

final case class LabelInsertSchema(
    externalId: String,
    name: String,
    description: Option[String],
    dataSetId: Option[Long]
)

final case class LabelReadSchema(
    externalId: Option[String],
    name: Option[String],
    description: Option[String],
    createdTime: Option[Instant],
    dataSetId: Option[Long]
)
