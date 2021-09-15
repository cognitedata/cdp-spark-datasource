package cognite.spark.v1

import java.time.Instant
import io.scalaland.chimney.dsl._
import cats.effect.IO
import com.cognite.sdk.scala.v1.{AssetCreate, File, FileCreate, FileUpdate, GenericClient}
import cognite.spark.v1.SparkSchemaHelper._
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.stringSeqToCogniteExternalIdSeq
import com.cognite.sdk.scala.common.{WithExternalId, WithId}
import com.cognite.sdk.scala.v1.resources.Files
import fs2.Stream
import io.scalaland.chimney.Transformer

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[File, Long](config, "files")
    with InsertableRelation
    with WritableRelation {

  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val files = rows.map(fromRow[FileCreate](_))
    createOrUpdateByExternalId[File, FileUpdate, FileCreate, FileCreate, Option, Files[IO]](
      Set.empty,
      files,
      client.files,
      doUpsert)
  }

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, File]] =
    Seq(client.files.list(limit))

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val files = rows.map(fromRow[FileCreate](_))
    client.files
      .create(files)
      .flatTap(_ => incMetrics(itemsCreated, files.size)) *> IO.unit
  }

  private def isUpdateEmpty(u: FileUpdate): Boolean = u == FileUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    val fileUpdates = rows.map(r => fromRow[FilesUpsertSchema](r))
    updateByIdOrExternalId[FilesUpsertSchema, FileUpdate, Files[IO], File](
      fileUpdates,
      client.files,
      isUpdateEmpty
    )
  }

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => fromRow[DeleteItem](r))
    val ids = deletes.map(_.id)
    client.files
      .deleteByIds(ids)
      .flatTap(_ => incMetrics(itemsDeleted, ids.length))
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val files = rows.map(fromRow[FilesUpsertSchema](_))

    genericUpsert[File, FilesUpsertSchema, FileCreate, FileUpdate, Files[IO]](
      files,
      isUpdateEmpty,
      client.files,
      mustBeUpdate = r => r.name.isEmpty && r.getExternalId().nonEmpty)
  }

  override def schema: StructType = structType[File]

  override def toRow(t: File): Row = asRow(t)

  override def uniqueId(a: File): Long = a.id
}
object FilesRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[FilesUpsertSchema]
  val insertSchema: StructType = structType[FilesInsertSchema]
  val readSchema: StructType = structType[FilesReadSchema]
}

final case class FilesUpsertSchema(
    id: Option[Long] = None,
    name: Option[String] = None,
    directory: Option[String] = None,
    externalId: OptionalField[String] = FieldNotSpecified,
    source: OptionalField[String] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    dataSetId: OptionalField[Long] = FieldNotSpecified,
    mimeType: OptionalField[String] = FieldNotSpecified,
    sourceCreatedTime: OptionalField[Instant] = FieldNotSpecified,
    sourceModifiedTime: OptionalField[Instant] = FieldNotSpecified,
    securityCategories: Option[Seq[Long]] = None
) extends WithNullableExtenalId
    with WithId[Option[Long]]

object FilesUpsertSchema {
  implicit val toCreate: Transformer[FilesUpsertSchema, FileCreate] =
    Transformer
      .define[FilesUpsertSchema, FileCreate]
      .withFieldComputed(
        _.name,
        _.name.getOrElse(
          throw new CdfSparkIllegalArgumentException("The name field must be set when creating files.")))
      .buildTransformer

}

final case class FilesInsertSchema(
    name: String,
    directory: Option[String] = None,
    source: Option[String] = None,
    externalId: Option[String] = None,
    mimeType: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    dataSetId: Option[Long] = None,
    sourceCreatedTime: Option[Instant] = None,
    sourceModifiedTime: Option[Instant] = None,
    securityCategories: Option[Seq[Long]] = None
)

final case class FilesReadSchema(
    id: Long = 0,
    name: String,
    source: Option[String] = None,
    directory: Option[String] = None,
    externalId: Option[String] = None,
    mimeType: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    assetIds: Option[Seq[Long]] = None,
    uploaded: Boolean = false,
    uploadedTime: Option[Instant] = None,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0),
    sourceCreatedTime: Option[Instant] = None,
    sourceModifiedTime: Option[Instant] = None,
    securityCategories: Option[Seq[Long]] = None,
    uploadUrl: Option[String] = None,
    dataSetId: Option[Long] = None
)
