package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.common.WithId
import com.cognite.sdk.scala.v1.resources.Files
import com.cognite.sdk.scala.v1.{
  CogniteInternalId,
  File,
  FileCreate,
  FileUpdate,
  FilesFilter,
  GenericClient
}
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class FilesRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1InsertableRelation[FilesReadSchema, Long](config, FilesRelation.name)
    with WritableRelation {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  override def getFromRowsAndCreate(rows: Seq[Row], doUpsert: Boolean = true): IO[Unit] = {
    val filesUpserts = rows.map(fromRow[FilesUpsertSchema](_))
    val files = filesUpserts.map(_.transformInto[FileCreate])
    createOrUpdateByExternalId[File, FileUpdate, FileCreate, FileCreate, Option, Files[IO]](
      Set.empty,
      files,
      client.files,
      doUpsert)
  }

  private def filesFilterFromMap(m: Map[String, String]): FilesFilter =
    FilesFilter(
      name = m.get("name"),
      mimeType = m.get("mimeType"),
      assetIds = m.get("assetIds").map(idsFromStringifiedArray),
      createdTime = timeRange(m, "createdTime"),
      lastUpdatedTime = timeRange(m, "lastUpdatedTime"),
      uploadedTime = timeRange(m, "uploadedTime"),
      sourceCreatedTime = timeRange(m, "sourceCreatedTime"),
      sourceModifiedTime = timeRange(m, "sourceModifiedTime"),
      uploaded = m.get("uploaded").map(_.toBoolean),
      source = m.get("source"),
      dataSetIds = m.get("dataSetId").map(idsFromStringifiedArray(_).map(CogniteInternalId(_))),
      labels = m.get("labels").flatMap(externalIdsToContainsAny),
      externalIdPrefix = m.get("externalIdPrefix")
    )

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO]): Seq[Stream[IO, FilesReadSchema]] = {
    val (ids, filters) =
      pushdownToFilters(sparkFilters, f => filesFilterFromMap(f.fieldValues), FilesFilter())
    executeFilter(client.files, filters, ids, config.partitions, config.limitPerPartition).map(
      _.map(
        _.into[FilesReadSchema]
          .withFieldComputed(_.labels, u => cogniteExternalIdSeqToStringSeq(u.labels))
          .transform))
  }

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val filesInsertions = rows.map(fromRow[FilesInsertSchema](_))
    val files = filesInsertions.map(
      _.into[FileCreate]
        .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
        .transform)
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
      mustBeUpdate = f => f.name.isEmpty && f.getExternalId.nonEmpty)
  }

  override def schema: StructType = structType[FilesReadSchema]()

  override def toRow(t: FilesReadSchema): Row = asRow(t)

  override def uniqueId(a: FilesReadSchema): Long = a.id
}
object FilesRelation
    extends UpsertSchema
    with ReadSchema
    with InsertSchema
    with DeleteWithIdSchema
    with UpdateSchemaFromUpsertSchema
    with NamedRelation {
  override val name: String = "files"
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override val upsertSchema: StructType = structType[FilesUpsertSchema]()
  override val insertSchema: StructType = structType[FilesInsertSchema]()
  override val readSchema: StructType = structType[FilesReadSchema]()

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
    securityCategories: Option[Seq[Long]] = None,
    labels: Option[Seq[String]] = None
) extends WithNullableExternalId
    with WithId[Option[Long]]

object FilesUpsertSchema {
  implicit val toCreate: Transformer[FilesUpsertSchema, FileCreate] =
    Transformer
      .define[FilesUpsertSchema, FileCreate]
      .withFieldComputed(
        _.name,
        _.name.getOrElse(
          throw new CdfSparkIllegalArgumentException("The name field must be set when creating files.")))
      .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
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
    securityCategories: Option[Seq[Long]] = None,
    labels: Option[Seq[String]] = None
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
    labels: Option[Seq[String]] = None,
    sourceCreatedTime: Option[Instant] = None,
    sourceModifiedTime: Option[Instant] = None,
    securityCategories: Option[Seq[Long]] = None,
    uploadUrl: Option[String] = None,
    dataSetId: Option[Long] = None
)
