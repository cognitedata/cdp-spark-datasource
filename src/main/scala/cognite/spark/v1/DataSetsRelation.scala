package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities.{executeFilterOnePartition, pushdownToFilters, timeRange}
import cognite.spark.compiletime.macros.SparkSchemaHelper._
import com.cognite.sdk.scala.common.WithId
import com.cognite.sdk.scala.v1.resources.DataSets
import com.cognite.sdk.scala.v1.{DataSet, DataSetCreate, DataSetFilter, DataSetUpdate, GenericClient}
import io.scalaland.chimney.Transformer
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

class DataSetsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[DataSet, String](config, DataSetsRelation.name)
    with WritableRelation {

  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._
  override def schema: StructType = structType[DataSet]()

  override def toRow(a: DataSet): Row = asRow(a)

  override def uniqueId(a: DataSet): String = a.id.toString

  override def getStreams(sparkFilters: Array[Filter])(
      client: GenericClient[IO]): Seq[fs2.Stream[IO, DataSet]] = {
    val (ids, filters) =
      pushdownToFilters(sparkFilters, f => dataSetFilterFromMap(f.fieldValues), DataSetFilter())
    Seq(executeFilterOnePartition(client.dataSets, filters, ids, config.limitPerPartition))
  }

  private def dataSetFilterFromMap(m: Map[String, String]) =
    DataSetFilter(
      createdTime = timeRange(m, "createdTime"),
      lastUpdatedTime = timeRange(m, "lastUpdatedTime"),
      externalIdPrefix = m.get("externalIdPrefix"),
      writeProtected = m.get("writeProtected").map(_.toBoolean)
    )

  override def insert(rows: Seq[Row]): IO[Unit] = {
    val dataSetCreates = rows.map(fromRow[DataSetCreate](_))
    client.dataSets
      .create(dataSetCreates)
      .flatTap(_ => incMetrics(itemsCreated, dataSetCreates.length)) *> IO.unit
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val dataSets = rows.map(fromRow[DataSetsUpsertSchema](_))
    genericUpsert[DataSet, DataSetsUpsertSchema, DataSetCreate, DataSetUpdate, DataSets[IO]](
      dataSets,
      isUpdateEmpty,
      client.dataSets,
      mustBeUpdate = d => d.name.toOption.isEmpty && d.getExternalId.nonEmpty
    )
  }

  private def isUpdateEmpty(u: DataSetUpdate): Boolean = u == DataSetUpdate()

  override def update(rows: Seq[Row]): IO[Unit] = {
    val dataSetsUpdates = rows.map(fromRow[DataSetsUpsertSchema](_))
    updateByIdOrExternalId[DataSetsUpsertSchema, DataSetUpdate, DataSets[IO], DataSet](
      dataSetsUpdates,
      client.dataSets,
      isUpdateEmpty)
  }

  override def delete(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Delete is not supported for data sets.")
}
object DataSetsRelation
    extends UpsertSchema
    with ReadSchema
    with InsertSchema
    with UpdateSchemaFromUpsertSchema
    with NamedRelation {
  override val name = "datasets"
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override val upsertSchema: StructType = structType[DataSetsUpsertSchema]()
  override val insertSchema: StructType = structType[DataSetsInsertSchema]()
  override val readSchema: StructType = structType[DataSetsReadSchema]()

}

case class DataSetsUpsertSchema(
    id: Option[Long],
    externalId: OptionalField[String] = FieldNotSpecified,
    name: OptionalField[String] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    writeProtected: Option[Boolean] = None)
    extends WithNullableExternalId
    with WithId[Option[Long]]
object DataSetsUpsertSchema {
  implicit val toCreate: Transformer[DataSetsUpsertSchema, DataSetCreate] =
    Transformer
      .define[DataSetsUpsertSchema, DataSetCreate]
      .withFieldComputed(
        _.writeProtected,
        _.writeProtected.getOrElse(
          throw new CdfSparkIllegalArgumentException(
            "The writeProtected field must be set when creating data sets.")
        )
      )
      .buildTransformer
}

case class DataSetsInsertSchema(
    externalId: Option[String] = None,
    name: Option[String] = None,
    description: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    writeProtected: Boolean = false)

case class DataSetsReadSchema(
    id: Long = 0,
    name: Option[String] = None,
    writeProtected: Boolean = false,
    externalId: Option[String] = None,
    description: Option[String] = None,
    metadata: Option[Map[String, String]] = None,
    createdTime: Instant = Instant.ofEpochMilli(0),
    lastUpdatedTime: Instant = Instant.ofEpochMilli(0)
)
