package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common.WithId
import com.cognite.sdk.scala.v1.resources.DataSets
import com.cognite.sdk.scala.v1.{DataSet, DataSetCreate, DataSetUpdate, GenericClient}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}

class DataSetsRelation(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[DataSet, String](config, "datasets")
    with InsertableRelation
    with WritableRelation {

  override def schema: StructType = structType[DataSet]

  override def toRow(a: DataSet): Row = asRow(a)

  override def uniqueId(a: DataSet): String = a.id.toString

  override def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[fs2.Stream[IO, DataSet]] =
    Seq(client.dataSets.list(limit))

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
      mustBeUpdate = d => d.name.toOption.isEmpty && d.getExternalId().nonEmpty
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
object DataSetsRelation extends UpsertSchema {
  val upsertSchema: StructType = structType[DataSetsUpsertSchema]
  val insertSchema: StructType = structType[DataSetCreate]
  val readSchema: StructType = structType[DataSet]
}

case class DataSetsUpsertSchema(
    id: Option[Long],
    externalId: OptionalField[String] = FieldNotSpecified,
    name: OptionalField[String] = FieldNotSpecified,
    description: OptionalField[String] = FieldNotSpecified,
    metadata: Option[Map[String, String]] = None,
    writeProtected: Boolean = false)
    extends WithNullableExtenalId
    with WithId[Option[Long]]
