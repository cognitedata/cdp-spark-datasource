package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper._
import cognite.spark.v1.wdl.{WellIngestionInsertSchema, WellsReadSchema}
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.{Filter, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class WellsRelation(
    config: RelationConfig)(override val sqlContext: SQLContext)
  extends CdfRelation(config, "wdl")
    with WritableRelation
    with TableScan {

  override def schema: StructType = structType[WellsReadSchema]()

  override def upsert(rows: Seq[Row]): IO[Unit] =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val insertions = rows.map(fromRow[WellIngestionInsertSchema](_))

      val wells = insertions.map(
        _.into[AssetCreate]
          .withFieldComputed(_.labels, u => stringSeqToCogniteExternalIdSeq(u.labels))
          .transform)
    }

  override def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException(
      "Create (abort) is not supported for data model instances. Use upsert instead.")

  def toRow(a: ProjectedDataModelInstance): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    new GenericRow(a.properties)
  }

  def uniqueId(a: ProjectedDataModelInstance): String = a.externalId

  def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
    @nowarn client: GenericClient[IO],
    limit: Option[Int],
    @nowarn numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = {
    val selectedPropsArray: Array[String] = if (selectedColumns.isEmpty) {
      schema.fields.map(_.name)
    } else {
      selectedColumns
    }

    val filter: DomainSpecificLanguageFilter = {
      val andFilters = filters.toVector.flatMap(getInstanceFilter)
      if (andFilters.isEmpty) EmptyFilter else DSLAndFilter(andFilters)
    }

    val dmiQuery = DataModelInstanceQuery(
      model = DataModelIdentifier(space = Some(spaceExternalId), model = modelExternalId),
      spaceExternalId = instanceSpaceExternalIdFilter,
      filter = filter,
      sort = None,
      limit = limit,
      cursor = None
    )

      Seq(
        alphaClient.nodes
          .queryStream(dmiQuery, limit)
          .map(r => toProjectedInstance(r, selectedPropsArray)))
  }

  override def buildScan(selectedColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedDataModelInstance, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedDataModelInstance, _) => toRow(item),
      uniqueId,
      getStreams(filters, selectedColumns)
    )

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceDeleteSchema](r))
      alphaClient.nodes
        .deleteItems(deletes.map(_.externalId), spaceExternalId)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for data model instances. Use upsert instead.")

  def getIndexedPropertyList(rSchema: StructType): Array[(Int, String, DataModelPropertyDefinition)] =
    rSchema.fields.zipWithIndex.map {
      case (field: StructField, index: Int) =>
        val propertyType = modelInfo.getOrElse(
          field.name,
          throw new CdfSparkException(
            s"Can't insert property `${field.name}` " +
              s"into data model $modelExternalId, the property does not exist in the definition")
        )
        (index, field.name, propertyType)
    }

  def wellFromRow(schema: StructType): Row => Node = {

    val externalIdIndex = getRequiredStringPropertyIndex(schema, modelType, "externalId")
    val indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)] = getIndexedPropertyList(
      schema)

    def parseWellRow(indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)])(
      row: Row): Node = {
      val externalId = getStringValueForFixedProperty(row, "externalId", externalIdIndex)
      val propertyValues: Map[String, DataModelProperty[_]] =
        getDataModelPropertyMap(indexedPropertyList, row)

      Node(externalId, Some(propertyValues))
    }

    parseNodeRow(indexedPropertyList)
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
