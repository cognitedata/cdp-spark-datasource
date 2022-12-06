package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.PushdownUtilities._
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.resources.Wells
import fs2.Stream
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.{Filter, InsertableRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time.Instant

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

final case class Wellheads(
    x: Double,
    y: Double,
    /* The coordinate reference systems of the wellhead. */
    crs: String,
)

final case class AssetSource(
    /* Asset external ID. */
    assetExternalId: String,
    /* Name of the source this asset external ID belongs to. */
    sourceName: String,
)

object DistanceUnitEnum extends Enumeration {
  type DistanceUnit = Value

  val meter = Value("meter")

  val foot = Value("foot")

  val inch = Value("inch")

  val yard = Value("yard")
}

final case class Distance(
    /* Amount of a given unit. */
    value: Double,
    unit: DistanceUnitEnum.DistanceUnit,
)

final case class Datum (
    /* Amount of a given unit. */
    value: Double,
    unit: DistanceUnitEnum.DistanceUnit,
    /* The name of the reference point. Eg. \"KB\" for kelly bushing. */
    reference: String,
)

final case class Wellbore (
    /* Unique identifier used to match wellbores from different sources. The matchingId must be unique within a source. */
    matchingId: String,
    /* Name of the wellbore. */
    name: String,
    /* Matching id of the associated well. */
    wellMatchingId: String,
    /* List of sources that are associated to this wellbore. */
    sources: Seq[AssetSource],
    /* Description of the wellbore. */
    description: Option[String] = None,
    /* Parent wellbore if it exists. */
    parentWellboreMatchingId: Option[String] = None,
    /* Also called UBI. */
    uniqueWellboreIdentifier: Option[String] = None,
    datum: Option[Datum] = None,
    /* Total days of drilling for this wellbore */
    totalDrillingDays: Option[Double] = None,
)

final case class WellsReadSchema(
    /* Unique identifier used to match wells from different sources. */
    matchingId: String,
    /* Name of the well. */
    name: String,
    wellhead: Wellheads,
    /* List of sources that are associated to this well. */
    sources: Seq[AssetSource],
    /* Description of the well. */
    description: Option[String] = None,
    /* Also called UWI. */
    uniqueWellIdentifier: Option[String] = None,
    /* Country of the well. */
    country: Option[String] = None,
    /* The quadrant of the well. This is the first part of the unique well identifier used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in quadrant `15`. */
    quadrant: Option[String] = None,
    /* Region of the well. */
    region: Option[String] = None,
    /* The block of the well. This is the second part of the unique well identifer used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in block `15/9`. */
    block: Option[String] = None,
    /* Field of the well. */
    field: Option[String] = None,
    /* Operator that owns the well. */
    operator: Option[String] = None,
    /* The date a new well was spudded or the date of first actual penetration of the earth with a drilling bit. */
    spudDate: Option[java.time.LocalDate] = None,
    /* Exploration or development. */
    wellType: Option[String] = None,
    /* Well licence. */
    license: Option[String] = None,
    /* Water depth of the well. Vertical distance from the mean sea level (MSL) to the sea bed. */
    waterDepth: Option[Distance] = None,
    /* List of wellbores that are associated to this well. */
    wellbores: Option[Seq[Wellbore]] = None,
)

final case class WellIngestionInsertSchema (
    /* Name of the well. */
    name: String,
    /* Connection between this well and the well asset with a given source. */
    source: AssetSource,
    /* Unique identifier used to match wells from different sources. The matchingId must be unique within a source. */
    matchingId: Option[String] = None,
    /* Description of the well. */
    description: Option[String] = None,
    /* Also called UWI. */
    uniqueWellIdentifier: Option[String] = None,
    /* Country of the well. */
    country: Option[String] = None,
    /* The quadrant of the well. This is the first part of the unique well identifier used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in quadrant `15`. */
    quadrant: Option[String] = None,
    /* Region of the well. */
    region: Option[String] = None,
    /* The date a new well was spudded or the date of first actual penetration of the earth with a drilling bit. */
    spudDate: Option[java.time.LocalDate] = None,
    /* The block of the well. This is the second part of the unique well identifer used on the norwegian continental shelf. The wellbore `15/9-19-RS` in the VOLVE fild is in block `15/9`. */
    block: Option[String] = None,
    /* Field of the well. */
    field: Option[String] = None,
    /* Operator that owns the well. */
    operator: Option[String] = None,
    /* Exploration or development. */
    wellType: Option[String] = None,
    /* Well licence. */
    license: Option[String] = None,
    /* Water depth of the well. Vertical distance from the mean sea level (MSL) to the sea bed. */
    waterDepth: Option[Distance] = None,
    wellhead: Option[Wellheads] = None,
)
