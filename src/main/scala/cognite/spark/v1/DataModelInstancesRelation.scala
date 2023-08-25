package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.compiletime.macros.SparkSchemaHelper
import cognite.spark.v1.DataModelInstancesHelper._
import com.cognite.sdk.scala.common.{
  CdpApiException,
  DSLAndFilter,
  DSLEqualsFilter,
  DSLExistsFilter,
  DSLInFilter,
  DSLNotFilter,
  DSLOrFilter,
  DSLPrefixFilter,
  DSLRangeFilter,
  DomainSpecificLanguageFilter,
  EmptyFilter
}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time._
import com.cognite.sdk.scala.v1.DataModelType.NodeType
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.annotation.nowarn

@deprecated("message", since = "0")
class DataModelInstanceRelation(
    config: RelationConfig,
    spaceExternalId: String,
    modelExternalId: String,
    instanceSpaceExternalId: Option[String] = None)(val sqlContext: SQLContext)
    extends CdfRelation(config, "datamodelinstances")
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._

  private val model: DataModel = alphaClient.dataModels
    .retrieveByExternalIds(Seq(modelExternalId), spaceExternalId = spaceExternalId)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(
          s"Could not resolve schema of data model $modelExternalId. " +
            s"Got an exception from the CDF API: ${e.message} (code: ${e.code})",
          e)
    }
    .unsafeRunSync()
    .headOption
    // TODO Missing model externalId used to result in CdpApiException, now it returns empty list
    //  Check with dms team
    .getOrElse(throw new CdfSparkException(
      s"Could not resolve schema of data model $modelExternalId. Please check if the model exists."))

  private val modelType = model.dataModelType

  val modelInfo: Map[String, DataModelPropertyDefinition] = {
    val modelProps = model.properties.getOrElse(Map())

    if (modelType == DataModelType.EdgeType) {
      modelProps ++ Map(
        "externalId" -> DataModelPropertyDefinition(PropertyType.Text, false),
        "type" -> DataModelPropertyDefinition(PropertyType.Text, false),
        "startNode" -> DataModelPropertyDefinition(PropertyType.Text, false),
        "endNode" -> DataModelPropertyDefinition(PropertyType.Text, false)
      )
    } else {
      modelProps ++ Map(
        "externalId" -> DataModelPropertyDefinition(PropertyType.Text, false)
      )
    }
  }

  override def schema: StructType =
    new StructType(modelInfo.map {
      case (name, prop) =>
        StructField(name, propertyTypeToSparkType(prop.`type`), nullable = prop.nullable)
    }.toArray)

  override def upsert(rows: Seq[Row]): IO[Unit] =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val instanceSpace = instanceSpaceExternalId
        .getOrElse(
          throw new CdfSparkException(s"instanceSpaceExternalId must be specified when upserting data."))
      if (modelType == NodeType) {
        val fromRowFn = nodeFromRow(rows.head.schema)
        val dataModelNodes: Seq[Node] = rows.map(fromRowFn)
        alphaClient.nodes
          .createItems(
            instanceSpace,
            DataModelIdentifier(Some(spaceExternalId), modelExternalId),
            overwrite = false,
            dataModelNodes)
          .flatTap(_ => incMetrics(itemsUpserted, dataModelNodes.length)) *> IO.unit
      } else {
        val fromRowFn = edgeFromRow(rows.head.schema)
        val dataModelEdges: Seq[Edge] = rows.map(fromRowFn)
        alphaClient.edges
          .createItems(
            instanceSpace,
            model = DataModelIdentifier(Some(spaceExternalId), modelExternalId),
            autoCreateStartNodes = true,
            autoCreateEndNodes = true,
            overwrite = false,
            dataModelEdges
          )
          .flatTap(_ => incMetrics(itemsUpserted, dataModelEdges.length)) *> IO.unit
      }
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

  // scalastyle:off cyclomatic.complexity
  private def getValue(propName: String, prop: DataModelProperty[_]): Any =
    prop.value match {
      case x: Iterable[_] if (x.size == 2) && (Seq("startNode", "endNode", "type") contains propName) =>
        x.mkString(":")
      case x: java.math.BigDecimal =>
        x.doubleValue
      case x: java.math.BigInteger => x.longValue
      case x: Array[java.math.BigDecimal] =>
        x.toVector.map(i => i.doubleValue)
      case x: Array[java.math.BigInteger] =>
        x.toVector.map(i => i.longValue)
      case x: BigDecimal =>
        x.doubleValue
      case x: BigInt => x.longValue
      case x: Array[BigDecimal] =>
        x.toVector.map(i => i.doubleValue)
      case x: Array[BigInt] =>
        x.toVector.map(i => i.longValue)
      case x: Array[LocalDate] =>
        x.toVector.map(i => java.sql.Date.valueOf(i))
      case x: java.time.LocalDate =>
        java.sql.Date.valueOf(x)
      case x: java.time.ZonedDateTime =>
        java.sql.Timestamp.from(x.toInstant)
      case x: Array[java.time.ZonedDateTime] =>
        x.toVector.map(i => java.sql.Timestamp.from(i.toInstant))
      case _ => prop.value
    }
  // scalastyle:on cyclomatic.complexity

  def toProjectedInstance(
      pmap: PropertyMap,
      requiredPropsArray: Array[String]): ProjectedDataModelInstance = {
    val dmiProperties = pmap.allProperties
    ProjectedDataModelInstance(
      externalId = pmap.externalId,
      properties = requiredPropsArray.map { name: String =>
        dmiProperties.get(name).map(getValue(name, _)).orNull
      }
    )
  }

  // scalastyle:off method.length cyclomatic.complexity
  def getInstanceFilter(sparkFilter: Filter): Option[DomainSpecificLanguageFilter] =
    sparkFilter match {
      case EqualTo(left, right) =>
        Some(
          DSLEqualsFilter(Seq(spaceExternalId, modelExternalId, left), parsePropertyValue(left, right)))
      case In(attribute, values) =>
        if (modelInfo(attribute).`type`.code.endsWith("[]")) {
          None
        } else {
          val setValues = values.filter(_ != null)
          Some(
            DSLInFilter(
              Seq(spaceExternalId, modelExternalId, attribute),
              setValues.map(parsePropertyValue(attribute, _)).toIndexedSeq))
        }
      case StringStartsWith(attribute, value) =>
        Some(
          DSLPrefixFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            parsePropertyValue(attribute, value)))
      case GreaterThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            gte = Some(parsePropertyValue(attribute, value))))
      case GreaterThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            gt = Some(parsePropertyValue(attribute, value))))
      case LessThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            lte = Some(parsePropertyValue(attribute, value))))
      case LessThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            lt = Some(parsePropertyValue(attribute, value))))
      case And(f1, f2) =>
        (getInstanceFilter(f1) ++ getInstanceFilter(f2)).reduceLeftOption((sf1, sf2) =>
          DSLAndFilter(Seq(sf1, sf2)))
      case Or(f1, f2) =>
        (getInstanceFilter(f1), getInstanceFilter(f2)) match {
          case (Some(sf1), Some(sf2)) =>
            Some(DSLOrFilter(Seq(sf1, sf2)))
          case _ =>
            None
        }
      case IsNotNull(attribute) =>
        Some(DSLExistsFilter(Seq(spaceExternalId, modelExternalId, attribute)))
      case Not(f) =>
        getInstanceFilter(f).map(DSLNotFilter)
      case _ => None
    }
  // scalastyle:on method.length cyclomatic.complexity

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

    // only take the first spaceExternalId and ignore the rest, maybe we can throw an error instead in case there are
    // more than one
    val instanceSpaceExternalIdFilter = filters
      .collectFirst {
        case EqualTo("spaceExternalId", value) => value.toString()
      }
      .getOrElse(spaceExternalId)

    val dmiQuery = DataModelInstanceQuery(
      model = DataModelIdentifier(space = Some(spaceExternalId), model = modelExternalId),
      spaceExternalId = instanceSpaceExternalIdFilter,
      filter = filter,
      sort = None,
      limit = limit,
      cursor = None
    )

    if (modelType == NodeType) {
      Seq(
        alphaClient.nodes
          .queryStream(dmiQuery, limit)
          .map(r => toProjectedInstance(r, selectedPropsArray)))
    } else {
      Seq(
        alphaClient.edges
          .queryStream(dmiQuery, limit)
          .map(r => toProjectedInstance(r, selectedPropsArray)))
    }
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
    if (modelType == DataModelType.NodeType) {
      alphaClient.nodes
        .deleteItems(deletes.map(_.externalId), spaceExternalId)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    } else {
      alphaClient.edges
        .deleteItems(deletes.map(_.externalId), spaceExternalId)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    }
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

  def getDataModelPropertyMap(
      indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)],
      row: Row): Map[String, DataModelProperty[_]] =
    indexedPropertyList
      .map {
        case (index, name, propT) =>
          name -> (row.get(index) match {
            case null if !propT.nullable => // scalastyle:off null
              throw new CdfSparkException(propertyNotNullableMessage(propT.`type`))
            case null => // scalastyle:off null
              None
            case _ =>
              toPropertyType(propT.`type`, spaceExternalId)(row.get(index))
          })
      }
      .collect { case (a, Some(value)) => a -> value }
      .toMap

  def edgeFromRow(schema: StructType): Row => Edge = {
    val externalIdIndex = getRequiredStringPropertyIndex(schema, modelType, "externalId")
    val startNodeIndex = getRequiredStringPropertyIndex(schema, modelType, "startNode")
    val endNodeIndex = getRequiredStringPropertyIndex(schema, modelType, "endNode")
    val typeIndex = getRequiredStringPropertyIndex(schema, modelType, "type")
    val indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)] = getIndexedPropertyList(
      schema)

    def parseEdgeRow(indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)])(
        row: Row): Edge = {
      val externalId = getStringValueForFixedProperty(row, "externalId", externalIdIndex)

      val edgeType = getDirectRelationIdentifierProperty(externalId, row, "type", typeIndex)
      val startNode = getDirectRelationIdentifierProperty(externalId, row, "startNode", startNodeIndex)
      val endNode = getDirectRelationIdentifierProperty(externalId, row, "endNode", endNodeIndex)

      val propertyValues: Map[String, DataModelProperty[_]] =
        getDataModelPropertyMap(indexedPropertyList, row)

      Edge(externalId, edgeType, startNode, endNode, Some(propertyValues))
    }

    parseEdgeRow(indexedPropertyList)
  }

  def nodeFromRow(schema: StructType): Row => Node = {
    val externalIdIndex = getRequiredStringPropertyIndex(schema, modelType, "externalId")
    val indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)] = getIndexedPropertyList(
      schema)

    def parseNodeRow(indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)])(
        row: Row): Node = {
      val externalId = getStringValueForFixedProperty(row, "externalId", externalIdIndex)
      val propertyValues: Map[String, DataModelProperty[_]] =
        getDataModelPropertyMap(indexedPropertyList, row)

      Node(externalId, Some(propertyValues))
    }

    parseNodeRow(indexedPropertyList)
  }
}

@deprecated("message", since = "0")
final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
@deprecated("message", since = "0")
final case class DataModelInstanceDeleteSchema(spaceExternalId: Option[String], externalId: String)
