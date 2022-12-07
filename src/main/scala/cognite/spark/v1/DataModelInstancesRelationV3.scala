package cognite.spark.v1

import cats.Apply
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstancesHelper._
import com.cognite.sdk.scala.common._
import com.cognite.sdk.scala.v1
import com.cognite.sdk.scala.v1.DataModelType.NodeType
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefinition,
  PropertyType
}
import com.cognite.sdk.scala.v1.fdm.common.refs.SourceReference
import com.cognite.sdk.scala.v1.fdm.containers._
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances.{
  EdgeOrNodeData,
  InstanceCreate,
  InstancePropertyValue,
  SlimNodeOrEdge
}
import com.cognite.sdk.scala.v1.fdm.views.{DataModelReference, ViewDefinition}
import com.cognite.sdk.scala.v1.{
  DataModel,
  DataModelIdentifier,
  DataModelInstanceQuery,
  DataModelProperty,
  DataModelPropertyDefinition,
  DataModelType,
  Edge,
  GenericClient,
  Node,
  PropertyMap
}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.time._
import java.time.format.DateTimeFormatter
import scala.annotation.nowarn
import scala.util.Try

class DataModelInstancesRelationV3(
    config: RelationConfig,
    space: String,
    viewExternalIdAndVersion: Option[(String, String)],
    containerExternalId: Option[String],
    instanceSpaceExternalId: Option[String] = None)(val sqlContext: SQLContext)
    extends CdfRelation(config, "datamodelinstancesV3")
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._

  private val viewDefinitionAndProperties
    : Option[(ViewDefinition, Map[String, ViewPropertyDefinition])] =
    viewExternalIdAndVersion
      .flatTraverse {
        case (viewExtId, viewVersion) =>
          val allProperties = alphaClient.views
            .retrieveItems(
              Seq(DataModelReference(space, viewExtId, viewVersion)),
              includeInheritedProperties = Some(true))
            .map(_.headOption)
            .flatMap {
              case Some(viewDef) =>
                viewDef.implements.traverse { v =>
                  alphaClient.views
                    .retrieveItems(v.map(vRef =>
                      DataModelReference(vRef.space, vRef.externalId, vRef.version)))
                    .map { inheritingViews =>
                      val inheritingProperties = inheritingViews
                        .map(_.properties)
                        .reduce((propMap1, propMap2) => propMap1 ++ propMap2)

                      (viewDef, inheritingProperties ++ viewDef.properties)
                    }
                }
              case None => IO.pure(None)
            }
          allProperties
      }
      .unsafeRunSync()

  private val containerDefinition: Option[ContainerDefinition] =
    containerExternalId
      .flatTraverse { extId =>
        alphaClient.containers
          .retrieveByExternalIds(Seq(ContainerId(space, extId)))
          .map(_.headOption)
      }
      .unsafeRunSync()

  val modelExternalId = ""
  private val model: DataModel = alphaClient.dataModels
    .retrieveByExternalIds(Seq(modelExternalId), spaceExternalId = space)
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
        "externalId" -> DataModelPropertyDefinition(v1.PropertyType.Text, false),
        "type" -> DataModelPropertyDefinition(v1.PropertyType.Text, false),
        "startNode" -> DataModelPropertyDefinition(v1.PropertyType.Text, false),
        "endNode" -> DataModelPropertyDefinition(v1.PropertyType.Text, false)
      )
    } else {
      modelProps ++ Map(
        "externalId" -> DataModelPropertyDefinition(v1.PropertyType.Text, false)
      )
    }
  }

  override def schema: StructType = {
    val fields = viewDefinitionAndProperties
      .map {
        case (_, fieldsMap) =>
          fieldsMap.map {
            case (identifier, propType) =>
              StructField(
                identifier,
                convertToSparkDataType(propType.`type`),
                nullable = propType.nullable.getOrElse(true))
          }
      }
      .orElse(containerDefinition.map { container =>
        container.properties.map {
          case (identifier, propType) =>
            DataTypes.createStructField(
              identifier,
              convertToSparkDataType(propType.`type`),
              propType.nullable.getOrElse(true))
        }
      })
      .getOrElse {
        throw new CdfSparkException(s"""
                                       |Correct (view external id, view version) pair or container external id should be specified.
                                       |Could not resolve schema from view external id: $viewExternalIdAndVersion or container external id: $containerExternalId
                                       |""".stripMargin)
      }
    DataTypes.createStructType(fields.toArray)
  }

  private def validateFieldTypesWithPropertyDefinition(
      propertyDefMap: Map[String, PropertyDefinition],
      fieldsMap: Map[String, StructField]): IO[Boolean] = {

    val (_existsInPropDef, missingInPropDef) = fieldsMap.partition {
      case (fieldName, _) => propertyDefMap.contains(fieldName)
    }

    val (nonNullableMissingFields, _nullableMissingFields) = missingInPropDef.partition {
      case (fieldName, _) => propertyDefMap.get(fieldName).flatMap(_.nullable).contains(false)
    }

    if (nonNullableMissingFields.nonEmpty) {
      IO.raiseError(
        new CdfSparkException(
          s"""Can't find required properties: ${nonNullableMissingFields.keySet
               .mkString(", ")}""".stripMargin
        )
      )
    } else {
      IO.pure(true)
    }
  }

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val result = upsertNodeWriteItems(rows)

    result.as[Unit](())
  }

  // scalastyle:off
  private def upsertNodeWriteItems(rows: Seq[Row]): IO[Seq[SlimNodeOrEdge]] =
    Apply[Option].map2(rows.headOption, containerDefinition)(Tuple2.apply).traverse {
      case (firstRow, container) if container.usedFor == ContainerUsage.Node =>
        val containerPropertyDefMap = container.properties
        val destinationContainer = ContainerReference(space, container.externalId)
        val rowSchema = firstRow.schema
        val fieldsByName = rowSchema.fields.map(f => f.name -> f).toMap
        createNodes(rows, rowSchema, containerPropertyDefMap, destinationContainer, fieldsByName)

      case (firstRow, container) if container.usedFor == ContainerUsage.Edge =>
        val containerPropertyDefMap = container.properties
        val destinationContainer = ContainerReference(space, container.externalId)
        val rowSchema = firstRow.schema
        val fieldsByName = rowSchema.fields.map(f => f.name -> f).toMap
//        createEdges(rows, rowSchema, containerPropertyDefMap, destinationContainer, fieldsByName)
        ???

      case (firstRow, container) if container.usedFor == ContainerUsage.All =>
        val containerPropertyDefMap = container.properties
        val destinationContainer = ContainerReference(space, container.externalId)
        val rowSchema = firstRow.schema
        val fieldsByName = rowSchema.fields.map(f => f.name -> f).toMap
        createNodes(rows, rowSchema, containerPropertyDefMap, destinationContainer, fieldsByName)
    }

  private def createNodes(
      rows: Seq[Row],
      rowSchema: StructType,
      propertyDefMap: Map[String, PropertyDefinition],
      destinationRef: SourceReference,
      fieldsByName: Map[String, StructField]): IO[Seq[SlimNodeOrEdge]] =
    validateFieldTypesWithPropertyDefinition(propertyDefMap, fieldsByName).flatMap { _ =>
      Try(rowSchema.fieldIndex("externalId")).toOption match {
        case Some(extIdFieldIndex) =>
          val containerWriteItems =
            createNodeWriteData(extIdFieldIndex, destinationRef, propertyDefMap, rows)

          containerWriteItems match {
            case Left(err) => IO.raiseError(err)
            case Right(items) =>
              val instanceCreate = InstanceCreate(
                items = items,
                autoCreateStartNodes = Some(true),
                autoCreateEndNodes = Some(true),
                replace = Some(false) // TODO: verify this
              )
              alphaClient.instances.createItems(instanceCreate)
          }
        case None =>
          IO.raiseError[Seq[SlimNodeOrEdge]](
            new CdfSparkException(s"Couldn't find externalId of the Node"))
      }
    }

  private def createNodeWriteData(
      nodeExternalIdFieldIndex: Int,
      destinationRef: SourceReference,
      propertyDefMap: Map[String, PropertyDefinition],
      rows: Seq[Row]): Either[CdfSparkException, List[NodeWrite]] =
    rows.toList.traverse { row =>
      val properties = row.schema.fields.toList.flatTraverse { field =>
        propertyDefMap.get(field.name).toList.traverse { propDef =>
          val instancePropertyValue = containerPropertyDefToInstancePropertyValue(row, field, propDef)
          instancePropertyValue.map(t => field.name -> t)
        }
      }
      properties.map { props =>
        NodeWrite(
          space = space,
          externalId = row.getString(nodeExternalIdFieldIndex),
          sources = Seq(
            EdgeOrNodeData(
              source = destinationRef,
              properties = Some(props.toMap)
            )
          )
        )
      }
    }

  private def containerPropertyDefToInstancePropertyValue(
      row: Row,
      field: StructField,
      propDef: PropertyDefinition): Either[CdfSparkException, InstancePropertyValue] = {
    val instancePropertyValueResult = propDef.`type` match {
      case DirectNodeRelationProperty(_) => // TODO: Verify this
        row
          .getSeq[String](row.schema.fieldIndex(field.name))
          .toList
          .traverse(io.circe.parser.parse)
          .map(InstancePropertyValue.ObjectsList.apply)
          .leftMap(e =>
            new CdfSparkException(
              s"Error parsing value of field '${field.name}' as a list of json objects: ${e.getMessage}"))
      case t if t.isList => toInstantPropertyValueList(row, field, propDef)
      case _ => toInstantPropertyValueNonList(row, field, propDef)
    }

    instancePropertyValueResult.leftMap {
      case e: CdfSparkException => e
      case e: Throwable =>
        new CdfSparkException(s"Error parsing value of field '${field.name}': ${e.getMessage}")
    }
  }

  private def toInstantPropertyValueList(
      row: Row,
      field: StructField,
      propDef: PropertyDefinition): Either[Throwable, InstancePropertyValue] =
    propDef.`type` match {
      case TextProperty(Some(true), _) =>
        Try(InstancePropertyValue.StringList(row.getSeq[String](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
        Try(InstancePropertyValue.BooleanList(row.getSeq[Boolean](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
        Try(InstancePropertyValue.DoubleList(row.getSeq[Double](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
        Try(InstancePropertyValue.DoubleList(row.getSeq[Double](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
        Try(InstancePropertyValue.IntegerList(row.getSeq[Long](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
        Try(InstancePropertyValue.IntegerList(row.getSeq[Long](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Numeric, Some(true)) =>
        Try(InstancePropertyValue.DoubleList(row.getSeq[Double](row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
        Try(
          InstancePropertyValue.TimestampList(
            row
              .getSeq[String](row.schema.fieldIndex(field.name))
              .map(ZonedDateTime.parse(_, DateTimeFormatter.ISO_ZONED_DATE_TIME)))).toEither
          .leftMap(e => new CdfSparkException(s"""
                                                 |Error parsing value of field '${field.name}' as a list of ISO formatted zoned timestamps: ${e.getMessage}
                                                 |Expected timestamp format is: ${DateTimeFormatter.ISO_ZONED_DATE_TIME.toString}
                                                 |""".stripMargin))
      case PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
        Try(
          InstancePropertyValue.DateList(
            row
              .getSeq[String](row.schema.fieldIndex(field.name))
              .map(LocalDate.parse(_, DateTimeFormatter.ISO_LOCAL_DATE)))).toEither
          .leftMap(e => new CdfSparkException(s"""
                                                 |Error parsing value of field '${field.name}' as a list of ISO formatted dates: ${e.getMessage}
                                                 |Expected date format is: ${DateTimeFormatter.ISO_LOCAL_DATE.toString}
                                                 |""".stripMargin))
      case PrimitiveProperty(PrimitivePropType.Json, Some(true)) | DirectNodeRelationProperty(_) =>
        row
          .getSeq[String](row.schema.fieldIndex(field.name))
          .toList
          .traverse(io.circe.parser.parse)
          .map(InstancePropertyValue.ObjectsList.apply)
          .leftMap(e =>
            new CdfSparkException(
              s"Error parsing value of field '${field.name}' as a list of json objects: ${e.getMessage}"))
    }

  private def toInstantPropertyValueNonList(row: Row, field: StructField, propDef: PropertyDefinition) =
    propDef.`type` match {
      case TextProperty(None | Some(false), _) =>
        Try(InstancePropertyValue.String(row.getString(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Boolean, None | Some(false)) =>
        Try(InstancePropertyValue.Boolean(row.getBoolean(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Float32, None | Some(false)) =>
        Try(InstancePropertyValue.Double(row.getDouble(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
        Try(InstancePropertyValue.Double(row.getDouble(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
        Try(InstancePropertyValue.Integer(row.getLong(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
        Try(InstancePropertyValue.Integer(row.getLong(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Numeric, None | Some(false)) =>
        Try(InstancePropertyValue.Double(row.getDouble(row.schema.fieldIndex(field.name)))).toEither
      case PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
        Try(
          InstancePropertyValue.Timestamp(
            ZonedDateTime.parse(
              row.getString(row.schema.fieldIndex(field.name)),
              DateTimeFormatter.ISO_ZONED_DATE_TIME))).toEither
          .leftMap(e => new CdfSparkException(s"""
                                                 |Error parsing value of field '${field.name}' as a list of ISO formatted zoned timestamps: ${e.getMessage}
                                                 |Expected timestamp format is: ${DateTimeFormatter.ISO_ZONED_DATE_TIME.toString}
                                                 |""".stripMargin))
      case PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
        Try(
          InstancePropertyValue.Date(
            LocalDate.parse(
              row.getString(row.schema.fieldIndex(field.name)),
              DateTimeFormatter.ISO_LOCAL_DATE))).toEither
          .leftMap(e => new CdfSparkException(s"""
                                                 |Error parsing value of field '${field.name}' as a list of ISO formatted dates: ${e.getMessage}
                                                 |Expected date format is: ${DateTimeFormatter.ISO_LOCAL_DATE.toString}
                                                 |""".stripMargin))
      case PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
        io.circe.parser
          .parse(row
            .getString(row.schema.fieldIndex(field.name)))
          .map(InstancePropertyValue.Object.apply)
          .leftMap(e =>
            new CdfSparkException(
              s"Error parsing value of field '${field.name}' as a list of json objects: ${e.getMessage}"))
    }

  private def convertToSparkDataType(propType: PropertyType): DataType = {
    def primitivePropTypeToSparkDataType(ppt: PrimitivePropType): DataType = ppt match {
      case PrimitivePropType.Timestamp => DataTypes.TimestampType
      case PrimitivePropType.Date => DataTypes.DateType
      case PrimitivePropType.Boolean => DataTypes.BooleanType
      case PrimitivePropType.Float32 => DataTypes.FloatType
      case PrimitivePropType.Float64 => DataTypes.DoubleType
      case PrimitivePropType.Int32 => DataTypes.IntegerType
      case PrimitivePropType.Int64 => DataTypes.LongType
      case PrimitivePropType.Numeric => DataTypes.DoubleType
      case PrimitivePropType.Json => DataTypes.StringType
    }

    propType match {
      case TextProperty(Some(true), _) => DataTypes.createArrayType(DataTypes.StringType)
      case PrimitiveProperty(ppt, Some(true)) =>
        DataTypes.createArrayType(primitivePropTypeToSparkDataType(ppt))
      case PrimitiveProperty(ppt, _) => primitivePropTypeToSparkDataType(ppt)
      case DirectNodeRelationProperty(_) =>
        DataTypes.createArrayType(DataTypes.StringType) // TODO: Verify this
    }
  }

  def upsert2(rows: Seq[Row]): IO[Unit] =
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
            DataModelIdentifier(Some(space), modelExternalId),
            overwrite = false,
            dataModelNodes)
          .flatTap(_ => incMetrics(itemsUpserted, dataModelNodes.length)) *> IO.unit
      } else {
        val fromRowFn = edgeFromRow(rows.head.schema)
        val dataModelEdges: Seq[Edge] = rows.map(fromRowFn)
        alphaClient.edges
          .createItems(
            instanceSpace,
            model = DataModelIdentifier(Some(space), modelExternalId),
            autoCreateStartNodes = true,
            autoCreateEndNodes = true,
            overwrite = false,
            dataModelEdges
          )
          .flatTap(_ => incMetrics(itemsUpserted, dataModelEdges.length)) *> IO.unit
      }
    }

  def insert(rows: Seq[Row]): IO[Unit] =
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
        Some(DSLEqualsFilter(Seq(space, modelExternalId, left), parsePropertyValue(left, right)))
      case In(attribute, values) =>
        if (modelInfo(attribute).`type`.code.endsWith("[]")) {
          None
        } else {
          val setValues = values.filter(_ != null)
          Some(
            DSLInFilter(
              Seq(space, modelExternalId, attribute),
              setValues.map(parsePropertyValue(attribute, _)).toIndexedSeq))
        }
      case StringStartsWith(attribute, value) =>
        Some(
          DSLPrefixFilter(Seq(space, modelExternalId, attribute), parsePropertyValue(attribute, value)))
      case GreaterThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            gte = Some(parsePropertyValue(attribute, value))))
      case GreaterThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            gt = Some(parsePropertyValue(attribute, value))))
      case LessThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
            lte = Some(parsePropertyValue(attribute, value))))
      case LessThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(space, modelExternalId, attribute),
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
        Some(DSLExistsFilter(Seq(space, modelExternalId, attribute)))
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
      .getOrElse(space)

    val dmiQuery = DataModelInstanceQuery(
      model = DataModelIdentifier(space = Some(space), model = modelExternalId),
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
        .deleteItems(deletes.map(_.externalId), space)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    } else {
      alphaClient.edges
        .deleteItems(deletes.map(_.externalId), space)
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    }
  }

  def update(rows: Seq[Row]): IO[Unit] =
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
              toPropertyType(propT.`type`)(row.get(index))
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

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
final case class DataModelInstanceDeleteSchema(spaceExternalId: Option[String], externalId: String)
