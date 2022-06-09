package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstanceRelation._
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

// scalastyle:off cyclomatic.complexity
class DataModelInstanceRelation(
    config: RelationConfig,
    spaceExternalId: String,
    modelExternalId: String,
    instanceSpaceExternalId: Option[String] = None)(val sqlContext: SQLContext)
    extends CdfRelation(config, "datamodelinstances")
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._
  import com.cognite.sdk.scala.v1.resources.DataModels.{
    dataModelPropertyTypeDecoder,
    dataModelPropertyTypeEncoder
  }

  private val model: DataModel = alphaClient.dataModels
    .retrieveByExternalIds(Seq(modelExternalId), spaceExternalId = spaceExternalId)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(
          s"Could not resolve schema of data model $modelExternalId. " +
            s"Got an exception from CDF API: ${e.message} (code: ${e.code})",
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
        "externalId" -> DataModelPropertyDefinition(PropertyType.Text, false),
        "type" -> DataModelPropertyDefinition(PropertyType.Text, true),
        "name" -> DataModelPropertyDefinition(PropertyType.Text, true),
        "description" -> DataModelPropertyDefinition(PropertyType.Text, true)
      )
    }
  }

  override def schema: StructType =
    new StructType(modelInfo.map {
      case (name, prop) =>
        StructField(name, propertyTypeToSparkType(prop.`type`), nullable = prop.nullable)
    }.toArray)

  private def upsertOrInsert(rows: Seq[Row], overwrite: Boolean) =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val fromRowFn = fromNodeRow(rows.head.schema)
      if (modelType == NodeType) {
        val dataModelNodes: Seq[Node] = rows.map(fromRowFn)
        alphaClient.nodes
          .createItems(
            instanceSpaceExternalId
              .getOrElse(throw new CdfSparkException("instanceSpaceExternalId must be specified")),
            DataModelIdentifier(Some(spaceExternalId), modelExternalId),
            overwrite,
            dataModelNodes
          )
          .flatTap(_ => incMetrics(itemsUpserted, dataModelNodes.length)) *> IO.unit
      } else {
        IO.unit
      }
    }

  override def upsert(rows: Seq[Row]): IO[Unit] = upsertOrInsert(rows, true)

  def insert(rows: Seq[Row]): IO[Unit] = upsertOrInsert(rows, false)

  def toRow(a: ProjectedDataModelInstance): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    Row.fromSeq(a.properties)
  }

  def uniqueId(a: ProjectedDataModelInstance): String = a.externalId

  private def getValue(prop: DataModelProperty[_]): Any =
    prop.value match {
      case x: java.math.BigDecimal =>
        x.doubleValue()
      case x: java.math.BigInteger => x.longValue()
      case x: Array[java.math.BigDecimal] =>
        x.toVector.map(i => i.doubleValue())
      case x: Array[java.math.BigInteger] =>
        x.toVector.map(i => i.longValue())
      case x: BigDecimal =>
        x.doubleValue()
      case x: BigInt => x.longValue()
      case x: Array[BigDecimal] =>
        x.toVector.map(i => i.doubleValue())
      case x: Array[BigInt] =>
        x.toVector.map(i => i.longValue())
      case _ => prop.value
    }

  def toProjectedInstance(
      pmap: PropertyMap,
      requiredPropsArray: Seq[String]): ProjectedDataModelInstance = {
    val dmiProperties = pmap.allProperties
    ProjectedDataModelInstance(
      externalId = pmap.externalId,
      properties = requiredPropsArray.map { name: String =>
        dmiProperties.get(name).map(getValue).orNull
      }.toArray
    )
  }

  // scalastyle:off method.length
  def getInstanceFilter(sparkFilter: Filter): Option[DomainSpecificLanguageFilter] =
    sparkFilter match {
      case EqualTo(left, right) => {
        Some(DSLEqualsFilter(Seq(spaceExternalId, modelExternalId, left), parsePropertyValue(right)))
      }
      case In(attribute, values) =>
        if (modelInfo(attribute).`type`.code.endsWith("[]")) {
          None
        } else {
          val setValues = values.filter(_ != null)
          Some(
            DSLInFilter(
              Seq(spaceExternalId, modelExternalId, attribute),
              setValues.map(parsePropertyValue)))
        }
      case StringStartsWith(attribute, value) =>
        Some(
          DSLPrefixFilter(Seq(spaceExternalId, modelExternalId, attribute), parsePropertyValue(value)))
      case GreaterThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            gte = Some(parsePropertyValue(value))))
      case GreaterThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            gt = Some(parsePropertyValue(value))))
      case LessThanOrEqual(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            lte = Some(parsePropertyValue(value))))
      case LessThan(attribute, value) =>
        Some(
          DSLRangeFilter(
            Seq(spaceExternalId, modelExternalId, attribute),
            lt = Some(parsePropertyValue(value))))
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
  // scalastyle:on method.length

  def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = {
    val selectedPropsArray: Seq[String] = if (selectedColumns.isEmpty) {
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
      filter = filter,
      sort = None,
      limit = limit,
      cursor = None)

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
    if (modelType == DataModelType.NodeType) {
      alphaClient.nodes
        .deleteByExternalIds(deletes.map(_.externalId))
        .flatTap(_ => incMetrics(itemsDeleted, rows.length))
    } else {
      ???
    }
  }

  def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for data model instances. Use upsert instead.")

  // scalastyle:off method.length
  def fromNodeRow(schema: StructType): Row => Node = {
    val externalIdIndex = schema.fieldNames.indexOf("externalId")
    if (externalIdIndex < 0) {
      throw new CdfSparkException("Can't upsert data model instances, `externalId` is missing.")
    }
    val typeIndex = schema.fieldNames.indexOf("type")
    val nameIndex = schema.fieldNames.indexOf("name")
    val descriptionIndex = schema.fieldNames.indexOf("description")

    val indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)] =
      schema.fields.zipWithIndex.map {
        case (field: StructField, index: Int) =>
          val propertyType = modelInfo.getOrElse(
            field.name,
            throw new CdfSparkException(
              s"Can't insert property `${field.name}` " +
                s"into data model $modelExternalId, the property does not exist in the definition")
          )
          (index, field.name, propertyType)
      }

    def parseNodeRow(indexedPropertyList: Array[(Int, String, DataModelPropertyDefinition)])(
        row: Row): Node = {
      val externalId = row.get(externalIdIndex) match {
        case x: String => x
        case _ =>
          throw SparkSchemaHelperRuntime.badRowError(row, "externalId", "String", "")
      }
      val nodeType: Option[String] = if (typeIndex < 0) None else Some(row.getAs[String](typeIndex))
      val nodeName: Option[String] = if (nameIndex < 0) None else Some(row.getAs[String](nameIndex))
      val nodeDescription: Option[String] =
        if (descriptionIndex < 0) None else Some(row.getAs[String](descriptionIndex))

      val propertyValues: Map[String, DataModelProperty[_]] = indexedPropertyList
        .map {
          case (index, name, propT) =>
            name -> (row.get(index) match {
              case null if !propT.nullable => // scalastyle:off null
                throw new CdfSparkException(propertyNotNullableMessage(propT.`type`))
              case null => // scalastyle:off null
                None
              case _ if Seq("externalId", "type", "name", "description") contains name =>
                None
              case _ =>
                Some(toPropertyType(propT.`type`)(row.get(index)))
            })
        }
        .collect { case (a, Some(value)) => a -> value }
        .toMap

      Node(
        externalId = externalId,
        `type` = nodeType,
        name = nodeName,
        description = nodeDescription,
        properties = Some(propertyValues)
      )
    }
    parseNodeRow(indexedPropertyList)
  }
  // scalastyle:on method.length
}
// scalastyle:off

object DataModelInstanceRelation {
  private def unknownPropertyTypeMessage(a: Any) = s"Unknown property type $a."

  private def notValidPropertyTypeMessage(
      a: Any,
      propertyType: String,
      sparkSqlType: Option[String] = None) = {
    val sparkSqlTypeMessage = sparkSqlType
      .map(
        tname =>
          s" Try to cast the value to $tname. " +
            s"For example, ‘$tname(col_name) as prop_name’ or ‘cast(col_name as $tname) as prop_name’.")
      .getOrElse("")

    s"$a of type ${a.getClass} is not a valid $propertyType.$sparkSqlTypeMessage"
  }
  private def propertyNotNullableMessage(propertyType: PropertyType[_]) =
    s"Property of ${propertyType.code} type is not nullable."
  // scalastyle:on

  private def parsePropertyValue(value: Any): DataModelProperty[_] = value match {
    case x: Double => PropertyType.Float64.Property(x)
    case x: Int => PropertyType.Int32.Property(x)
    case x: Float => PropertyType.Float32.Property(x)
    case x: Long => PropertyType.Int64.Property(x)
    case x: java.math.BigDecimal => PropertyType.Float64.Property(x.doubleValue())
    case x: java.math.BigInteger => PropertyType.Int64.Property(x.longValue())
    case x: BigDecimal => PropertyType.Float64.Property(x.doubleValue())
    case x: BigInt => PropertyType.Int64.Property(x.longValue())
    case x: String => PropertyType.Text.Property(x)
    case x: Boolean => PropertyType.Boolean.Property(x)
    case x: Array[Double] => PropertyType.Array.Float64.Property(x)
    case x: Array[Int] => PropertyType.Array.Int32.Property(x)
    case x: Array[Float] => PropertyType.Array.Float32.Property(x)
    case x: Array[Long] => PropertyType.Array.Int64.Property(x)
    case x: Array[String] => PropertyType.Array.Text.Property(x)
    case x: Array[Boolean] => PropertyType.Array.Boolean.Property(x)
    case x: Array[java.math.BigDecimal] =>
      PropertyType.Array.Float64.Property(x.toVector.map(i => i.doubleValue()))
    case x: Array[java.math.BigInteger] =>
      PropertyType.Array.Int64.Property(x.toVector.map(i => i.longValue()))
    case x: Array[BigDecimal] =>
      PropertyType.Array.Float64.Property(x.toVector.map(i => i.doubleValue()))
    case x: Array[BigInt] => PropertyType.Array.Int64.Property(x.toVector.map(i => i.longValue()))
    case x: LocalDate => PropertyType.Date.Property(x)
    case x: java.sql.Date => PropertyType.Date.Property(x.toLocalDate)
    case x: LocalDateTime => PropertyType.Date.Property(x.toLocalDate)
    case x: Instant =>
      PropertyType.Timestamp.Property(
        OffsetDateTime.ofInstant(x, ZoneId.systemDefault()).toZonedDateTime)
    case x: java.sql.Timestamp =>
      PropertyType.Timestamp.Property(
        OffsetDateTime.ofInstant(x.toInstant, ZoneId.systemDefault()).toZonedDateTime)
    case x: java.time.ZonedDateTime =>
      PropertyType.Timestamp.Property(x)
    case x: Array[LocalDate] => PropertyType.Array.Date.Property(x)
    case x: Array[java.sql.Date] => PropertyType.Array.Date.Property(x.map(_.toLocalDate))
    case x: Array[LocalDateTime] => PropertyType.Array.Date.Property(x.map(_.toLocalDate))
    case x: Array[Instant] =>
      PropertyType.Array.Timestamp
        .Property(x.map(OffsetDateTime.ofInstant(_, ZoneId.systemDefault()).toZonedDateTime))
    case x: Array[java.sql.Timestamp] =>
      PropertyType.Array.Timestamp.Property(x.map(ts =>
        OffsetDateTime.ofInstant(ts.toInstant, ZoneId.systemDefault()).toZonedDateTime))
    case x: Array[java.time.ZonedDateTime] =>
      PropertyType.Array.Timestamp.Property(x)
    case x =>
      throw new CdfSparkException(s"Unsupported value ${x.toString} of type ${x.getClass.getName}")
  }

  def propertyTypeToSparkType(propertyType: PropertyType[_]): DataType =
    propertyType match {
      case PropertyType.Text =>
        DataTypes.StringType
      case PropertyType.Boolean => DataTypes.BooleanType
      case PropertyType.Numeric | PropertyType.Float64 => DataTypes.DoubleType
      case PropertyType.Float32 => DataTypes.FloatType
      case PropertyType.Int => DataTypes.IntegerType
      case PropertyType.Int32 => DataTypes.IntegerType
      case PropertyType.Int64 => DataTypes.LongType
      case PropertyType.Bigint => DataTypes.LongType
      case PropertyType.Date => DataTypes.DateType
      case PropertyType.Timestamp => DataTypes.TimestampType
      case PropertyType.DirectRelation => DataTypes.StringType
      case PropertyType.Geometry => DataTypes.StringType
      case PropertyType.Geography => DataTypes.StringType
      case PropertyType.Array.Text => DataTypes.createArrayType(DataTypes.StringType)
      case PropertyType.Array.Boolean => DataTypes.createArrayType(DataTypes.BooleanType)
      case PropertyType.Array.Numeric | PropertyType.Array.Float64 =>
        DataTypes.createArrayType(DataTypes.DoubleType)
      case PropertyType.Array.Float32 => DataTypes.createArrayType(DataTypes.FloatType)
      case PropertyType.Array.Int => DataTypes.createArrayType(DataTypes.IntegerType)
      case PropertyType.Array.Bigint => DataTypes.createArrayType(DataTypes.LongType)
      case a => throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }

  private def toDateProperty: Any => DataModelProperty[_] = {
    case x: LocalDate => PropertyType.Date.Property(x)
    case x: java.sql.Date => PropertyType.Date.Property(x.toLocalDate)
    case x: LocalDateTime => PropertyType.Date.Property(x.toLocalDate)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "date", Some("date")))
  }

  private def toTimestampProperty: Any => DataModelProperty[_] = {
    case x: Instant =>
      PropertyType.Timestamp.Property(
        OffsetDateTime.ofInstant(x, ZoneId.systemDefault()).toZonedDateTime)
    case x: java.sql.Timestamp =>
      // Using ZonedDateTime directly without OffSetDateTime, adds ZoneId to the end of encoded string
      // 2019-10-02T07:00+09:00[Asia/Seoul] instead of 2019-10-02T07:00+09:00, API does not like it.
      PropertyType.Timestamp.Property(
        OffsetDateTime.ofInstant(x.toInstant, ZoneId.systemDefault()).toZonedDateTime)
    case x: ZonedDateTime =>
      PropertyType.Timestamp.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Timestamp.code, Some("timestamp")))
  }

  private def toDirectRelationProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.DirectRelation.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.DirectRelation.code, Some("string")))
  }

  private def toGeographyProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.Geography.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Geography.code, Some("string")))
  }

  private def toGeometryProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.Geometry.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Geometry.code, Some("string")))
  }

  private def toFloat32Property: Any => DataModelProperty[_] = {
    case x: Float => PropertyType.Float32.Property(x)
    case x: Int => PropertyType.Float32.Property(x.toFloat)
    case x: java.math.BigDecimal => PropertyType.Float32.Property(x.floatValue())
    case x: java.math.BigInteger => PropertyType.Float32.Property(x.floatValue())
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Float32.code, Some("float")))
  }

  private def toFloat64Property(propAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Double => PropertyType.Float64.Property(x)
    case x: Float => PropertyType.Float64.Property(x.toDouble)
    case x: Int => PropertyType.Float64.Property(x.toDouble)
    case x: Long => PropertyType.Float64.Property(x.toDouble)
    case x: java.math.BigDecimal => PropertyType.Float64.Property(x.doubleValue())
    case x: java.math.BigInteger => PropertyType.Float64.Property(x.doubleValue())
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propAlias.code, Some("double")))
  }

  private def toBooleanProperty: Any => DataModelProperty[_] = {
    case x: Boolean => PropertyType.Boolean.Property(x)
    case a =>
      throw new CdfSparkException(
        notValidPropertyTypeMessage(a, PropertyType.Boolean.code, Some("boolean")))
  }

  private def toInt32Property(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Int => PropertyType.Int.Property(x)
    case x: java.math.BigInteger => PropertyType.Int.Property(x.intValue())
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code, Some("int")))
  }

  private def toInt64Property(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Int => PropertyType.Bigint.Property(x.toLong)
    case x: Long => PropertyType.Bigint.Property(x)
    case x: java.math.BigInteger => PropertyType.Bigint.Property(x.longValue())
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code, Some("bigint")))
  }

  private def toStringProperty: Any => DataModelProperty[_] = {
    case x: String => PropertyType.Text.Property(x)
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, PropertyType.Text.code, Some("string")))
  }

  private def toStringArrayProperty: Any => DataModelProperty[_] = {
    case x: Iterable[_] if x.isEmpty => PropertyType.Array.Text.Property(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[String] =>
      PropertyType.Array.Text.Property(x.map(i => i.asInstanceOf[String]).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, PropertyType.Array.Text.code))
  }
  private def toFloat32ArrayProperty: Any => DataModelProperty[_] = {
    case x: Iterable[_] if x.isEmpty => PropertyType.Array.Float32.Property(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Float] =>
      PropertyType.Array.Float32.Property(x.map(i => i.asInstanceOf[Float]).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      PropertyType.Array.Float32.Property(x.map(i => i.asInstanceOf[Int].toFloat).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigDecimal] =>
      PropertyType.Array.Float32
        .Property(x.map(i => i.asInstanceOf[java.math.BigDecimal].floatValue()).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      PropertyType.Array.Float32
        .Property(x.map(i => i.asInstanceOf[java.math.BigInteger].floatValue()).toVector)
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, PropertyType.Array.Float32.code))
  }

  private def toFloat64ArrayProperty(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Iterable[_] if x.isEmpty => PropertyType.Array.Float64.Property(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Double] =>
      PropertyType.Array.Float64.Property(x.map(i => i.asInstanceOf[Double]).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Float] =>
      PropertyType.Array.Float64.Property(x.map(i => i.asInstanceOf[Float].toDouble).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Long] =>
      PropertyType.Array.Float64.Property(x.map(i => i.asInstanceOf[Long].toDouble).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      PropertyType.Array.Float64.Property(x.map(i => i.asInstanceOf[Int].toDouble).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigDecimal] =>
      PropertyType.Array.Float64
        .Property(x.map(i => i.asInstanceOf[java.math.BigDecimal].doubleValue()).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      PropertyType.Array.Float64
        .Property(x.map(i => i.asInstanceOf[java.math.BigInteger].doubleValue()).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code))
  }

  private def toBooleanArrayProperty: Any => DataModelProperty[_] = {
    case x: Iterable[_] if x.isEmpty => PropertyType.Array.Boolean.Property(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Boolean] =>
      PropertyType.Array.Boolean.Property(x.map(i => i.asInstanceOf[Boolean]).toVector)
    case a =>
      throw new CdfSparkException(notValidPropertyTypeMessage(a, PropertyType.Array.Boolean.code))
  }

  private def toInt32ArrayProperty(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Iterable[_] if x.isEmpty => PropertyType.Array.Int32.Property(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      PropertyType.Array.Int32.Property(x.map(i => i.asInstanceOf[Int]).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      PropertyType.Array.Int32
        .Property(x.map(i => i.asInstanceOf[java.math.BigInteger].intValue()).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code))
  }

  private def toInt64ArrayProperty(propertyAlias: PropertyType[_]): Any => DataModelProperty[_] = {
    case x: Iterable[_] if x.isEmpty => PropertyType.Array.Bigint.Property(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      PropertyType.Array.Bigint.Property(x.map(i => i.asInstanceOf[Int].toLong).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Long] =>
      PropertyType.Array.Bigint.Property(x.map(i => i.asInstanceOf[Long]).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      PropertyType.Array.Bigint
        .Property(x.map(i => i.asInstanceOf[java.math.BigInteger].longValue()).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias.code))
  }

  private def toPropertyType(propertyType: PropertyType[_]): Any => DataModelProperty[_] =
    propertyType match {
      case PropertyType.Float32 => toFloat32Property
      case PropertyType.Float64 | PropertyType.Numeric => toFloat64Property(propertyType)
      case PropertyType.Boolean => toBooleanProperty
      case PropertyType.Int32 | PropertyType.Int => toInt32Property(propertyType)
      case PropertyType.Int64 | PropertyType.Bigint => toInt64Property(propertyType)
      case PropertyType.Text => toStringProperty
      case PropertyType.Date => toDateProperty
      case PropertyType.Timestamp => toTimestampProperty
      case PropertyType.DirectRelation => toDirectRelationProperty
      case PropertyType.Geometry => toGeometryProperty
      case PropertyType.Geography => toGeographyProperty
      case PropertyType.Array.Text => toStringArrayProperty
      case PropertyType.Array.Float32 => toFloat32ArrayProperty
      case PropertyType.Array.Float64 | PropertyType.Array.Numeric =>
        toFloat64ArrayProperty(propertyType)
      case PropertyType.Array.Boolean => toBooleanArrayProperty
      case PropertyType.Array.Int | PropertyType.Array.Int32 => toInt32ArrayProperty(propertyType)
      case PropertyType.Array.Int64 | PropertyType.Array.Bigint => toInt64ArrayProperty(propertyType)
      case a =>
        throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }

}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
final case class DataModelInstanceDeleteSchema(externalId: String)

// scalastyle:on cyclomatic.complexity
