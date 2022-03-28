package cognite.spark.v1

import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstanceRelation._
import com.cognite.sdk.scala.common.{CdpApiException, Items}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class DataModelInstanceRelation(config: RelationConfig, modelExternalId: String)(
    val sqlContext: SQLContext)
    extends CdfRelation(config, "datamodelinstances")
    with WritableRelation
    with PrunedFilteredScan {
  import CdpConnector._

  val modelInfo: Map[String, DataModelProperty] = alphaClient.dataModels
    .retrieveByExternalIds(Seq(modelExternalId), true, false)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(s"Could not resolve schema of data model $modelExternalId.", e)
    }
    .unsafeRunSync()
    .head
    .properties
    .getOrElse(Map())

  override def schema: StructType = new StructType(
    modelInfo.map {
      case (name, prop) =>
        StructField(name, propertyTypeToSparkType(prop.`type`), nullable = prop.nullable)
    }.toArray
  )

  override def upsert(rows: Seq[Row]): IO[Unit] =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val fromRowFn = fromRow(rows.head.schema)
      val dataModelInstances: Seq[DataModelInstanceCreate] = rows.map(fromRowFn)
      alphaClient.dataModelInstances
        .createItems(Items(dataModelInstances))
        .flatTap(_ => incMetrics(itemsUpserted, dataModelInstances.length)) *> IO.unit
    }

  def toRow(a: ProjectedDataModelInstance): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    Row.fromSeq(a.properties)
  }

  def uniqueId(a: ProjectedDataModelInstance): String = a.externalId

  def toProjectedInstance(
      dmi: DataModelInstanceQueryResponse,
      requiredPropsArray: Seq[String]): ProjectedDataModelInstance = {
    val dmiProperties = dmi.properties.getOrElse(Map())
    ProjectedDataModelInstance(
      externalId = dmi.properties
        .flatMap(_.get("externalId"))
        .map(_.asInstanceOf[StringProperty].value)
        .getOrElse(
          throw new CdfSparkException("Can't read data model instance, `externalId` is missing.")),
      properties = requiredPropsArray.map { name: String =>
        val value = dmiProperties.get(name)
        value.map(fromPropertyType).orNull
      }.toArray
    )
  }

  // scalastyle:off cyclomatic.complexity
  def getInstanceFilter(sparkFilter: Filter): Option[DataModelInstanceFilter] =
    sparkFilter match {
      case EqualTo(left, right) => {
        Some(DMIEqualsFilter(Seq(modelExternalId, left), parsePropertyValue(right)))
      }
      case In(attribute, values) =>
        if (modelInfo(attribute).`type`.endsWith("[]")) {
          None
        } else {
          val setValues = values.filter(_ != null)
          Some(DMIInFilter(Seq(modelExternalId, attribute), setValues.map(parsePropertyValue)))
        }
      case StringStartsWith(attribute, value) =>
        Some(DMIPrefixFilter(Seq(modelExternalId, attribute), parsePropertyValue(value)))
      case GreaterThanOrEqual(attribute, value) =>
        Some(DMIRangeFilter(Seq(modelExternalId, attribute), gte = Some(parsePropertyValue(value))))
      case GreaterThan(attribute, value) =>
        Some(DMIRangeFilter(Seq(modelExternalId, attribute), gt = Some(parsePropertyValue(value))))
      case LessThanOrEqual(attribute, value) =>
        Some(DMIRangeFilter(Seq(modelExternalId, attribute), lte = Some(parsePropertyValue(value))))
      case LessThan(attribute, value) =>
        Some(DMIRangeFilter(Seq(modelExternalId, attribute), lt = Some(parsePropertyValue(value))))
      case And(f1, f2) =>
        (getInstanceFilter(f1) ++ getInstanceFilter(f2)).reduceLeftOption((sf1, sf2) =>
          DMIAndFilter(Seq(sf1, sf2)))
      case Or(f1, f2) =>
        (getInstanceFilter(f1), getInstanceFilter(f2)) match {
          case (Some(sf1), Some(sf2)) =>
            Some(DMIOrFilter(Seq(sf1, sf2)))
          case _ =>
            None
        }
      case IsNotNull(attribute) =>
        Some(DMIExistsFilter(Seq(modelExternalId, attribute)))
      case Not(f) =>
        getInstanceFilter(f).map(DMINotFilter)
      case _ => None
    }
  // scalastyle:on cyclomatic.complexity

  def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = {
    val selectedPropsArray: Seq[String] = if (selectedColumns.isEmpty) {
      schema.fields.map(_.name)
    } else {
      selectedColumns
    }

    val filter = {
      val andFilters = filters.toVector.flatMap(getInstanceFilter)
      if (andFilters.isEmpty) None else Some(DMIAndFilter(andFilters))
    }

    val dmiQuery = DataModelInstanceQuery(
      modelExternalId = modelExternalId,
      filter = filter,
      sort = None,
      limit = limit,
      cursor = None)
    Seq(
      alphaClient.dataModelInstances
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

  def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert is not supported for data model instances. Use upsert instead.")

  def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for data model instances. Use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[DataModelInstanceDeleteSchema](r))
    alphaClient.dataModelInstances
      .deleteByExternalIds(deletes.map(_.externalId))
      .flatTap(_ => incMetrics(itemsDeleted, rows.length))
  }

  def fromRow(schema: StructType): Row => DataModelInstanceCreate = {
    val externalIdIndex = schema.fieldNames.indexOf("externalId")
    if (externalIdIndex < 0) {
      throw new CdfSparkException("Can't upsert data model instances, `externalId` is missing.")
    }

    val indexedPropertyList: Array[(Int, String, DataModelProperty)] = schema.fields.zipWithIndex
      .map {
        case (field: StructField, index: Int) =>
          val propertyType = modelInfo.getOrElse(
            field.name,
            throw new CdfSparkException(
              s"Can't insert property `${field.name}` " +
                s"into data model $modelExternalId, the property does not exist in the definition")
          )
          (index, field.name, propertyType)
      }
    def parseRow(indexedPropertyList: Array[(Int, String, DataModelProperty)])(
        row: Row): DataModelInstanceCreate = {
      row.get(externalIdIndex) match {
        case x: String => x
        case _ =>
          throw SparkSchemaHelperRuntime.badRowError(row, "externalId", "String", "")
      }
      val propertyValues: Map[String, PropertyType] = indexedPropertyList
        .map {
          case (index, name, propT) =>
            name -> (row.get(index) match {
              case null if !propT.nullable => // scalastyle:off null
                throw new CdfSparkException(propertyNotNullableMessage(propT.`type`))
              case null => // scalastyle:off null
                None
              case _ =>
                Some(toPropertyType(propT.`type`)(row.get(index)))
            })
        }
        .collect { case (a, Some(value)) => a -> value }
        .toMap

      DataModelInstanceCreate(modelExternalId, properties = Some(propertyValues))
    }
    parseRow(indexedPropertyList)
  }
}

object DataModelInstanceRelation {
  private def unknownPropertyTypeMessage(a: Any) = s"Unknown property type $a."

  private def notValidPropertyTypeMessage(a: Any, propertyType: String) =
    s"$a is not a valid $propertyType"

  private def propertyNotNullableMessage(propertyType: String) =
    s"Property of $propertyType type is not nullable."

  //scalastyle:off cyclomatic.complexity
  private def parsePropertyValue(value: Any): PropertyType = value match {
    case x: Double => Float64Property(x)
    case x: Int => Int32Property(x)
    case x: Float => Float32Property(x)
    case x: Long => Int64Property(x)
    case x: java.math.BigDecimal => Float64Property(x.doubleValue())
    case x: java.math.BigInteger => Int64Property(x.longValue())
    case x: String => StringProperty(x)
    case x: Boolean => BooleanProperty(x)
    case x: Array[Double] => ArrayProperty(x.toVector.map(Float64Property))
    case x: Array[Int] => ArrayProperty(x.toVector.map(Int32Property))
    case x: Array[Float] => ArrayProperty(x.toVector.map(Float32Property))
    case x: Array[Long] => ArrayProperty(x.toVector.map(Int64Property))
    case x: Array[String] => ArrayProperty(x.toVector.map(StringProperty))
    case x: Array[Boolean] => ArrayProperty(x.toVector.map(BooleanProperty))
    case x: Array[java.math.BigDecimal] =>
      ArrayProperty(x.toVector.map(i => Float64Property(i.doubleValue())))
    case x: Array[java.math.BigInteger] =>
      ArrayProperty(x.toVector.map(i => Int64Property(i.longValue())))
    case x: LocalDate => DateProperty(x)
    case x: java.sql.Date => DateProperty(x.toLocalDate)
    case x: LocalDateTime => DateProperty(x.toLocalDate)
    case x: Instant =>
      TimeStampProperty(OffsetDateTime.ofInstant(x, ZoneId.systemDefault()).toZonedDateTime)
    case x: java.sql.Timestamp =>
      TimeStampProperty(OffsetDateTime.ofInstant(x.toInstant, ZoneId.systemDefault()).toZonedDateTime)
    case x: java.time.ZonedDateTime =>
      TimeStampProperty(x)
    case x => throw new CdfSparkException(s"Cannot parse the value with udentified type: $x")
  }

  private def fromPropertyType(x: PropertyType): Any = x match {
    case Int32Property(value) => value
    case Int64Property(value) => value
    case Float32Property(value) => value
    case Float64Property(value) => value
    case BooleanProperty(value) => value
    case StringProperty(value) => value
    case DateProperty(value) => java.sql.Date.valueOf(value)
    case TimeStampProperty(value) => java.sql.Timestamp.from(value.toInstant)
    case DirectRelationProperty(value) => value
    case GeographyProperty(value) => value
    case GeometryProperty(value) => value
    case ArrayProperty(values) => values.map(fromPropertyType)
    case x => throw new CdfSparkException(s"Unknown property type with value $x")
  }

  def propertyTypeToSparkType(propertyType: String): DataType =
    propertyType.toLowerCase match {
      case "text" =>
        DataTypes.StringType
      case "boolean" => DataTypes.BooleanType
      case "numeric" | "float64" => DataTypes.DoubleType
      case "float32" => DataTypes.FloatType
      case "int32" | "int" => DataTypes.IntegerType
      case "int64" | "bigint" => DataTypes.LongType
      case "date" => DataTypes.DateType
      case "timestamp" => DataTypes.TimestampType
      case "direct_relation" => DataTypes.StringType
      case "geometry" => DataTypes.StringType
      case "geography" => DataTypes.StringType
      case "text[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "boolean[]" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "numeric[]" | "float64[]" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "float32[]" => DataTypes.createArrayType(DataTypes.FloatType)
      case "int32[]" | "int[]" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "int64[]" | "bigint[]" => DataTypes.createArrayType(DataTypes.LongType)
      case a => throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }

  private def toDateProperty: Any => DateProperty = {
    case x: LocalDate => DateProperty(x)
    case x: java.sql.Date => DateProperty(x.toLocalDate)
    case x: LocalDateTime => DateProperty(x.toLocalDate)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "date"))
  }

  private def toTimestampProperty: Any => TimeStampProperty = {
    case x: Instant =>
      TimeStampProperty(OffsetDateTime.ofInstant(x, ZoneId.systemDefault()).toZonedDateTime)
    case x: java.sql.Timestamp =>
      // Using ZonedDateTime directly without OffSetDateTime, adds ZoneId to the end of encoded string
      // 2019-10-02T07:00+09:00[Asia/Seoul] instead of 2019-10-02T07:00+09:00, API does not like it.
      TimeStampProperty(OffsetDateTime.ofInstant(x.toInstant, ZoneId.systemDefault()).toZonedDateTime)
    case x: ZonedDateTime =>
      TimeStampProperty(x)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "timestamp"))
  }

  private def toDirectRelationProperty: Any => DirectRelationProperty = {
    case x: String => DirectRelationProperty(x)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "direct_relation"))
  }

  private def toGeographyProperty: Any => GeographyProperty = {
    case x: String => GeographyProperty(x)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "geography"))
  }

  private def toGeometryProperty: Any => GeometryProperty = {
    case x: String => GeometryProperty(x)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "geometry"))
  }

  private def toFloat32Property: Any => Float32Property = {
    case x: Float => Float32Property(x)
    case x: Int => Float32Property(x.toFloat)
    case x: java.math.BigDecimal => Float32Property(x.floatValue())
    case x: java.math.BigInteger => Float32Property(x.floatValue())
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "float32"))
  }

  private def toFloat64Property(propAlias: String): Any => Float64Property = {
    case x: Double => Float64Property(x)
    case x: Float => Float64Property(x.toDouble)
    case x: Int => Float64Property(x.toDouble)
    case x: Long => Float64Property(x.toDouble)
    case x: java.math.BigDecimal => Float64Property(x.doubleValue())
    case x: java.math.BigInteger => Float64Property(x.doubleValue())
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propAlias))
  }

  private def toBooleanProperty: Any => BooleanProperty = {
    case x: Boolean => BooleanProperty(x)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "boolean"))
  }

  private def toInt32Property(propertyAlias: String): Any => Int32Property = {
    case x: Int => Int32Property(x)
    case x: java.math.BigInteger => Int32Property(x.intValue())
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias))
  }

  private def toInt64Property(propertyAlias: String): PartialFunction[Any, Int64Property] = {
    case x: Int => Int64Property(x.toLong)
    case x: Long => Int64Property(x)
    case x: java.math.BigInteger => Int64Property(x.longValue())
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias))
  }

  private def toStringProperty: Any => StringProperty = {
    case x: String => StringProperty(x)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "text"))
  }

  private def toStringArrayProperty: Any => ArrayProperty[StringProperty] = {
    case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[String] =>
      ArrayProperty(x.map(i => StringProperty(i.asInstanceOf[String])).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "text[]"))
  }
  private def toFloat32ArrayProperty: Any => ArrayProperty[Float32Property] = {
    case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Float] =>
      ArrayProperty(x.map(i => Float32Property(i.asInstanceOf[Float])).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      ArrayProperty(x.map(i => Float32Property(i.asInstanceOf[Int].toFloat)).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigDecimal] =>
      ArrayProperty(
        x.map(i => Float32Property(i.asInstanceOf[java.math.BigDecimal].floatValue())).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      ArrayProperty(
        x.map(i => Float32Property(i.asInstanceOf[java.math.BigInteger].floatValue())).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "float32[]"))
  }

  private def toFloat64ArrayProperty(propertyAlias: String): Any => ArrayProperty[Float64Property] = {
    case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Double] =>
      ArrayProperty(x.map(i => Float64Property(i.asInstanceOf[Double])).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Float] =>
      ArrayProperty(x.map(i => Float64Property(i.asInstanceOf[Float].toDouble)).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Long] =>
      ArrayProperty(x.map(i => Float64Property(i.asInstanceOf[Long].toDouble)).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      ArrayProperty(x.map(i => Float64Property(i.asInstanceOf[Int].toDouble)).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigDecimal] =>
      ArrayProperty(
        x.map(i => Float64Property(i.asInstanceOf[java.math.BigDecimal].doubleValue())).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      ArrayProperty(
        x.map(i => Float64Property(i.asInstanceOf[java.math.BigInteger].doubleValue())).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias))
  }

  private def toBooleanArrayProperty: Any => ArrayProperty[BooleanProperty] = {
    case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Boolean] =>
      ArrayProperty(x.map(i => BooleanProperty(i.asInstanceOf[Boolean])).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, "boolean[]"))
  }

  private def toInt32ArrayProperty(propertyAlias: String): Any => ArrayProperty[Int32Property] = {
    case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      ArrayProperty(x.map(i => Int32Property(i.asInstanceOf[Int])).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      ArrayProperty(x.map(i => Int32Property(i.asInstanceOf[java.math.BigInteger].intValue())).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias))
  }

  private def toInt64ArrayProperty(propertyAlias: String): Any => ArrayProperty[Int64Property] = {
    case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
    case x: Iterable[_] if x.head.isInstanceOf[Int] =>
      ArrayProperty(x.map(i => Int64Property(i.asInstanceOf[Int].toLong)).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[Long] =>
      ArrayProperty(x.map(i => Int64Property(i.asInstanceOf[Long])).toVector)
    case x: Iterable[_] if x.head.isInstanceOf[java.math.BigInteger] =>
      ArrayProperty(x.map(i => Int64Property(i.asInstanceOf[java.math.BigInteger].longValue())).toVector)
    case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyAlias))
  }

  private def toPropertyType(propertyType: String): Any => PropertyType =
    propertyType.toLowerCase match {
      case "float32" => toFloat32Property
      case "float64" | "numeric" => toFloat64Property(propertyType)
      case "boolean" => toBooleanProperty
      case "int" | "int32" => toInt32Property(propertyType)
      case "int64" | "bigint" => toInt64Property(propertyType)
      case "text" => toStringProperty
      case "date" => toDateProperty
      case "timestamp" => toTimestampProperty
      case "direct_relation" => toDirectRelationProperty
      case "geometry" => toGeometryProperty
      case "geography" => toGeographyProperty
      case "text[]" => toStringArrayProperty
      case "float32[]" => toFloat32ArrayProperty
      case "float64[]" | "numeric[]" => toFloat64ArrayProperty(propertyType)
      case "boolean[]" => toBooleanArrayProperty
      case "int[]" | "int32[]" => toInt32ArrayProperty(propertyType)
      case "int64[]" | "bigint[]" => toInt64ArrayProperty(propertyType)
      case a =>
        throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }
  //scalastyle:on cyclomatic.complexity

}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
final case class DataModelInstanceDeleteSchema(externalId: String)
