package cognite.spark.v1

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
        value.map(fromProperty).orNull
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

  def getStreams(filters: Array[Filter], requiredColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = {
    val requiredPropsArray: Seq[String] = if (requiredColumns.isEmpty) {
      schema.fields.map(_.name)
    } else {
      requiredColumns
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
        .map(r => toProjectedInstance(r, requiredPropsArray)))
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedDataModelInstance, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedDataModelInstance, _) => toRow(item),
      uniqueId,
      getStreams(filters, requiredColumns)
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
                Some(
                  toPropertyType(propT.`type`).applyOrElse(
                    row.get(index),
                    (_: Any) =>
                      throw SparkSchemaHelperRuntime
                        .badRowError(row, name, propT.`type`, "")
                  ))
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
    case x => throw new CdfSparkException(s"Cannot parse the value with udentified type: $x")
  }

  private def fromProperty(x: PropertyType): Any = x match {
    case Int32Property(value) => value
    case Int64Property(value) => value
    case Float32Property(value) => value
    case Float64Property(value) => value
    case BooleanProperty(value) => value
    case StringProperty(value) => value
    case ArrayProperty(values) => values.map(fromProperty)
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
      case "text[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "boolean[]" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "numeric[]" | "float64[]" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "float32[]" => DataTypes.createArrayType(DataTypes.FloatType)
      case "int32[]" | "int[]" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "int64[]" | "bigint[]" => DataTypes.createArrayType(DataTypes.LongType)
      case a => throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }

  private def toPropertyType(propertyType: String): PartialFunction[Any, PropertyType] = // scalastyle:off
    propertyType.toLowerCase match {
      case "float32" => {
        case x: Float => Float32Property(x)
        case x: Double => Float32Property(x.toFloat)
        case x: Int => Float32Property(x.toFloat)
        case x: Long => Float32Property(x.toFloat)
        case x: java.math.BigDecimal => Float32Property(x.floatValue())
        case x: java.math.BigInteger => Float32Property(x.floatValue())
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "float64" | "numeric" => {
        case x: Double => Float64Property(x)
        case x: Float => Float64Property(x.toDouble)
        case x: Int => Float64Property(x.toDouble)
        case x: Long => Float64Property(x.toDouble)
        case x: java.math.BigDecimal => Float64Property(x.doubleValue())
        case x: java.math.BigInteger => Float64Property(x.doubleValue())
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "boolean" => {
        case x: Boolean => BooleanProperty(x)
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "int" | "int32" => {
        case x: Int => Int32Property(x)
        case x: java.math.BigInteger => Int32Property(x.longValue().toInt)
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "int64" | "bigint" => {
        case x: Int => Int64Property(x.toLong)
        case x: Long => Int64Property(x)
        case x: java.math.BigInteger => Int64Property(x.longValue())
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "text" => {
        case x: String => StringProperty(x)
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "text[]" => {
        case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
        case x: Iterable[String] @unchecked if x.head.isInstanceOf[String] =>
          ArrayProperty(x.toVector.map(StringProperty))
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "float32[]" => {
        case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
        case x: Iterable[Float] @unchecked if x.head.isInstanceOf[Float] =>
          ArrayProperty(x.toVector.map(Float32Property))
        case x: Iterable[Int] @unchecked if x.head.isInstanceOf[Int] =>
          ArrayProperty(x.toVector.map(i => Float32Property(i.toFloat)))
        case x: Iterable[Long] @unchecked if x.head.isInstanceOf[Long] =>
          ArrayProperty(x.toVector.map(i => Float32Property(i.toFloat)))
        case x: Iterable[java.math.BigDecimal] @unchecked if x.head.isInstanceOf[java.math.BigDecimal] =>
          ArrayProperty(x.toVector.map(i => Float32Property(i.floatValue())))
        case x: Iterable[java.math.BigInteger] @unchecked if x.head.isInstanceOf[java.math.BigInteger] =>
          ArrayProperty(x.toVector.map(i => Float32Property(i.floatValue())))
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "float64[]" | "numeric[]" => {
        case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
        case x: Iterable[Double] @unchecked if x.head.isInstanceOf[Double] =>
          ArrayProperty(x.toVector.map(Float64Property))
        case x: Iterable[Float] @unchecked if x.head.isInstanceOf[Float] =>
          ArrayProperty(x.toVector.map(f => Float64Property(f.toDouble)))
        case x: Iterable[Int] @unchecked if x.head.isInstanceOf[Int] =>
          ArrayProperty(x.toVector.map(f => Float64Property(f.toDouble)))
        case x: Iterable[Long] @unchecked if x.head.isInstanceOf[Long] =>
          ArrayProperty(x.toVector.map(f => Float64Property(f.toDouble)))
        case x: Iterable[java.math.BigDecimal] @unchecked if x.head.isInstanceOf[java.math.BigDecimal] =>
          ArrayProperty(x.toVector.map(i => Float64Property(i.doubleValue())))
        case x: Iterable[java.math.BigInteger] @unchecked if x.head.isInstanceOf[java.math.BigInteger] =>
          ArrayProperty(x.toVector.map(i => Float64Property(i.doubleValue())))
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "boolean[]" => {
        case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
        case x: Iterable[Boolean] @unchecked if x.head.isInstanceOf[Boolean] =>
          ArrayProperty(x.toVector.map(BooleanProperty))
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "int[]" | "int32[]" => {
        case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
        case x: Iterable[Int] @unchecked if x.head.isInstanceOf[Int] =>
          ArrayProperty(x.toVector.map(Int32Property))
        case x: Iterable[java.math.BigInteger] @unchecked if x.head.isInstanceOf[java.math.BigInteger] =>
          ArrayProperty(x.toVector.map(i => Int32Property(i.intValue())))
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case "int64[]" | "bigint[]" => {
        case x: Iterable[_] if x.isEmpty => ArrayProperty(Vector.empty)
        case x: Iterable[Int] @unchecked if x.head.isInstanceOf[Int] =>
          ArrayProperty(x.toVector.map(i => Int64Property(i.toLong)))
        case x: Iterable[Long] @unchecked if x.head.isInstanceOf[Long] =>
          ArrayProperty(x.toVector.map(Int64Property))
        case x: Iterable[java.math.BigInteger] @unchecked if x.head.isInstanceOf[java.math.BigInteger] =>
          ArrayProperty(x.toVector.map(i => Int64Property(i.longValue())))
        case a => throw new CdfSparkException(notValidPropertyTypeMessage(a, propertyType))
      }
      case a =>
        throw new CdfSparkException(unknownPropertyTypeMessage(a))
    }
  // scalastyle:on cyclomatic.complexity

}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
final case class DataModelInstanceDeleteSchema(externalId: String)
