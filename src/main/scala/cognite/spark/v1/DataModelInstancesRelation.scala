package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstanceRelation._
import com.cognite.sdk.scala.common.{CdpApiException, Items}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.circe.Json
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

  val modelInfo: DataModel = alphaClient.dataModels
    .retrieveByExternalIds(Seq(modelExternalId), true, false)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(s"Could not resolve schema of data model $modelExternalId.", e)
    }
    .unsafeRunSync()
    .head

  val modelPropertyStructFields: Seq[StructField] = modelInfo.properties
    .map { props =>
      props.map {
        case (name, prop) => {
          StructField(name, propertyTypeToSparkType(prop.`type`), nullable = prop.nullable)
        }
      }
    }
    .toList
    .flatten

  val propertyTypes: Map[String, (String, Boolean)] = modelInfo.properties
    .getOrElse(Map())
    .zipWithIndex
    .map {
      case ((name, prop), i: Int) =>
        (name, (prop.`type`, prop.nullable))
    }
    .toMap

  override def schema: StructType = new StructType(modelPropertyStructFields.toArray)

  override def upsert(rows: Seq[Row]): IO[Unit] =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val fromRowFn = fromRow(rows.head.schema)
      val dataModelInstances: Seq[DataModelInstance] = rows.map(fromRowFn)
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
        .flatMap(_.asString)
        .getOrElse(
          throw new CdfSparkException("Can't read data model instance, `externalId` is missing.")),
      properties = requiredPropsArray.map { name: String =>
        val value = dmiProperties.getOrElse(name, Json.Null)
        parseFromJson(value, propertyTypes(name)._1)
          .getOrElse(throw new CdfSparkException(s"Unexpected value $value in property $name"))

      }.toArray
    )
  }

  // scalastyle:off cyclomatic.complexity
  def getInstanceFilter(sparkFilter: Filter): Seq[DataModelInstanceFilter] =
    sparkFilter match {
      case EqualTo(left, right) => {
        Seq(DMIEqualsFilter(Seq(modelExternalId, left), parseValue(right)))
      }
      case In(attribute, values) =>
        if (Seq(
            "text[]",
            "boolean[]",
            "numeric[]",
            "float32[]",
            "float64[]",
            "int32[]",
            "int[]",
            "int64[]",
            "bigint[]") contains propertyTypes(attribute)._1) {
          Seq()
        } else {
          val setValues = values.filter(_ != null)
          Seq(DMIInFilter(Seq(modelExternalId, attribute), setValues.map(parseValue)))
        }
      case GreaterThanOrEqual(attribute, value) =>
        Seq(DMIRangeFilter(Seq(modelExternalId, attribute), gte = Some(parseValue(value))))
      case GreaterThan(attribute, value) =>
        Seq(DMIRangeFilter(Seq(modelExternalId, attribute), gt = Some(parseValue(value))))
      case LessThanOrEqual(attribute, value) =>
        Seq(DMIRangeFilter(Seq(modelExternalId, attribute), lte = Some(parseValue(value))))
      case LessThan(attribute, value) =>
        Seq(DMIRangeFilter(Seq(modelExternalId, attribute), lt = Some(parseValue(value))))
      case And(f1, f2) =>
        val instancef1 = getInstanceFilter(f1)
        val instancef2 = getInstanceFilter(f2)
        Seq(DMIAndFilter(instancef1 ++ instancef2))
      case Or(f1, f2) =>
        val instancef1 = getInstanceFilter(f1)
        val instancef2 = getInstanceFilter(f2)
        Seq(DMIOrFilter(instancef1 ++ instancef2))
      case IsNotNull(attribute) =>
        Seq(DMIExistsFilter(Seq(modelExternalId, attribute)))
      case Not(f) =>
        Seq(DMINotFilter(getInstanceFilter(f).head))
      case _ => Seq()
    }
  // scalastyle:on cyclomatic.complexity

  def getStreams(filters: Array[Filter], requiredColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = {
    val requiredPropsArray: Seq[String] = if (requiredColumns.isEmpty) {
      modelPropertyStructFields.map(_.name)
    } else {
      requiredColumns
    }

    val filter = if (filters.isEmpty) {
      None
    } else {
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

  def fromRow(schema: StructType): Row => DataModelInstance = {
    val externalIdIndex = schema.fieldNames.indexOf("externalId")
    if (externalIdIndex < 0) {
      throw new CdfSparkException("Can't upsert data model instances, `externalId` is missing.")
    }

    val properties: Array[(Int, String, (String, Boolean))] = schema.fields.zipWithIndex
      .map {
        case (field: StructField, index: Int) =>
          val propertyType = propertyTypes.getOrElse(
            field.name,
            throw new CdfSparkException(
              s"Can't insert property `${field.name}` " +
                s"into data model $modelExternalId, the property does not exist in the definition")
          )
          (index, field.name, propertyType)
      }

    def parseRow(row: Row): DataModelInstance = {
      val _ = row.get(externalIdIndex) match {
        case x: String => x
        case _ => throw SparkSchemaHelperRuntime.badRowError(row, "externalId", "String", "")
      }
      val propertyValues: Map[String, Json] = properties.map {
        case (index, name, propT) =>
          name -> tryGetValue(propT._1, propT._2).applyOrElse(
            row.get(index),
            (_: Any) =>
              throw SparkSchemaHelperRuntime
                .badRowError(row, name, propT._1, "")
          )
      }.toMap
      DataModelInstance(modelExternalId, properties = Some(propertyValues))
    }
    parseRow
  }
}

object DataModelInstanceRelation {
  private def propertyNotNullableMessage(propertyType: String) =
    s"Property of $propertyType type is not nullable."
  private def unknownPropertyType(a: Any) = s"Unknown property type $a."

  //scalastyle:off cyclomatic.complexity
  private def parseValue(value: Any): Json = value match {
    case x: Double => jsonFromDouble(x)
    case x: Int => jsonFromDouble(x.toDouble)
    case x: Float => jsonFromDouble(x.toDouble)
    case x: Long => jsonFromDouble(x.toDouble)
    case x: java.math.BigDecimal => jsonFromDouble(x.doubleValue)
    case x: java.math.BigInteger => jsonFromDouble(x.doubleValue)
    case x: String => Json.fromString(x)
    case x: Boolean => Json.fromBoolean(x)
    case x: Array[Any] =>
      Json.fromValues(x.map(parseValue))
    case null => Json.Null // scalastyle:off null
  }

  private def parseFromJson(value: Json, propType: String): Option[Any] =
    if (value.isNull) {
      Some(null) // scalastyle:off null
    } else {
      propType.toLowerCase match {
        case "text" =>
          value.asString
        case "boolean" =>
          value.asBoolean
        case "numeric" | "float64" =>
          value.asNumber.map(_.toDouble)
        case "float32" =>
          value.asNumber.map(_.toFloat)
        case "int" | "int32" =>
          value.asNumber.flatMap(_.toInt)
        case "int64" | "bigint" =>
          value.asNumber.flatMap(_.toLong)
        case "text[]" =>
          Some(value.asArray.getOrElse(Vector()).flatMap(_.asString))
        case "boolean[]" =>
          Some(value.asArray.getOrElse(Vector()).flatMap(_.asBoolean))
        case "numeric[]" | "float64[]" =>
          Some(value.asArray.getOrElse(Vector()).flatMap(_.asNumber.map(_.toDouble)))
        case "float32[]" =>
          Some(value.asArray.getOrElse(Vector()).flatMap(_.asNumber.map(_.toFloat)))
        case "int[]" | "int32[]" =>
          Some(value.asArray.getOrElse(Vector()).flatMap(_.asNumber.flatMap(_.toInt).toList))
        case "int64[]" | "bigint[]" =>
          Some(value.asArray.getOrElse(Vector()).flatMap(_.asNumber.flatMap(_.toLong).toList))
        case a => throw new CdfSparkException(unknownPropertyType(a))
      }
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
      case a => throw new CdfSparkException(unknownPropertyType(a))
    }

  private def jsonFromDouble(num: Double): Json =
    Json.fromDouble(num).getOrElse(throw new CdfSparkException(s"Numeric value $num"))
  private def tryGetValue(propertyType: String, nullable: Boolean): PartialFunction[Any, Json] = // scalastyle:off
    propertyType.toLowerCase match {
      case "float32" | "float64" | "numeric" => {
        case null if !nullable => throw new CdfSparkException(propertyNotNullableMessage(propertyType))
        case null => Json.Null // scalastyle:off null
        case x: Double => jsonFromDouble(x)
        case x: Int => jsonFromDouble(x.toDouble)
        case x: Float => jsonFromDouble(x.toDouble)
        case x: Long => jsonFromDouble(x.toDouble)
        case x: java.math.BigDecimal => jsonFromDouble(x.doubleValue)
        case x: java.math.BigInteger => jsonFromDouble(x.doubleValue)
      }
      case "boolean" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: Boolean => Json.fromBoolean(x)
      }
      case "int" | "int64" | "int32" | "bigint" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: Int => Json.fromInt(x)
        case x: Long => Json.fromLong(x)
        case x: java.math.BigInteger => Json.fromBigInt(x)
      }
      case "text" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: String => Json.fromString(x)
      }
      case "text[]" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: Iterable[_] if x.isEmpty => Json.arr()
        case x: Iterable[String] @unchecked if x.head.isInstanceOf[String] =>
          Json.fromValues(x.map(Json.fromString))
      }
      case "float32[]" | "float64[]" | "numeric[]" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: Iterable[_] if x.isEmpty => Json.arr()
        case x: Iterable[Double] @unchecked if x.head.isInstanceOf[Double] =>
          Json.fromValues(x.map(value => jsonFromDouble(value.doubleValue)))
        case x: Iterable[Int] @unchecked if x.head.isInstanceOf[Int] =>
          Json.fromValues(x.map(value => jsonFromDouble(value.doubleValue)))
        case x: Iterable[Float] @unchecked if x.head.isInstanceOf[Float] =>
          Json.fromValues(x.map(value => jsonFromDouble(value.doubleValue)))
        case x: Iterable[Long] @unchecked if x.head.isInstanceOf[Long] =>
          Json.fromValues(x.map(value => jsonFromDouble(value.doubleValue)))
        case x: Iterable[java.math.BigDecimal] @unchecked if x.head.isInstanceOf[java.math.BigDecimal] =>
          Json.fromValues(x.map(value => jsonFromDouble(value.doubleValue)))
        case x: Iterable[java.math.BigInteger] @unchecked if x.head.isInstanceOf[java.math.BigInteger] =>
          Json.fromValues(x.map(value => jsonFromDouble(value.doubleValue)))
      }
      case "boolean[]" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: Iterable[_] if x.isEmpty => Json.arr()
        case x: Iterable[Boolean] @unchecked if x.head.isInstanceOf[Boolean] =>
          Json.fromValues(x.map(Json.fromBoolean))
      }
      case "int[]" | "int64[]" | "int32[]" | "bigint[]" => {
        case null if !nullable =>
          throw new CdfSparkException(propertyNotNullableMessage(propertyType)) // scalastyle:off null
        case null => Json.Null // scalastyle:off null
        case x: Iterable[_] if x.isEmpty => Json.arr()
        case x: Iterable[Int] @unchecked if x.head.isInstanceOf[Int] =>
          Json.fromValues(x.map(Json.fromInt))
        case x: Iterable[Long] @unchecked if x.head.isInstanceOf[Long] =>
          Json.fromValues(x.map(Json.fromLong))
        case x: Iterable[java.math.BigInteger] @unchecked if x.head.isInstanceOf[java.math.BigInteger] =>
          Json.fromValues(x.map(value => Json.fromLong(value.doubleValue.toLong)))
      }
      case a =>
        throw new CdfSparkException(unknownPropertyType(a))
    }
  // scalastyle:on cyclomatic.complexity

}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
final case class DataModelInstanceDeleteSchema(externalId: String)
