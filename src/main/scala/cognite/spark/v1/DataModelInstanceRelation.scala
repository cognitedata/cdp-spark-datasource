package cognite.spark.v1

import cats.Foldable
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstanceRelation._
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common.{CdpApiException, Items}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.circe.Json
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class DataModelInstanceRelation(config: RelationConfig, modelExternalId: String)(
    val sqlContext: SQLContext)
    extends CdfRelation(config, "mappinginstances")
    with WritableRelation {
  import CdpConnector._

  val mappingInfo: DataModelMapping = client.dataModelMappings
    .retrieveByExternalId(modelExternalId)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(s"Could not resolve schema of sequence $modelExternalId.", e)
    }
    .unsafeRunSync()

  val mappingPropertyStructFields: Seq[StructField] = mappingInfo.properties
    .map { props =>
      props.map {
        case (name, prop) =>
          StructField(
            name,
            propertyTypeToSparkType(prop.`type`),
            nullable = prop.nullable.getOrElse(DEFAULT_NULLABLE))
      }
    }
    .toList
    .flatten

  val propertyTypes: Map[String, (String, Boolean)] = mappingInfo.properties
    .map { props =>
      props.map {
        case (name, prop) =>
          (name, (prop.`type`, prop.nullable.getOrElse(DEFAULT_NULLABLE)))
      }
    }
    .getOrElse(Map())

  override def schema: StructType = new StructType(
    Array(StructField("externalId", DataTypes.StringType, nullable = false)) ++ mappingPropertyStructFields
  )

  override def upsert(rows: Seq[Row]): IO[Unit] =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val fromRowFn = fromRow(rows.head.schema)
      val dataModelInstances: Seq[DataModelInstance] = rows.map(fromRowFn)
      client.dataModelInstances.createItems(Items(dataModelInstances)) *> IO.unit
    }

  def toRow(a: ProjectedDataModelInstance): Row = ???

  def uniqueId(a: ProjectedDataModelInstance): String = a.externalId

  // scalastyle:off cyclomatic.complexity
  def getInstanceFilter(sparkFilter: Filter): Seq[DataModelInstanceFilter] =
    sparkFilter match {
      case EqualTo(left, right) if left != "externalId" =>
        Seq(DMIEqualsFilter(Seq(left), parseValue(right)))
      case In(attribute, values) if attribute != "externalId" =>
        val setValues = values.filter(_ != null)
        Seq(if (multiValuedTypes contains propertyTypes(attribute)._1) {
          DMIContainsAnyFilter(Seq(attribute), setValues.map(parseValue)) // Not sure
        } else {
          DMIInFilter(Seq(attribute), setValues.map(parseValue))
        })
      case GreaterThanOrEqual(attribute, value) =>
        Seq(DMIRangeFilter(Seq(attribute), gte = Some(parseValue(value))))
      case GreaterThan(attribute, value) =>
        Seq(DMIRangeFilter(Seq(attribute), gt = Some(parseValue(value))))
      case LessThanOrEqual(attribute, value) =>
        Seq(DMIRangeFilter(Seq(attribute), lte = Some(parseValue(value))))
      case LessThan(attribute, value) =>
        Seq(DMIRangeFilter(Seq(attribute), lt = Some(parseValue(value))))
      case StringStartsWith(attribute, value) if attribute != "externalId" =>
        Seq(DMIPrefixFilter(Seq(attribute), parseValue(value)))
      case And(f1, f2) =>
        val instancef1 = getInstanceFilter(f1)
        val instancef2 = getInstanceFilter(f2)
        Seq(DMIAndFilter(instancef1 ++ instancef2))
      case Or(f1, f2) =>
        val instancef1 = getInstanceFilter(f1)
        val instancef2 = getInstanceFilter(f2)
        Seq(DMIOrFilter(instancef1 ++ instancef2))
      case IsNotNull(attribute) =>
        Seq(DMIExistsFilter(Seq(attribute)))
      case Not(f) =>
        Seq(DMINotFilter(getInstanceFilter(f).head))
      case _ => Seq()
    }
  // scalastyle:on cyclomatic.complexity

  def toProjectedInstance(dmi: DataModelInstanceQueryResponse): ProjectedDataModelInstance =
    ProjectedDataModelInstance(
      externalId = dmi.externalId,
      properties = dmi.properties
        .map(_.map {
          case (name, value) =>
            parseFromJson(value, propertyTypes(name)._1)
        })
        .toArray
        .flatten)

  def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = {
    val dmiFilter = if (filters.isEmpty) {
      None
    } else {
      val andFilters = filters.toVector.flatMap(getInstanceFilter)
      if (andFilters.isEmpty) None else Some(DMIAndFilter(andFilters))
    }

    val dmiQuery = DataModelInstanceQuery(
      modelExternalId = modelExternalId,
      filter = dmiFilter,
      sort = None,
      limit = limit,
      cursor = None)
    val res = client.dataModelInstances.query(dmiQuery)
    val items: IO[Seq[DataModelInstanceQueryResponse]] = res.map(_.items)
    val nextCursor = res.map(_.nextCursor)
    items.map(_.map(toProjectedInstance))

  }

  def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  def update(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def delete(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Delete not supported for data model instances. Use upsert instead.")

  def fromRow(schema: StructType): Row => DataModelInstance = {
    val externalIdIndex = schema.fieldNames.indexOf("externalId")
    if (externalIdIndex < 0) {
      throw new CdfSparkException("Can't upsert data model instances, `externalId` is missing.")
    }

    val properties: Array[(Int, String, (String, Boolean))] = schema.fields.zipWithIndex
      .filter(props => !Seq("externalId").contains(props._1.name))
      .map {
        case (field: StructField, index: Int) =>
          val propertyType = propertyTypes.getOrElse(
            field.name,
            throw new CdfSparkException(s"Can't insert property `${field.name}` " +
              s"into data model mapping $modelExternalId, the property does not exist in the definition")
          )
          (index, field.name, propertyType)
      }

    def parseRow(row: Row): DataModelInstance = {
      val externalId = row.get(externalIdIndex) match {
        case x: String => x
        case _ => throw SparkSchemaHelperRuntime.badRowError(row, "externalId", "String", "")
      }

      val propertyValues: Map[String, Json] = properties.map {
        case (index, name, propType) =>
          val (propT, nullable) = propType
          name -> tryGetValue(propT, nullable).applyOrElse(
            row.get(index),
            (_: Any) =>
              throw SparkSchemaHelperRuntime
                .badRowError(row, name, propT, "")
          )
      }.toMap
      DataModelInstance(Some(modelExternalId), Some(externalId), properties = Some(propertyValues))
    }
    parseRow
  }
}

object DataModelInstanceRelation {
  val DEFAULT_NULLABLE = true
  val stringTypes = Seq(
    "text",
    "geometry",
    "geography",
    "direct_relation",
    "timestamp",
    "date"
  )
  val multiValuedTypes = Seq(
    "text[]",
    "boolean[]",
    "numeric[]",
    "float32[]",
    "float64[]",
    "int32[]",
    "int[]",
    "int64[]",
    "bigint[]"
  )

  // scalastyle:off cyclomatic.complexity
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

  private def parseFromJson(value: Json, propType: String): Any =
    if (value.isNull) {
      Some(null)
    } else {
      propType.toLowerCase match {
        case prop if stringTypes contains prop =>
          value.asString
        case "numeric" | "float64" | "float32" => value.asNumber.map(_.toDouble)
        case "int" | "int32" | "int64" | "bigint" => value.asNumber.flatMap(_.toLong)
        case "text[]" =>
          value.asArray.getOrElse(Vector()).flatMap(_.asString)
        case "boolean[]" =>
          value.asArray.getOrElse(Vector()).flatMap(_.asBoolean)
        case "numeric[]" | "float64[]" | "float32[]" =>
          value.asArray.getOrElse(Vector()).flatMap(_.asNumber.map(_.toDouble))
        case "int[]" | "int32[]" | "int64[]" | "bigint[]" =>
          value.asNumber.flatMap(_.toLong)
          value.asArray.getOrElse(Vector()).flatMap(_.asNumber.map(_.toLong))
        case a => throw new CdfSparkException(s"Unknown property type $a")
      }
    }

  def propertyTypeToSparkType(propertyType: String): DataType =
    propertyType.toLowerCase match {
      case complex if stringTypes contains complex =>
        DataTypes.StringType
      case "boolean" => DataTypes.BooleanType
      case "numeric" | "float64" => DataTypes.DoubleType
      case "float32" => DataTypes.FloatType
      case "float64" => DataTypes.DoubleType
      case "int32" | "int" => DataTypes.IntegerType
      case "int64" | "bigint" => DataTypes.LongType
      case propType if multiValuedTypes contains propType =>
        DataTypes.createArrayType(propertyTypeToSparkType(propType))
      case a => throw new CdfSparkException(s"Unknown property type $a")
    }

  private def jsonFromDouble(num: Double): Json =
    Json.fromDouble(num).getOrElse(throw new CdfSparkException(s"Numeric value $num"))
  private def tryGetValue(propertyType: String, nullable: Boolean): PartialFunction[Any, Json] = // scalastyle:off
    propertyType.toLowerCase match {
      case null if !nullable => // scalastyle:off null
        throw new CdfSparkException(s"Property is not nullable: $propertyType")
      case "float32" | "float64" | "numeric" => {
        case null => Json.Null // scalastyle:off null
        case x: Double => jsonFromDouble(x)
        case x: Int => jsonFromDouble(x.toDouble)
        case x: Float => jsonFromDouble(x.toDouble)
        case x: Long => jsonFromDouble(x.toDouble)
        case x: java.math.BigDecimal => jsonFromDouble(x.doubleValue)
        case x: java.math.BigInteger => jsonFromDouble(x.doubleValue)
      }
      case "boolean" => {
        case null => Json.Null // scalastyle:off null
        case x: Boolean => Json.fromBoolean(x)
      }
      case "int" | "int64" | "int32" | "bigint" => {
        case null => Json.Null // scalastyle:off null
        case x: Int => Json.fromInt(x)
        case x: Long => Json.fromLong(x)
        case x: java.math.BigInteger => Json.fromBigInt(x)
      }
      case complex if stringTypes contains complex => {
        case null => Json.Null // scalastyle:off null
        case x: String => Json.fromString(x)
      }
      case a =>
        throw new CdfSparkException(s"Unknown property type $a")
    }
  // scalastyle:on cyclomatic.complexity

}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
