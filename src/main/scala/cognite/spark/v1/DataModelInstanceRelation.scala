package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstanceRelation.{propertyTypeToSparkType, tryGetValue}
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.circe.Json
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class DataModelInstanceRelation(config: RelationConfig, modelExternalId: String)(
    val sqlContext: SQLContext)
    extends CdfRelation(config, "mapping")
    with WritableRelation {
  import CdpConnector._

  val mappingInfo: DataModelMapping = client.dataModelMappings
    .retrieveByExternalId(modelExternalId)
    .adaptError {
      case e: CdpApiException =>
        new CdfSparkException(s"Could not resolve schema of sequence $modelExternalId.", e)
    }
    .unsafeRunSync()

  val mappingTypes: Seq[StructField] = mappingInfo.properties
    .map { props =>
      props.map {
        case (name, prop) =>
          StructField(
            name,
            propertyTypeToSparkType(prop.`type`),
            nullable = prop.nullable.getOrElse(true))
      }
    }
    .toList
    .flatten

  val columnTypes: Map[String, String] = mappingInfo.properties
    .map { props =>
      props.map {
        case (name, prop) =>
          (name, prop.`type`)
      }
    }
    .getOrElse(Map())

  override def schema: StructType = new StructType(
    Array(StructField("externalId", DataTypes.StringType, nullable = false)) ++ mappingTypes
  )

  override def upsert(rows: Seq[Row]): IO[Unit] = ???

  def toRow(a: ProjectedDataModelInstance): Row = ???

  def uniqueId(a: ProjectedDataModelInstance): String = a.externalId

  def getStreams(filters: Array[Filter])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedDataModelInstance]] = ???

  def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert not supported for data model instances. Use upsert instead.")

  def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update not supported for data model instances. Use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Delete not supported for data model instances. Use upsert instead.")

  def fromRow(schema: StructType): (Array[String], Row => DataModelInstance) = {
    val externalIdIndex = schema.fieldNames.indexOf("externalId")
    if (externalIdIndex < 0) {
      throw new CdfSparkException("Can't upsert data model instances, column `externalId` is missing.")
    }

    val columns: Array[(Int, String, String)] = schema.fields.zipWithIndex
      .filter(cols => !Seq("externalId").contains(cols._1.name))
      .map {
        case (field: StructField, index: Int) =>
          val columnType = columnTypes.getOrElse(
            field.name,
            throw new CdfSparkException(s"Can't insert column `${field.name}` " +
              s"into data model mapping $modelExternalId, the property does not exist in the definition")
          )
          (index, field.name, columnType)
      }
    def parseRow(row: Row): DataModelInstance = {
      val externalId = row.get(externalIdIndex) match {
        case x: String => x
        case _ => throw SparkSchemaHelperRuntime.badRowError(row, "externalId", "String", "")
      }

      val columnValues: Map[String, Json] = columns.map {
        case (index, name, columnType) =>
          name -> tryGetValue(columnType).applyOrElse(
            row.get(index),
            (_: Any) =>
              throw SparkSchemaHelperRuntime
                .badRowError(row, name, columnType, "")
          )
      }.toMap
      DataModelInstance(Some(modelExternalId), Some(externalId), properties = Some(columnValues))
    }
    (columns.map(_._2), parseRow)
  }
}

object DataModelInstanceRelation {
  val stringTypes = Seq(
    "text",
    "string",
    "json",
    "string",
    "geometry",
    "geography",
    "direct_relation",
    "text[]",
    "boolean[]",
    "numeric[]",
    "float32[]",
    "timestamp",
    "date",
    "float64[]",
    "int32[]",
    "int[]",
    "int64[]",
    "bigint[]"
  )
  // scalastyle:off cyclomatic.complexity
  def propertyTypeToSparkType(columnType: String): DataType =
    columnType.toLowerCase match {
      case complex if stringTypes contains complex =>
        DataTypes.StringType
      case "boolean" => DataTypes.BooleanType
      case "numeric" => DataTypes.DoubleType
      case "float32" => DataTypes.FloatType
      case "float64" => DataTypes.DoubleType
      case "int32" | "int" => DataTypes.IntegerType
      case "int64" | "bigint" => DataTypes.LongType
      case a => throw new CdfSparkException(s"Unknown column type $a")
    }
  // scalastyle:on cyclomatic.complexity

  private def jsonFromDouble(num: Double): Json =
    Json.fromDouble(num).getOrElse(throw new CdfSparkException(s"Numeric value $num"))
  private def tryGetValue(columnType: String): PartialFunction[Any, Json] = // scalastyle:off
    columnType.toLowerCase match {
      case "double" | "numeric" | "float64" => {
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
      }
      case complex if stringTypes contains complex => {
        case null => Json.Null // scalastyle:off null
        case x: String => Json.fromString(x)
      }
      case a =>
        throw new CdfSparkException(s"Unknown column type $a")
    }
}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
