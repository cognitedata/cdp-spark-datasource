package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.DataModelInstanceRelation.propertyTypeToSparkType
import cognite.spark.v1.SparkSchemaHelper._
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1._
import fs2.Stream
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

  def fromRow(rows: StructType): (Array[String], Row => DataModelInstance) =
    ???
}

object DataModelInstanceRelation {
  // scalastyle:off cyclomatic.complexity
  def propertyTypeToSparkType(columnType: String): DataType =
    columnType.toLowerCase match {
      case "text" => DataTypes.StringType
      case "boolean" => DataTypes.BooleanType
      case "numeric" => DataTypes.DoubleType
      case "float32" => DataTypes.FloatType
      case "float64" => DataTypes.DoubleType
      case "int32" | "int" => DataTypes.IntegerType
      case "int64" | "bigint" => DataTypes.LongType
      case "timestamp" => DataTypes.TimestampType
      case "date" => DataTypes.DateType
      case "text[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "boolean[]" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "numeric[]" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "float32[]" => DataTypes.createArrayType(DataTypes.FloatType)
      case "float64[]" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "int32[]" | "int[]" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "int64[]" | "bigint[]" => DataTypes.createArrayType(DataTypes.LongType)
      case "json" => ???
      case "geometry" => ???
      case "geography" => ???
      case "direct_relation" => ???
      case a => throw new CdfSparkException(s"Unknown column type $a")
    }
  // scalastyle:on cyclomatic.complexity
}

final case class ProjectedDataModelInstance(externalId: String, properties: Array[Any])
