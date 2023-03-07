package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterValueDefinition.{
  ComparableFilterValue,
  SeqFilterValue
}
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Row, SQLContext}

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.time._
import java.util.Locale
import scala.util.Try

abstract class FlexibleDataModelBaseRelation(config: RelationConfig, sqlContext: SQLContext)
    extends CdfRelation(config, FlexibleDataModelRelation.ResourceType)
    with PrunedFilteredScan
    with WritableRelation {

  override def buildScan(selectedColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedFlexibleDataModelInstance, String](
      sqlContext.sparkContext,
      config,
      (item: ProjectedFlexibleDataModelInstance, _) => toRow(item),
      _.externalId,
      getStreams(filters, selectedColumns)
    )

  def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]]

  protected def toRow(a: ProjectedFlexibleDataModelInstance): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    new GenericRow(a.properties.map {
      // For startNode, endNode, type & DirectRelationReference
      case a: Array[Any] => new GenericRow(a)
      case e => e
    })
  }

  // scalastyle:off cyclomatic.complexity
  private def extractInstancePropertyValue: InstancePropertyValue => Any = {
    case InstancePropertyValue.String(value) => value
    case InstancePropertyValue.Int32(value) => value
    case InstancePropertyValue.Int64(value) => value
    case InstancePropertyValue.Float32(value) => value
    case InstancePropertyValue.Float64(value) => value
    case InstancePropertyValue.Boolean(value) => value
    case InstancePropertyValue.Date(value) => java.sql.Date.valueOf(value)
    case InstancePropertyValue.Timestamp(value) => java.sql.Timestamp.from(value.toInstant)
    case InstancePropertyValue.Object(value) => value.noSpaces
    case InstancePropertyValue.ViewDirectNodeRelation(value) =>
      value.map(r => Array(r.space, r.externalId)).orNull
    case InstancePropertyValue.StringList(value) => value
    case InstancePropertyValue.BooleanList(value) => value
    case InstancePropertyValue.Int32List(value) => value
    case InstancePropertyValue.Int64List(value) => value
    case InstancePropertyValue.Float32List(value) => value
    case InstancePropertyValue.Float64List(value) => value
    case InstancePropertyValue.DateList(value) => value.map(v => java.sql.Date.valueOf(v))
    case InstancePropertyValue.TimestampList(value) =>
      value.map(v => java.sql.Timestamp.from(v.toInstant))
    case InstancePropertyValue.ObjectList(value) => value.map(_.noSpaces)
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity method.length
  protected def toInstanceFilter(
      instanceType: InstanceType,
      sparkFilter: Filter,
      space: String,
      versionedExternalId: String): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) if attribute.equalsIgnoreCase("space") =>
        Right(createNodeOrEdgeSpaceFilter(instanceType, value))
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("type") =>
        createEdgeAttributeFilter("type", value)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("startNode") =>
        createEdgeAttributeFilter("startNode", value)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("endNode") =>
        createEdgeAttributeFilter("endNode", value)
      case EqualTo(attribute, value) =>
        toFilterValueDefinition(attribute, value).map(
          FilterDefinition.Equals(Seq(space, versionedExternalId, attribute), _))
      case In(attribute, values) =>
        toSeqFilterValueDefinition(attribute, values).map(
          FilterDefinition.In(Seq(space, versionedExternalId, attribute), _))
      case GreaterThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, versionedExternalId, attribute), gte = Some(f)))
      case GreaterThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, versionedExternalId, attribute), gt = Some(f)))
      case LessThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, versionedExternalId, attribute), lte = Some(f)))
      case LessThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(f =>
          FilterDefinition.Range(property = Seq(space, versionedExternalId, attribute), lt = Some(f)))
      case StringStartsWith(attribute, value) =>
        Right(
          FilterDefinition
            .Prefix(Seq(space, versionedExternalId, attribute), FilterValueDefinition.String(value)))
      case And(f1, f2) =>
        List(f1, f2)
          .traverse(
            toInstanceFilter(instanceType, _, space = space, versionedExternalId = versionedExternalId))
          .map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        List(f1, f2)
          .traverse(
            toInstanceFilter(instanceType, _, space = space, versionedExternalId = versionedExternalId))
          .map(FilterDefinition.Or.apply)
      case IsNotNull(attribute) =>
        Right(FilterDefinition.Exists(Seq(space, versionedExternalId, attribute)))
      case IsNull(attribute) =>
        Right(FilterDefinition.Not(FilterDefinition.Exists(Seq(space, versionedExternalId, attribute))))
      case Not(f) =>
        toInstanceFilter(instanceType, f, space = space, versionedExternalId = versionedExternalId)
          .map(FilterDefinition.Not.apply)
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Unsupported filter '${f.getClass.getSimpleName}', ${f.toString}"))
    }
  // scalastyle:on cyclomatic.complexity

  protected def toNodeOrEdgeAttributeFilter(
      instanceType: InstanceType,
      sparkFilter: Filter): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) if attribute.equalsIgnoreCase("space") =>
        Right(createNodeOrEdgeSpaceFilter(instanceType, value))
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("startNode") =>
        createEdgeAttributeFilter("startNode", value)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("endNode") =>
        createEdgeAttributeFilter("endNode", value)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("type") =>
        createEdgeAttributeFilter("type", value)
      case f =>
        Left(new CdfSparkIllegalArgumentException(
          s"Unsupported node or edge attribute filter '${f.getClass.getSimpleName}': ${String.valueOf(f)}"))
    }

  // filter for `type`, `startNode` & `endNode`
  private def createEdgeAttributeFilter(
      attribute: String,
      struct: GenericRowWithSchema): Either[CdfSparkException, FilterDefinition] =
    Try {
      val space = struct.getString(struct.fieldIndex("space"))
      val externalId = struct.getString(struct.fieldIndex("externalId"))
      FilterDefinition.Equals(
        property = Vector("edge", attribute),
        value = FilterValueDefinition.StringList(Vector(space, externalId))
      )
    }.toEither.leftMap { _ =>
      new CdfSparkIllegalArgumentException(
        s"""Invalid filter value for: 'edge $attribute'
           |Expecting a struct with 'space' & 'externalId' attributes, but found: ${struct.json}
           |""".stripMargin
      )
    }

  private def createNodeOrEdgeSpaceFilter(instanceType: InstanceType, value: Any): FilterDefinition =
    FilterDefinition.Equals(
      property = Vector(instanceType.productPrefix.toLowerCase(Locale.US), "space"),
      value = FilterValueDefinition.String(String.valueOf(value))
    )

  protected def relationReferenceSchema(name: String, nullable: Boolean): StructField =
    DataTypes.createStructField(
      name,
      DataTypes.createStructType(
        Array(
          DataTypes.createStructField("space", DataTypes.StringType, false),
          DataTypes.createStructField("externalId", DataTypes.StringType, false)
        )
      ),
      nullable
    )

  // scalastyle:off cyclomatic.complexity
  protected def toProjectedInstance(
      i: InstanceDefinition,
      selectedInstanceProps: Array[String]): ProjectedFlexibleDataModelInstance = {
    // Merging all the properties without considering the space & view/container externalId
    // At the time of this impl there is no requirement to consider properties with same name
    // in different view/containers
    val allAvailablePropValues =
      i.properties.getOrElse(Map.empty).values.flatMap(_.values).fold(Map.empty)(_ ++ _)
    i match {
      case n: InstanceDefinition.NodeDefinition =>
        ProjectedFlexibleDataModelInstance(
          externalId = n.externalId,
          properties = selectedInstanceProps.map {
            case s if s.equalsIgnoreCase("space") => n.space
            case s if s.equalsIgnoreCase("spaceExternalId") => n.space
            case s if s.equalsIgnoreCase("externalId") => n.externalId
            case p => allAvailablePropValues.get(p).map(extractInstancePropertyValue).orNull
          }
        )
      case e: InstanceDefinition.EdgeDefinition =>
        ProjectedFlexibleDataModelInstance(
          externalId = e.externalId,
          properties = selectedInstanceProps.map {
            case s if s.equalsIgnoreCase("space") => e.space
            case s if s.equalsIgnoreCase("spaceExternalId") => e.space
            case s if s.equalsIgnoreCase("externalId") => e.externalId
            case s if s.equalsIgnoreCase("startNode") => Array(e.startNode.space, e.startNode.externalId)
            case s if s.equalsIgnoreCase("endNode") => Array(e.endNode.space, e.endNode.externalId)
            case s if s.equalsIgnoreCase("type") => Array(e.`type`.space, e.`type`.externalId)
            case p => allAvailablePropValues.get(p).map(extractInstancePropertyValue).orNull
          }
        )
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def toComparableFilterValueDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, ComparableFilterValue] =
    toFilterValueDefinition(attribute, value) match {
      case Right(v: ComparableFilterValue) => Right(v)
      case Right(v) =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""Invalid filter value!
               |Expecting a value of type number, string, boolean, json,
               |array[number], array[string], array[boolean], array[json] for '$attribute',
               |but found ${v.getClass.getSimpleName} in ${value.toString}
               |""".stripMargin
          )
        )
      case Left(err) => Left(err)
    }

  // scalastyle:off cyclomatic.complexity
  private def toFilterValueDefinition(
      attribute: String,
      value: Any): Either[CdfSparkException, FilterValueDefinition] =
    value match {
      case v: String =>
        io.circe.parser.parse(v) match {
          case Right(json) if json.isObject => Right(FilterValueDefinition.Object(json))
          case _ => Right(FilterValueDefinition.String(v))
        }
      case v: Float => Right(FilterValueDefinition.Double(v.toDouble))
      case v: Double => Right(FilterValueDefinition.Double(v))
      case v: Int => Right(FilterValueDefinition.Integer(v.toLong))
      case v: Long => Right(FilterValueDefinition.Integer(v))
      case v: Boolean => Right(FilterValueDefinition.Boolean(v))
      case v: java.math.BigDecimal => Right(FilterValueDefinition.Double(v.doubleValue))
      case v: BigInteger => Right(FilterValueDefinition.Integer(v.longValue))
      case v: LocalDate =>
        Right(FilterValueDefinition.String(v.format(InstancePropertyValue.Date.formatter)))
      case v: LocalDateTime =>
        Right(FilterValueDefinition.String(v.format(InstancePropertyValue.Timestamp.formatter)))
      case v: Instant =>
        Right(
          FilterValueDefinition.String(
            OffsetDateTime
              .ofInstant(v, ZoneId.of("UTC"))
              .toZonedDateTime
              .format(InstancePropertyValue.Timestamp.formatter)))
      case v: ZonedDateTime =>
        Right(FilterValueDefinition.String(v.format(InstancePropertyValue.Timestamp.formatter)))
      case v: Date =>
        Right(FilterValueDefinition.String(v.toLocalDate.format(InstancePropertyValue.Date.formatter)))
      case v: Timestamp =>
        Right(
          FilterValueDefinition.String(
            OffsetDateTime
              .ofInstant(v.toInstant, ZoneId.of("UTC"))
              .toZonedDateTime
              .format(InstancePropertyValue.Timestamp.formatter)))
      case v: Array[Any] =>
        toFilterValueListDefinition(attribute, v.toVector)
      case v: GenericRowWithSchema =>
        toFilterDirectNodeRelation(v)
      case v =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""Invalid filter value!
               |Expecting a value of type number, string, boolean or an array of them for '$attribute', but found ${v.toString}""".stripMargin
          )
        )
    }
  // scalastyle:on cyclomatic.complexity

  private def toFilterDirectNodeRelation(
      v: GenericRowWithSchema): Either[CdfSparkIllegalArgumentException, FilterValueDefinition] =
    Try {
      val space = v.getString(v.fieldIndex("space"))
      val externalId = v.getString(v.fieldIndex("externalId"))
      FilterValueDefinition.StringList(Vector(space, externalId))
    }.toEither.leftMap { _ =>
      new CdfSparkIllegalArgumentException(
        s"""Invalid filter value!
           |Expecting a struct with 'space' & 'externalId' attributes, but found: ${v.json}
           |""".stripMargin
      )
    }

  // scalastyle:off cyclomatic.complexity method.length
  private def toFilterValueListDefinition(
      attribute: String,
      values: Vector[Any]): Either[CdfSparkException, FilterValueDefinition] = {
    val result = values.headOption match {
      case Some(s: String) =>
        io.circe.parser.parse(s).toOption match {
          case Some(_) =>
            Try(FilterValueDefinition.ObjectList(values.flatMap(v =>
              io.circe.parser.parse(String.valueOf(v)).toOption))).toEither
          case None =>
            Try(FilterValueDefinition.StringList(values.map(_.asInstanceOf[String]))).toEither
        }
      case Some(_: Int | _: Long) =>
        Try(FilterValueDefinition.IntegerList(values.map(_.asInstanceOf[Long]))).toEither
      case Some(_: Float | _: Double) =>
        Try(FilterValueDefinition.DoubleList(values.map(_.asInstanceOf[Double]))).toEither
      case Some(_: Boolean) =>
        Try(FilterValueDefinition.BooleanList(values.map(_.asInstanceOf[Boolean]))).toEither
      case _ =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting an array of number, string, boolean or json for '$attribute', but found ${String
              .valueOf(values)}"
          )
        )
    }
    result.leftMap {
      case e: CdfSparkIllegalArgumentException => e
      case e: Throwable =>
        val arrAsStr = values.map(String.valueOf).mkString(",")
        new CdfSparkIllegalArgumentException(
          s"""Expecting an array of number, string, boolean or json for '$attribute', but found $arrAsStr
             |${e.getMessage}
             |""".stripMargin
        )
    }
  }

  private def toSeqFilterValueDefinition(
      attribute: String,
      value: Array[Any]): Either[CdfSparkException, SeqFilterValue] =
    toFilterValueDefinition(attribute, value) match {
      case Right(v: SeqFilterValue) => Right(v)
      case Right(FilterValueDefinition.Double(v)) => Right(FilterValueDefinition.DoubleList(Seq(v)))
      case Right(FilterValueDefinition.Integer(v)) => Right(FilterValueDefinition.IntegerList(Seq(v)))
      case Right(FilterValueDefinition.String(v)) => Right(FilterValueDefinition.StringList(Seq(v)))
      case Right(FilterValueDefinition.Boolean(v)) => Right(FilterValueDefinition.BooleanList(Seq(v)))
      case Right(FilterValueDefinition.Object(v)) => Right(FilterValueDefinition.ObjectList(Seq(v)))
      case Right(v) =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Expecting a SeqFilterValue for '$attribute', but found ${v.getClass.getSimpleName} as ${value
              .mkString(",")}"
          )
        )
      case Left(err) => Left(err)
    }
}

object FlexibleDataModelBaseRelation {
  final case class ProjectedFlexibleDataModelInstance(externalId: String, properties: Array[Any])
}
