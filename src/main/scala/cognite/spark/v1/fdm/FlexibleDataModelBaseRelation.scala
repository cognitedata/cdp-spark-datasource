package cognite.spark.v1.fdm

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.fdm.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.{
  CdfRelation,
  CdfSparkException,
  CdfSparkIllegalArgumentException,
  DeleteSchema,
  NamedRelation,
  RelationConfig,
  SdkV1Rdd,
  UpsertSchema,
  WritableRelation
}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterValueDefinition.{
  ComparableFilterValue,
  SeqFilterValue
}
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import fs2.Stream
import io.circe.Json
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, Literal}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.time._
import java.util.Locale
import scala.util.Try

abstract class FlexibleDataModelBaseRelation(config: RelationConfig, sqlContext: SQLContext)
    extends CdfRelation(config, FlexibleDataModelRelationFactory.ResourceType)
    with PrunedFilteredScan
    with WritableRelation {

  override def buildScan(selectedColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    SdkV1Rdd[ProjectedFlexibleDataModelInstance, (String, String)](
      sqlContext.sparkContext,
      config,
      (item: ProjectedFlexibleDataModelInstance, _) => toRow(item),
      instance => (instance.space, instance.externalId),
      getStreams(filters, selectedColumns)
    )

  def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]]

  protected def toRow(a: ProjectedFlexibleDataModelInstance): Row = {
    if (config.collectMetrics) {
      itemsRead.inc()
    }
    FlexibleDataModelInstanceMapping.toRow(a.properties)
  }

  protected def optionalDebug(shouldEnable: Boolean): Option[InstanceDebugParameters] =
    if (shouldEnable) {
      Some(InstanceDebugParameters(timeout = None, emitResults = Some(true), profile = None))
    } else {
      None
    }

  private def isReservedAttribute(instanceType: InstanceType, attribute: String) = {
    val alwaysReservedAttributes: Set[String] = Set("space", "externalId", "_type")

    // type is supported as an alias to _type for edges for legacy reasons.
    val edgeReservedAttributes: Set[String] = Set("type", "startNode", "endNode")
    alwaysReservedAttributes.contains(attribute) ||
    (instanceType == InstanceType.Edge && edgeReservedAttributes.contains(attribute))
  }

  private def getVersionedViewExternalId(viewReference: ViewReference): String =
    s"${viewReference.externalId}/${viewReference.version}"

  private def getViewAttributeReference(
      viewReference: ViewReference,
      propertyName: String): Seq[String] =
    Seq(viewReference.space, getVersionedViewExternalId(viewReference), propertyName)

  protected def toFilter(
      instanceType: InstanceType,
      sparkFilter: Filter,
      // viewReference is required for non-reserved attribute filters resolution
      viewReference: Option[ViewReference],
      isSyncRequest: Boolean = false
  ): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      // reserved attributes case, make sure to update toReservedAttributeFilter when adding more
      case EqualTo(attribute, _) if isReservedAttribute(instanceType, attribute) =>
        toReservedAttributeFilter(instanceType, sparkFilter)
      case EqualNullSafe(attribute, value) if isReservedAttribute(instanceType, attribute) =>
        value match {
          case Literal(null, _) =>
            toReservedAttributeFilter(instanceType, IsNull(attribute))
          case _ =>
            toReservedAttributeFilter(instanceType, EqualTo(attribute, value))
        }
      case IsNull(attribute) if isReservedAttribute(instanceType, attribute) =>
        toReservedAttributeFilter(instanceType, sparkFilter)
      case IsNotNull(attribute) if isReservedAttribute(instanceType, attribute) =>
        toReservedAttributeFilter(instanceType, sparkFilter)
      case StringStartsWith(attribute, _)
          if isReservedAttribute(instanceType, attribute) && !isSyncRequest =>
        toReservedAttributeFilter(instanceType, sparkFilter)
      case In(attribute, _) if isReservedAttribute(instanceType, attribute) && !isSyncRequest =>
        toReservedAttributeFilter(instanceType, sparkFilter)
      // end reserved attributes case
      case And(f1, f2) =>
        Vector(f1, f2)
          .traverse(toFilter(instanceType, _, viewReference))
          .map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        Vector(f1, f2)
          .traverse(toFilter(instanceType, _, viewReference))
          .map(FilterDefinition.Or.apply)
      case Not(f) =>
        toFilter(instanceType, f, viewReference)
          .map(FilterDefinition.Not.apply)
      case _ if viewReference.isDefined && !isSyncRequest =>
        toInstanceFilter(sparkFilter, viewReference.get)
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""Unsupported filter '${f.getClass.getSimpleName}', ${f.toString}
               | for space: ${viewReference.map(_.space).getOrElse("not specified")}
               | and versionedExternalId: ${viewReference
                 .map(getVersionedViewExternalId)
                 .getOrElse("not specified")}
               | and instanceType: $instanceType """.stripMargin))
    }

  private def toInstanceFilter(
      sparkFilter: Filter,
      viewReference: ViewReference): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) =>
        toFilterValueDefinition(attribute, value).map(
          FilterDefinition.Equals(getViewAttributeReference(viewReference, attribute), _))
      case EqualNullSafe(attribute, value) =>
        value match {
          case Literal(null, _) =>
            toInstanceFilter(IsNull(attribute), viewReference)
          case _ =>
            toInstanceFilter(EqualTo(attribute, value), viewReference)
        }
      case In(attribute, values) =>
        toSeqFilterValueDefinition(attribute, values).map(
          FilterDefinition.In(getViewAttributeReference(viewReference, attribute), _))
      case GreaterThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(
          f =>
            FilterDefinition
              .Range(property = getViewAttributeReference(viewReference, attribute), gte = Some(f)))
      case GreaterThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(
          f =>
            FilterDefinition
              .Range(property = getViewAttributeReference(viewReference, attribute), gt = Some(f)))
      case LessThanOrEqual(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(
          f =>
            FilterDefinition
              .Range(property = getViewAttributeReference(viewReference, attribute), lte = Some(f)))
      case LessThan(attribute, value) =>
        toComparableFilterValueDefinition(attribute, value).map(
          f =>
            FilterDefinition
              .Range(property = getViewAttributeReference(viewReference, attribute), lt = Some(f)))
      case StringStartsWith(attribute, value) =>
        Right(
          FilterDefinition
            .Prefix(
              getViewAttributeReference(viewReference, attribute),
              FilterValueDefinition.String(value)))
      case IsNotNull(attribute) =>
        Right(FilterDefinition.Exists(getViewAttributeReference(viewReference, attribute)))
      case IsNull(attribute) =>
        Right(
          FilterDefinition.Not(
            FilterDefinition.Exists(getViewAttributeReference(viewReference, attribute))))
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Unsupported filter '${f.getClass.getSimpleName}', ${f.toString}"))
    }

  // Some reserved attributes are not attached to a view but directly to the node/edge
  // This handles filtering on these attributes.
  private def toReservedAttributeFilter(
      instanceType: InstanceType,
      sparkFilter: Filter): Either[CdfSparkException, FilterDefinition] = {
    val nodeOrEdgeStringAttributes = Seq("space", "externalId")
    val nodeOrEdgeReferenceAttributes = Seq("type", "_type", "startNode", "endNode")
    sparkFilter match {
      case EqualTo(attribute, value) if nodeOrEdgeStringAttributes.contains(attribute) =>
        Right(
          FilterDefinition.Equals(
            createNodeOrEdgeCommonAttributeRef(instanceType, attribute),
            FilterValueDefinition.String(String.valueOf(value))))
      case EqualTo(attribute, value: GenericRowWithSchema)
          if nodeOrEdgeReferenceAttributes.contains(attribute) =>
        createEqualsAttributeFilter(createNodeOrEdgeCommonAttributeRef(instanceType, attribute), value)
      case In(attribute, values) if nodeOrEdgeStringAttributes.contains(attribute) =>
        toSeqFilterValueDefinition(attribute, values)
          .filterOrElse(
            {
              case FilterValueDefinition.StringList(_) => true
              case _ => false
            },
            new CdfSparkIllegalArgumentException(
              s"Unsupported filter '${sparkFilter.getClass.getSimpleName}', ${sparkFilter.toString}")
          )
          .map(FilterDefinition.In(createNodeOrEdgeCommonAttributeRef(instanceType, attribute), _))
      case StringStartsWith(attribute, value) if nodeOrEdgeStringAttributes.contains(attribute) =>
        Right(
          FilterDefinition.Prefix(
            createNodeOrEdgeCommonAttributeRef(instanceType, attribute),
            FilterValueDefinition.String(value)))
      case IsNotNull(attribute) =>
        Right(FilterDefinition.Exists(createNodeOrEdgeCommonAttributeRef(instanceType, attribute)))
      case IsNull(attribute) =>
        Right(
          FilterDefinition.Not(
            FilterDefinition.Exists(createNodeOrEdgeCommonAttributeRef(instanceType, attribute))))
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""Unsupported node or edge attribute filter '${f.getClass.getSimpleName}': ${String
                 .valueOf(f)}
             | for instanceType: $instanceType
             |""".stripMargin))
    }
  }

  protected def toProjectedInstance(
      i: InstanceDefinition,
      cursor: Option[String],
      selectedInstanceProps: Array[String]): ProjectedFlexibleDataModelInstance =
    FlexibleDataModelInstanceMapping.toProjectedInstance(
      i,
      cursor,
      selectedInstanceProps,
      key => schema.apply(key).dataType)

  protected def deriveViewPropertySchemaWithUsageSpecificAttributes(
      usage: Usage,
      viewProps: Map[String, ViewPropertyDefinition]): StructType = {
    val fields = viewProps.flatMap {
      case (propName, propDef) =>
        propDef match {
          case corePropDef: PropertyDefinition.ViewCorePropertyDefinition =>
            Vector(
              FlexibleDataModelInstanceMapping.propertyField(
                propName,
                corePropDef.`type`,
                corePropDef.nullable.getOrElse(true)))
          case _ => Vector.empty
        }
    } ++ usageBasedSchemaAttributes(usage) ++ metadataAttributes()
    DataTypes.createStructType(fields.toArray)
  }

  protected def metadataAttributes(): Array[StructField] = Array.empty

  // schema fields for relation references and node/edge identifiers
  protected def usageBasedSchemaAttributes(usage: Usage): Array[StructField] =
    FlexibleDataModelInstanceMapping.usageBasedSchemaAttributes(usage)

  // Filter definition for node/edge `type`, `startNode` & `endNode`
  private def createEqualsAttributeFilter(
      attributeVector: Seq[String],
      struct: GenericRowWithSchema): Either[CdfSparkException, FilterDefinition] =
    Try {
      val space = struct.getString(struct.fieldIndex("space"))
      val externalId = struct.getString(struct.fieldIndex("externalId"))
      FilterDefinition.Equals(
        property = attributeVector,
        value = FilterValueDefinition.Object(
          Json.obj("space" -> Json.fromString(space), "externalId" -> Json.fromString(externalId)))
      )
    }.toEither.leftMap { _ =>
      new CdfSparkIllegalArgumentException(
        s"""Invalid filter value for: '$attributeVector'
           |Expecting a struct with 'space' & 'externalId' attributes, but found: ${struct.json}
           |""".stripMargin
      )
    }

  // Filter definitions for attributes for nodes & edges
  private def createNodeOrEdgeCommonAttributeRef(
      instanceType: InstanceType,
      attribute: String): Seq[String] = {
    val instanceAttribute = {
      if (attribute.equalsIgnoreCase("_type"))
        "type"
      else
        attribute
    }
    Vector(instanceType.productPrefix.toLowerCase(Locale.US), instanceAttribute)
  }

  protected def relationReferenceSchema(
      name: String,
      nullable: Boolean,
      list: Boolean = false): StructField =
    FlexibleDataModelInstanceMapping.relationReferenceSchema(name, nullable, list)

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

  private def toFilterDirectNodeRelation(
      v: GenericRowWithSchema): Either[CdfSparkIllegalArgumentException, FilterValueDefinition] =
    Try {
      val space = v.getString(v.fieldIndex("space"))
      val externalId = v.getString(v.fieldIndex("externalId"))
      FilterValueDefinition.Object(
        Json.obj("space" -> Json.fromString(space), "externalId" -> Json.fromString(externalId)))
    }.toEither.leftMap { _ =>
      new CdfSparkIllegalArgumentException(
        s"""Invalid filter value!
           |Expecting a struct with 'space' & 'externalId' attributes, but found: ${v.json}
           |""".stripMargin
      )
    }

  private def toFilterValueListDefinition(
      attribute: String,
      values: Vector[Any]): Either[CdfSparkException, FilterValueDefinition] = {
    val result = values.headOption match {
      case Some(s: String) =>
        io.circe.parser.parse(s).toOption match {
          // Check if it's a complex Json Object first.
          case Some(x) if x.isArray || x.isObject =>
            Try(FilterValueDefinition.ObjectList(values.flatMap(v =>
              io.circe.parser.parse(String.valueOf(v)).toOption))).toEither
          case _ =>
            Try(FilterValueDefinition.StringList(values.map(_.asInstanceOf[String]))).toEither
        }
      case Some(_: Int) =>
        Try(FilterValueDefinition.IntegerList(values.map(_.asInstanceOf[Int].toLong))).toEither
      case Some(_: Long) =>
        Try(FilterValueDefinition.IntegerList(values.map(_.asInstanceOf[Long]))).toEither
      case Some(_: Float) =>
        Try(FilterValueDefinition.DoubleList(values.map(_.asInstanceOf[Float].toDouble))).toEither
      case Some(_: Double) =>
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

object FlexibleDataModelBaseRelation extends NamedRelation with UpsertSchema with DeleteSchema {
  import cognite.spark.compiletime.macros.SparkSchemaHelper.structType
  import cognite.spark.v1.StructTypeEncoder
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  // TODO: this seems to be incomplete or fully wrong schema
  final case class DataModelBaseRelationUpsertSchema(externalId: String) {}

  override val upsertSchema: StructType = structType[DataModelBaseRelationUpsertSchema]()

  // TODO: this seems to be missing optional "space" parameter
  final case class DataModelBaseRelationDeleteSchema(externalId: String) {}

  override val deleteSchema: StructType = structType[DataModelBaseRelationDeleteSchema]()

  override val name = "instances"
  final case class ProjectedFlexibleDataModelInstance(
      space: String,
      externalId: String,
      properties: Array[Any])
}
