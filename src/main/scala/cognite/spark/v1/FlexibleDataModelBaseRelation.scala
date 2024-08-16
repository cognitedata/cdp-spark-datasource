package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.FilterValueDefinition.{
  ComparableFilterValue,
  SeqFilterValue
}
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.instances._
import fs2.Stream
import io.circe.Json
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
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
    new GenericRow(a.properties.map {
      // For startNode, endNode, type & DirectRelationReference & list of DirectRelationReference
      case a: Array[Array[Any]] => a.map(new GenericRow(_))
      case a: Array[Any] => new GenericRow(a)
      case e => e
    })
  }

  private def isReservedAttribute(instanceType: InstanceType, attribute: String) = {
    val alwaysReservedAttributes: Set[String] = Set("space", "externalId", "_type")

    // type is supported as an alias to _type for edges for legacy reasons.
    val edgeReservedAttributes: Set[String] = Set("type", "startNode", "endNode")
    alwaysReservedAttributes.contains(attribute) ||
    (instanceType == InstanceType.Edge && edgeReservedAttributes.contains(attribute))
  }

  private def extractInstancePropertyValue(key: String, value: InstancePropertyValue): Any =
    FlexibleDataModelRelationUtils.extractInstancePropertyValue(schema.apply(key).dataType, value)

  protected def toFilter(
      instanceType: InstanceType,
      sparkFilter: Filter,
      space: Option[String],
      versionedExternalId: Option[String]
  ): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, _) if isReservedAttribute(instanceType, attribute) =>
        toNodeOrEdgeAttributeFilter(instanceType, sparkFilter)
      case IsNull(attribute) if isReservedAttribute(instanceType, attribute) =>
        toNodeOrEdgeAttributeFilter(instanceType, sparkFilter)
      case IsNotNull(attribute) if isReservedAttribute(instanceType, attribute) =>
        toNodeOrEdgeAttributeFilter(instanceType, sparkFilter)
      case StringStartsWith(attribute, _) if isReservedAttribute(instanceType, attribute) =>
        toNodeOrEdgeAttributeFilter(instanceType, sparkFilter)
      case In(attribute, _) if isReservedAttribute(instanceType, attribute) =>
        toNodeOrEdgeAttributeFilter(instanceType, sparkFilter)
      case And(f1, f2) =>
        Vector(f1, f2)
          .traverse(toFilter(instanceType, _, space = space, versionedExternalId))
          .map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        Vector(f1, f2)
          .traverse(toFilter(instanceType, _, space = space, versionedExternalId))
          .map(FilterDefinition.Or.apply)
      case Not(f) =>
        toFilter(instanceType, f, space = space, versionedExternalId)
          .map(FilterDefinition.Not.apply)
      case _ if space.isDefined && versionedExternalId.isDefined =>
        toInstanceFilter(sparkFilter, space.get, versionedExternalId.get)
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"""Unsupported filter '${f.getClass.getSimpleName}', ${f.toString}
               | for space: ${space.getOrElse("not specified")}
               | and versionedExternalId: ${versionedExternalId.getOrElse("not specified")}
               | and instanceType: $instanceType """.stripMargin))
    }

  private def toInstanceFilter(
      sparkFilter: Filter,
      space: String,
      versionedExternalId: String): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
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
      case IsNotNull(attribute) =>
        Right(FilterDefinition.Exists(Seq(space, versionedExternalId, attribute)))
      case IsNull(attribute) =>
        Right(FilterDefinition.Not(FilterDefinition.Exists(Seq(space, versionedExternalId, attribute))))
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Unsupported filter '${f.getClass.getSimpleName}', ${f.toString}"))
    }

  // Some reserved attributes are not attached to a view but directly to the node/edge
  // This handles filtering on these attributes.
  private def toNodeOrEdgeAttributeFilter(
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
      selectedInstanceProps: Array[String]): ProjectedFlexibleDataModelInstance = {
    // Merging all the properties without considering the space & view/container externalId
    // At the time of this impl there is no requirement to consider properties with same name
    // in different view/containers

    val allAvailablePropValues: Map[String, InstancePropertyValue] =
      i.properties.getOrElse(Map.empty).values.flatMap(_.values).fold(Map.empty)(_ ++ _)
    i match {
      case n: InstanceDefinition.NodeDefinition =>
        ProjectedFlexibleDataModelInstance(
          externalId = n.externalId,
          properties = selectedInstanceProps.map {
            case s if s.equalsIgnoreCase("space") => n.space
            case s if s.equalsIgnoreCase("spaceExternalId") => n.space
            case s if s.equalsIgnoreCase("externalId") => n.externalId
            case s if s.equalsIgnoreCase("metadata.cursor") => cursor.getOrElse("")
            case s if s.equalsIgnoreCase("_type") =>
              n.`type`.map(t => Array(t.space, t.externalId)).orNull
            case s if s.equalsIgnoreCase("node.version") => n.version.getOrElse(-1)
            case s if s.equalsIgnoreCase("node.lastUpdatedTime") => n.lastUpdatedTime
            case s if s.equalsIgnoreCase("node.deletedTime") => n.deletedTime.getOrElse(0L)
            case s if s.equalsIgnoreCase("node.createdTime") => n.createdTime
            case p => allAvailablePropValues.get(p).map(it => extractInstancePropertyValue(p, it)).orNull
          },
          space = n.space
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
            case s if s.equalsIgnoreCase("_type") => Array(e.`type`.space, e.`type`.externalId)
            case s if s.equalsIgnoreCase("type") => Array(e.`type`.space, e.`type`.externalId)
            case s if s.equalsIgnoreCase("metadata.cursor") => cursor.getOrElse("")
            case s if s.equalsIgnoreCase("edge.version") => e.version.getOrElse(-1)
            case s if s.equalsIgnoreCase("edge.lastUpdatedTime") => e.lastUpdatedTime
            case s if s.equalsIgnoreCase("edge.deletedTime") => e.deletedTime.getOrElse(0L)
            case s if s.equalsIgnoreCase("edge.createdTime") => e.createdTime
            case p => allAvailablePropValues.get(p).map(it => extractInstancePropertyValue(p, it)).orNull
          },
          space = e.space
        )
    }
  }

  protected def deriveViewPropertySchemaWithUsageSpecificAttributes(
      usage: Usage,
      viewProps: Map[String, ViewPropertyDefinition]): StructType = {
    def primitivePropTypeToSparkDataType(ppt: PrimitivePropType): DataType = ppt match {
      case PrimitivePropType.Timestamp => DataTypes.TimestampType
      case PrimitivePropType.Date => DataTypes.DateType
      case PrimitivePropType.Boolean => DataTypes.BooleanType
      case PrimitivePropType.Float32 => DataTypes.FloatType
      case PrimitivePropType.Float64 => DataTypes.DoubleType
      case PrimitivePropType.Int32 => DataTypes.IntegerType
      case PrimitivePropType.Int64 => DataTypes.LongType
      case PrimitivePropType.Json => DataTypes.StringType
    }

    val fields = viewProps.flatMap {
      case (propName, propDef) =>
        propDef match {
          case corePropDef: PropertyDefinition.ViewCorePropertyDefinition =>
            val nullable = corePropDef.nullable.getOrElse(true)
            corePropDef.`type` match {
              case t: DirectNodeRelationProperty if t.isList =>
                Vector(relationReferenceSchema(propName, nullable = nullable, list = true))
              case _: DirectNodeRelationProperty =>
                Vector(relationReferenceSchema(propName, nullable = nullable))
              case t: TextProperty if t.isList =>
                Vector(
                  DataTypes.createStructField(
                    propName,
                    DataTypes.createArrayType(DataTypes.StringType, nullable),
                    nullable))
              case _: TextProperty =>
                Vector(DataTypes.createStructField(propName, DataTypes.StringType, nullable))
              case p @ PrimitiveProperty(ppt, _) if p.isList =>
                Vector(
                  DataTypes.createStructField(
                    propName,
                    DataTypes.createArrayType(primitivePropTypeToSparkDataType(ppt), nullable),
                    nullable))
              case PrimitiveProperty(ppt, _) =>
                Vector(
                  DataTypes.createStructField(propName, primitivePropTypeToSparkDataType(ppt), nullable))
              case _: CDFExternalIdReference =>
                Vector(DataTypes.createStructField(propName, DataTypes.StringType, nullable))
            }
          case _ => Vector.empty
        }
    } ++ usageBasedSchemaAttributes(usage) ++ metadataAttributes()
    DataTypes.createStructType(fields.toArray)
  }

  protected def metadataAttributes(): Array[StructField] = Array.empty

  // schema fields for relation references and node/edge identifiers
  protected def usageBasedSchemaAttributes(usage: Usage): Array[StructField] = {
    val nodeAttributes = Array(
      DataTypes.createStructField("node.version", DataTypes.LongType, true),
      DataTypes.createStructField("node.createdTime", DataTypes.LongType, true),
      DataTypes.createStructField("node.lastUpdatedTime", DataTypes.LongType, true),
      DataTypes.createStructField("node.deletedTime", DataTypes.LongType, true),
    )

    val edgeAttributes = Array(
      DataTypes.createStructField("edge.version", DataTypes.LongType, true),
      DataTypes.createStructField("edge.createdTime", DataTypes.LongType, true),
      DataTypes.createStructField("edge.lastUpdatedTime", DataTypes.LongType, true),
      DataTypes.createStructField("edge.deletedTime", DataTypes.LongType, true),
      relationReferenceSchema("type", nullable = true)
    )

    val baseAttributes = Array(
      DataTypes.createStructField("space", DataTypes.StringType, false),
      DataTypes.createStructField("externalId", DataTypes.StringType, false)
    )

    def relationReferenceAttributes(nullable: Boolean) = Array(
      relationReferenceSchema("startNode", nullable),
      relationReferenceSchema("endNode", nullable)
    )

    def typeAttribute(nullable: Boolean) =
      Array(
        relationReferenceSchema("_type", nullable),
      )

    usage match {
      case Usage.Node =>
        baseAttributes ++ nodeAttributes ++ typeAttribute(true)
      case Usage.Edge =>
        baseAttributes ++ edgeAttributes ++ typeAttribute(false) ++ relationReferenceAttributes(false)
      case Usage.All =>
        baseAttributes ++ nodeAttributes ++ edgeAttributes ++ typeAttribute(true) ++ relationReferenceAttributes(
          true)
    }
  }

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
        s"""Invalid filter value for: '${attributeVector}'
           |Expecting a struct with 'space' & 'externalId' attributes, but found: ${struct.json}
           |""".stripMargin
      )
    }

  // Filter definitions for attributes for nodes & edges
  private def createNodeOrEdgeCommonAttributeRef(
      instanceType: InstanceType,
      attribute: String): Seq[String] =
    if (attribute.equalsIgnoreCase("_type")) {
      Vector(instanceType.productPrefix.toLowerCase(Locale.US), "type")
    } else {
      Vector(instanceType.productPrefix.toLowerCase(Locale.US), attribute)
    }

  private def relationReferenceInnerStruct(): StructType =
    DataTypes.createStructType(
      Array(
        DataTypes.createStructField("space", DataTypes.StringType, false),
        DataTypes.createStructField("externalId", DataTypes.StringType, false)
      )
    )
  protected def relationReferenceSchema(
      name: String,
      nullable: Boolean,
      list: Boolean = false): StructField =
    if (list) {
      DataTypes.createStructField(
        name,
        DataTypes.createArrayType(relationReferenceInnerStruct()),
        nullable
      )
    } else {
      DataTypes.createStructField(
        name,
        relationReferenceInnerStruct(),
        nullable
      )
    }

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
  final case class ProjectedFlexibleDataModelInstance(
      space: String,
      externalId: String,
      properties: Array[Any])
}
