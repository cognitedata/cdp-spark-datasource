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

  private def extractInstancePropertyValue(key: String, value: InstancePropertyValue): Any =
    FlexibleDataModelRelationUtils.extractInstancePropertyValue(schema.apply(key).dataType, value)

  protected def toInstanceFilter(
      instanceType: InstanceType,
      sparkFilter: Filter,
      space: String,
      versionedExternalId: String): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) if attribute.equalsIgnoreCase("space") =>
        Right(
          FilterDefinition.Equals(
            createNodeOrEdgeCommonAttributeRef(instanceType, "space"),
            FilterValueDefinition.String(String.valueOf(value))))
      case EqualTo(attribute, value) if attribute.equalsIgnoreCase("externalId") =>
        Right(
          FilterDefinition.Equals(
            createNodeOrEdgeCommonAttributeRef(instanceType, "externalId"),
            FilterValueDefinition.String(String.valueOf(value))))
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("type") =>
        createEqualsAttributeFilter("type", value, instanceType)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("startNode") =>
        createEqualsAttributeFilter("startNode", value, instanceType)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("endNode") =>
        createEqualsAttributeFilter("endNode", value, instanceType)
      case EqualTo(attribute, value) =>
        toFilterValueDefinition(attribute, value).map(
          FilterDefinition.Equals(Seq(space, versionedExternalId, attribute), _))
      case In(attribute, values) if attribute.equalsIgnoreCase("space") =>
        toSeqFilterValueDefinition(attribute, values)
          .filterOrElse(
            {
              case FilterValueDefinition.StringList(_) => true
              case _ => false
            },
            new CdfSparkIllegalArgumentException(
              s"Unsupported filter '${sparkFilter.getClass.getSimpleName}', ${sparkFilter.toString}")
          )
          .map(FilterDefinition.In(createNodeOrEdgeCommonAttributeRef(instanceType, "space"), _))
      case In(attribute, values) if attribute.equalsIgnoreCase("externalId") =>
        toSeqFilterValueDefinition(attribute, values)
          .filterOrElse(
            {
              case FilterValueDefinition.StringList(_) => true
              case _ => false
            },
            new CdfSparkIllegalArgumentException(
              s"Unsupported filter '${sparkFilter.getClass.getSimpleName}', ${sparkFilter.toString}")
          )
          .map(FilterDefinition.In(createNodeOrEdgeCommonAttributeRef(instanceType, "externalId"), _))
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
      case StringStartsWith(attribute, value) if attribute.equalsIgnoreCase("space") =>
        Right(
          FilterDefinition.Prefix(
            createNodeOrEdgeCommonAttributeRef(instanceType, "space"),
            FilterValueDefinition.String(value)))
      case StringStartsWith(attribute, value) if attribute.equalsIgnoreCase("externalId") =>
        Right(
          FilterDefinition.Prefix(
            createNodeOrEdgeCommonAttributeRef(instanceType, "externalId"),
            FilterValueDefinition.String(value)))
      case StringStartsWith(attribute, value) =>
        Right(
          FilterDefinition
            .Prefix(Seq(space, versionedExternalId, attribute), FilterValueDefinition.String(value)))
      case And(f1, f2) =>
        Vector(f1, f2)
          .traverse(
            toInstanceFilter(instanceType, _, space = space, versionedExternalId = versionedExternalId))
          .map(FilterDefinition.And.apply)
      case Or(f1, f2) =>
        Vector(f1, f2)
          .traverse(
            toInstanceFilter(instanceType, _, space = space, versionedExternalId = versionedExternalId))
          .map(FilterDefinition.Or.apply)
      //Node types are optional so exists will be automatically checked
      //Types are a property of the node/edge and should not use the view but directly check on the node
      case IsNotNull(attribute)
          if attribute.equalsIgnoreCase("type") && instanceType == InstanceType.Node =>
        Right(FilterDefinition.Exists(Seq("node", "type")))
      case IsNotNull(attribute) =>
        Right(FilterDefinition.Exists(Seq(space, versionedExternalId, attribute)))
      case IsNull(attribute)
          if attribute.equalsIgnoreCase("type") && instanceType == InstanceType.Node =>
        Right(FilterDefinition.Not(FilterDefinition.Exists(Seq("node", "type"))))
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

  protected def toNodeOrEdgeAttributeFilter(
      instanceType: InstanceType,
      sparkFilter: Filter): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value) if attribute.equalsIgnoreCase("space") =>
        Right(
          FilterDefinition.Equals(
            createNodeOrEdgeCommonAttributeRef(instanceType, "space"),
            FilterValueDefinition.String(String.valueOf(value))))
      case EqualTo(attribute, value) if attribute.equalsIgnoreCase("externalId") =>
        Right(
          FilterDefinition.Equals(
            createNodeOrEdgeCommonAttributeRef(instanceType, "externalId"),
            FilterValueDefinition.String(String.valueOf(value))))
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("startNode") =>
        createEqualsAttributeFilter("startNode", value, instanceType)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("endNode") =>
        createEqualsAttributeFilter("endNode", value, instanceType)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("type") =>
        createEqualsAttributeFilter("type", value, instanceType)
      case Or(f1, f2) =>
        Vector(f1, f2)
          .traverse(toNodeOrEdgeAttributeFilter(instanceType, _))
          .map(FilterDefinition.Or.apply)
      case And(f1, f2) =>
        Vector(f1, f2)
          .traverse(toNodeOrEdgeAttributeFilter(instanceType, _))
          .map(FilterDefinition.And.apply)
      case Not(f) =>
        toNodeOrEdgeAttributeFilter(instanceType, f)
          .map(FilterDefinition.Not.apply)
      //Node types are optional so exists will be automatically checked
      //Types are a property of the node/edge and should not use the view but directly check on the node
      case IsNotNull(attribute)
          if attribute.equalsIgnoreCase("type") && instanceType == InstanceType.Node =>
        Right(FilterDefinition.Exists(Seq("node", "type")))
      case IsNull(attribute)
          if attribute.equalsIgnoreCase("type") && instanceType == InstanceType.Node =>
        Right(FilterDefinition.Not(FilterDefinition.Exists(Seq("node", "type"))))
      case f =>
        Left(new CdfSparkIllegalArgumentException(
          s"Unsupported node or edge attribute filter '${f.getClass.getSimpleName}': ${String.valueOf(f)}"))
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
            case s if s.equalsIgnoreCase("type") =>
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
      DataTypes.createStructField("node.deletedTime", DataTypes.LongType, true)
    )

    val edgeAttributes = Array(
      DataTypes.createStructField("edge.version", DataTypes.LongType, true),
      DataTypes.createStructField("edge.createdTime", DataTypes.LongType, true),
      DataTypes.createStructField("edge.lastUpdatedTime", DataTypes.LongType, true),
      DataTypes.createStructField("edge.deletedTime", DataTypes.LongType, true)
    )

    usage match {
      case Usage.Node =>
        Array(
          DataTypes.createStructField("space", DataTypes.StringType, false),
          DataTypes.createStructField("externalId", DataTypes.StringType, false),
          relationReferenceSchema("type", nullable = true)
        ) ++ nodeAttributes
      case Usage.Edge =>
        Array(
          DataTypes.createStructField("space", DataTypes.StringType, false),
          DataTypes.createStructField("externalId", DataTypes.StringType, false),
          relationReferenceSchema("type", nullable = false),
          relationReferenceSchema("startNode", nullable = false),
          relationReferenceSchema("endNode", nullable = false)
        ) ++ edgeAttributes
      case Usage.All =>
        Array(
          DataTypes.createStructField("space", DataTypes.StringType, false),
          DataTypes.createStructField("externalId", DataTypes.StringType, false),
          relationReferenceSchema("type", nullable = true),
          relationReferenceSchema("startNode", nullable = true),
          relationReferenceSchema("endNode", nullable = true)
        ) ++ nodeAttributes ++ edgeAttributes
    }
  }

  // Filter definition for edge `type`, `startNode` & `endNode`
  private def createEqualsAttributeFilter(
      attribute: String,
      struct: GenericRowWithSchema,
      instanceType: InstanceType): Either[CdfSparkException, FilterDefinition] = {
    val instanceTypeString = instanceType match {
      case InstanceType.Edge => "edge"
      case InstanceType.Node => "node"
    }
    Try {
      val space = struct.getString(struct.fieldIndex("space"))
      val externalId = struct.getString(struct.fieldIndex("externalId"))
      FilterDefinition.Equals(
        property = Vector(instanceTypeString, attribute),
        value = FilterValueDefinition.Object(
          Json.obj("space" -> Json.fromString(space), "externalId" -> Json.fromString(externalId)))
      )
    }.toEither.leftMap { _ =>
      new CdfSparkIllegalArgumentException(
        s"""Invalid filter value for: '$instanceTypeString $attribute'
           |Expecting a struct with 'space' & 'externalId' attributes, but found: ${struct.json}
           |""".stripMargin
      )
    }
  }

  // Filter definitions for "space" & "externalId" attributes for nodes & edges
  private def createNodeOrEdgeCommonAttributeRef(
      instanceType: InstanceType,
      attribute: String): Seq[String] =
    Vector(instanceType.productPrefix.toLowerCase(Locale.US), attribute)

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
