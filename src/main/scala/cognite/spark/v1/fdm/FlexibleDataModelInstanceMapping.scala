package cognite.spark.v1.fdm

import cognite.spark.v1.fdm.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType._
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.instances._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

/**
  * Mapping between flexible data model instances and Spark schemas/rows. It only depends on SDK
  * domain types, so it is shared by every relation that exposes instances as rows, whatever the
  * wire format they were decoded from.
  */
private[spark] object FlexibleDataModelInstanceMapping {

  private def primitivePropTypeToSparkDataType(ppt: PrimitivePropType): DataType = ppt match {
    case PrimitivePropType.Timestamp => DataTypes.TimestampType
    case PrimitivePropType.Date => DataTypes.DateType
    case PrimitivePropType.Boolean => DataTypes.BooleanType
    case PrimitivePropType.Float32 => DataTypes.FloatType
    case PrimitivePropType.Float64 => DataTypes.DoubleType
    case PrimitivePropType.Int32 => DataTypes.IntegerType
    case PrimitivePropType.Int64 => DataTypes.LongType
    case PrimitivePropType.Json => DataTypes.StringType
  }

  def propertyField(name: String, propertyType: PropertyType, nullable: Boolean): StructField =
    propertyType match {
      case t: DirectNodeRelationProperty if t.isList =>
        relationReferenceSchema(name, nullable = nullable, list = true)
      case _: DirectNodeRelationProperty =>
        relationReferenceSchema(name, nullable = nullable)
      case t: TextProperty if t.isList =>
        DataTypes.createStructField(
          name,
          DataTypes.createArrayType(DataTypes.StringType, nullable),
          nullable)
      case _: TextProperty =>
        DataTypes.createStructField(name, DataTypes.StringType, nullable)
      case _: EnumProperty =>
        DataTypes.createStructField(name, DataTypes.StringType, nullable)
      case p @ PrimitiveProperty(ppt, _) if p.isList =>
        DataTypes.createStructField(
          name,
          DataTypes.createArrayType(primitivePropTypeToSparkDataType(ppt), nullable),
          nullable)
      case PrimitiveProperty(ppt, _) =>
        DataTypes.createStructField(name, primitivePropTypeToSparkDataType(ppt), nullable)
      case p: CDFExternalIdReference if p.isList =>
        DataTypes.createStructField(
          name,
          DataTypes.createArrayType(DataTypes.StringType, nullable),
          nullable)
      case _: CDFExternalIdReference =>
        DataTypes.createStructField(name, DataTypes.StringType, nullable)
    }

  // schema fields for relation references and node/edge identifiers
  def usageBasedSchemaAttributes(usage: Usage): Array[StructField] = {
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

  private def relationReferenceInnerStruct(): StructType =
    DataTypes.createStructType(
      Array(
        DataTypes.createStructField("space", DataTypes.StringType, false),
        DataTypes.createStructField("externalId", DataTypes.StringType, false)
      )
    )

  def relationReferenceSchema(name: String, nullable: Boolean, list: Boolean = false): StructField =
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

  /**
    * @param dataTypeOf Spark data type of a selected column, used to convert property values
    */
  def toProjectedInstance(
      i: InstanceDefinition,
      cursor: Option[String],
      selectedInstanceProps: Array[String],
      dataTypeOf: String => DataType): ProjectedFlexibleDataModelInstance = {
    // Merging all the properties without considering the space & view/container externalId
    // At the time of this impl there is no requirement to consider properties with same name
    // in different view/containers

    def extractInstancePropertyValue(key: String, value: InstancePropertyValue): Any =
      FlexibleDataModelRelationUtils.extractInstancePropertyValue(dataTypeOf(key), value)

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

  def toRow(properties: Array[Any]): Row =
    new GenericRow(properties.map {
      // For startNode, endNode, type & DirectRelationReference & list of DirectRelationReference
      case a: Array[Array[Any]] => a.map(new GenericRow(_))
      case a: Array[Any] => new GenericRow(a)
      case e => e
    })
}
