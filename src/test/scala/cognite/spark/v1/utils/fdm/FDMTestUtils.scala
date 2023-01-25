package cognite.spark.v1.utils.fdm

import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ContainerPropertyDefinition,
  ViewPropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{
  DirectNodeRelationProperty,
  PrimitiveProperty,
  TextProperty
}
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefaultValue,
  PropertyType
}
import com.cognite.sdk.scala.v1.fdm.containers._
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.{EdgeWrite, NodeWrite}
import com.cognite.sdk.scala.v1.fdm.instances.{
  DirectRelationReference,
  EdgeOrNodeData,
  InstancePropertyValue
}
import io.circe.{Json, JsonObject}

import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import scala.util.Random

object FDMTestUtils {

  val AllContainerPropertyTypes: List[PropertyType] = List[PropertyType](
    TextProperty(),
    PrimitiveProperty(`type` = PrimitivePropType.Boolean),
    PrimitiveProperty(`type` = PrimitivePropType.Float32),
    PrimitiveProperty(`type` = PrimitivePropType.Float64),
    PrimitiveProperty(`type` = PrimitivePropType.Int32),
    PrimitiveProperty(`type` = PrimitivePropType.Int64),
    PrimitiveProperty(`type` = PrimitivePropType.Timestamp),
    PrimitiveProperty(`type` = PrimitivePropType.Date),
    PrimitiveProperty(`type` = PrimitivePropType.Json),
    TextProperty(list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Boolean, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Float32, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Float64, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Int32, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Int64, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Timestamp, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Date, list = Some(true)),
    PrimitiveProperty(`type` = PrimitivePropType.Json, list = Some(true)),
    DirectNodeRelationProperty(container = None)
  )

  val AllPropertyDefaultValues: List[PropertyDefaultValue] = List(
    PropertyDefaultValue.String("abc"),
    PropertyDefaultValue.Boolean(true),
    PropertyDefaultValue.Int32(101),
    PropertyDefaultValue.Int64(Long.MaxValue),
    PropertyDefaultValue.Float32(101.1f),
    PropertyDefaultValue.Float64(Double.MaxValue),
    PropertyDefaultValue.Object(
      Json.fromJsonObject(
        JsonObject.fromMap(
          Map(
            "a" -> Json.fromString("a"),
            "b" -> Json.fromInt(1),
            "c" -> Json.fromFloatOrString(2.1f)
          )
        )
      )
    )
  )

  def toViewPropertyDefinition(
      containerPropDef: ContainerPropertyDefinition,
      containerRef: Option[ContainerReference],
      containerPropertyIdentifier: Option[String]): ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = containerPropDef.nullable,
      autoIncrement = containerPropDef.autoIncrement,
      defaultValue = containerPropDef.defaultValue,
      description = containerPropDef.description,
      name = containerPropDef.name,
      `type` = containerPropDef.`type`,
      container = containerRef,
      containerPropertyIdentifier = containerPropertyIdentifier
    )

  // scalastyle:off cyclomatic.complexity method.length
  def createAllPossibleContainerPropCombinations: Map[String, ContainerPropertyDefinition] = {
    val boolOptions = List(
      true,
      false
    )

    (for {
      p <- AllContainerPropertyTypes
      nullable <- boolOptions
      withDefault <- boolOptions
    } yield {
      val defaultValue = propertyDefaultValueForPropertyType(p, withDefault)

      val autoIncrementApplicableProp = p match {
        case PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) => true
        case PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) => true
        case _ => false
      }

      val autoIncrement = autoIncrementApplicableProp && !nullable && !withDefault

      val alwaysNullable = p match {
        case DirectNodeRelationProperty(_) => true
        case _ => false
      }
      val nullability = alwaysNullable || nullable

      val nameComponents = Vector(
        p match {
          case p: PrimitiveProperty => p.`type`.productPrefix
          case _ => p.getClass.getSimpleName
        },
        if (p.isList) "List" else "NonList",
        if (autoIncrementApplicableProp) {
          if (autoIncrement) "WithAutoIncrement" else "WithoutAutoIncrement"
        } else { "" },
        if (defaultValue.nonEmpty) "WithDefaultValue" else "WithoutDefaultValue",
        if (nullability) "Nullable" else "NonNullable"
      )

      s"${nameComponents.mkString("")}" -> ContainerPropertyDefinition(
        nullable = Some(nullability),
        autoIncrement = Some(autoIncrement),
        defaultValue = defaultValue,
        description = Some(s"Test ${nameComponents.mkString(" ")} Description"),
        name = Some(s"Test-${nameComponents.mkString("-")}-Name"),
        `type` = p
      )
    }).toMap
  }
  // scalastyle:on cyclomatic.complexity method.length

  def createAllPossibleViewPropCombinations: Map[String, ViewPropertyDefinition] =
    createAllPossibleContainerPropCombinations.map {
      case (key, prop) => key -> toViewPropertyDefinition(prop, None, None)
    }

  def viewPropStr: Vector[String] =
    createAllPossibleViewPropCombinations.map {
      case (propName, prop) =>
        val propTypeStr = prop.`type` match {
          case t: TextProperty =>
            val collation = t.collation.map(s => s""""$s"""")
            s"""PropertyType.TextProperty(${t.list}, $collation)"""
          case p: PrimitiveProperty =>
            s"PropertyType.PrimitiveProperty(PrimitivePropType.${p.`type`},${p.list})"
          case d: DirectNodeRelationProperty => d.toString
        }

        val defaultValueStr = prop.defaultValue.map {
          case PropertyDefaultValue.String(value) =>
            s"""Some(PropertyDefaultValue.String("$value"))""".stripMargin
          case PropertyDefaultValue.Float32(value) =>
            s"""Some(PropertyDefaultValue.Float32(${value}F))""".stripMargin
          case PropertyDefaultValue.Object(value) =>
            val jsonStr = s"""${value.noSpaces}""".stripMargin
            s"""io.circe.parser.parse("$jsonStr"").toOption.map(PropertyDefaultValue.Object)""".stripMargin
          case p => s"Some(PropertyDefaultValue.$p)"
        }

        s"""
       | val $propName: ViewPropertyDefinition = ViewPropertyDefinition(
       |      nullable = ${prop.nullable},
       |      autoIncrement = ${prop.autoIncrement},
       |      defaultValue = ${defaultValueStr.getOrElse("None")},
       |      description = Some("${prop.description.get}"),
       |      name = Some("${prop.name.get}"),
       |      `type` = $propTypeStr,
       |      container = None,
       |      containerPropertyIdentifier = None
       |    )
       |""".stripMargin
    }.toVector

  def createTestContainer(
      space: String,
      containerExternalId: String,
      usage: Usage
  ): ContainerCreateDefinition = {
    val allPossibleProperties: Map[String, ContainerPropertyDefinition] =
      createAllPossibleContainerPropCombinations
    val allPossiblePropertyKeys = allPossibleProperties.keys.toList

    val constraints: Map[String, ContainerConstraint] = Map(
      "uniqueConstraint" -> ContainerConstraint.UniquenessConstraint(
        allPossiblePropertyKeys.take(5)
      )
    )

    val indexes: Map[String, IndexDefinition] = Map(
      "index1" -> IndexDefinition.BTreeIndexDefinition(allPossiblePropertyKeys.take(2)),
      "index2" -> IndexDefinition.BTreeIndexDefinition(allPossiblePropertyKeys.slice(5, 7))
    )

    val containerToCreate = ContainerCreateDefinition(
      space = space,
      externalId = containerExternalId,
      name = Some(s"Test-${usage.productPrefix}-Container-$containerExternalId-Name"),
      description = Some(s"Test ${usage.productPrefix} Container $containerExternalId Description"),
      usedFor = Some(usage),
      properties = allPossibleProperties,
      constraints = Some(constraints),
      indexes = Some(indexes)
    )

    containerToCreate
  }

  def createInstancePropertyForContainerProperty(
      propName: String,
      containerPropType: PropertyType
  ): InstancePropertyValue =
    if (containerPropType.isList) {
      listContainerPropToInstanceProperty(propName, containerPropType)
    } else {
      nonListContainerPropToInstanceProperty(propName, containerPropType)
    }

  def createNodeWriteData(container: ContainerDefinition): NodeWrite =
    container.usedFor match {
      case Usage.Edge =>
        throw new IllegalArgumentException(s"Container: ${container.externalId} doesn't support nodes!")
      case _ =>
        val space = container.space
        val containerRef = container.toSourceReference
        val containerProps = container.properties
        val instanceValuesForProps = containerProps.map {
          case (propName, prop) =>
            propName -> createInstancePropertyForContainerProperty(propName, prop.`type`)
        }
        val (nullables, nonNullables) = instanceValuesForProps.partition {
          case (propName, _) =>
            containerProps(propName).nullable.getOrElse(true)
        }

        val nodeData = if (nullables.isEmpty) {
          List(toInstanceData(containerRef, nonNullables))
        } else {
          val withAllProps = toInstanceData(containerRef, instanceValuesForProps)
          val withRequiredProps = toInstanceData(containerRef, nonNullables)
          val withEachNonRequired = nullables.map {
            case (propName, propVal) =>
              toInstanceData(containerRef, nonNullables + (propName -> propVal))
          }
          List(withAllProps, withRequiredProps) ++ withEachNonRequired
        }

        NodeWrite(
          space,
          s"node_instances_${container.externalId}",
          nodeData
        )
    }

  def createEdgeWriteData(
      container: ContainerDefinition,
      startNode: DirectRelationReference,
      endNode: DirectRelationReference
  ): EdgeWrite =
    container.usedFor match {
      case Usage.Node =>
        throw new IllegalArgumentException(s"Container: ${container.externalId} doesn't support edges!")
      case _ =>
        val space = container.space
        val containerRef = container.toSourceReference
        val containerProps = container.properties
        val instanceValuesForProps = containerProps.map {
          case (propName, prop) =>
            propName -> createInstancePropertyForContainerProperty(propName, prop.`type`)
        }
        val (nullables, nonNullables) = instanceValuesForProps.partition {
          case (propName, _) =>
            containerProps(propName).nullable.getOrElse(true)
        }

        val nodeData = if (nullables.isEmpty) {
          List(toInstanceData(containerRef, nonNullables))
        } else {
          val withAllProps = toInstanceData(containerRef, instanceValuesForProps)
          val withRequiredProps = toInstanceData(containerRef, nonNullables)
          val withEachNonRequired = nullables.map {
            case (propName, propVal) =>
              toInstanceData(containerRef, nonNullables + (propName -> propVal))
          }
          List(withAllProps, withRequiredProps) ++ withEachNonRequired
        }

        val edgeExternalId = s"edge_instances_${container.externalId}"
        EdgeWrite(
          `type` = DirectRelationReference(space, edgeExternalId),
          space = space,
          externalId = edgeExternalId,
          startNode = startNode,
          endNode = endNode,
          nodeData
        )
    }

  // scalastyle:off cyclomatic.complexity
  private def listContainerPropToInstanceProperty(
      propName: String,
      containerPropType: PropertyType
  ): InstancePropertyValue =
    containerPropType match {
      case PropertyType.TextProperty(Some(true), _) =>
        InstancePropertyValue.StringList(List(s"${propName}Value1", s"${propName}Value2"))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
        InstancePropertyValue.BooleanList(List(true, false, true, false))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
        InstancePropertyValue.Int32List((1 to 10).map(_ => Random.nextInt(10000)).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
        InstancePropertyValue.Int64List((1 to 10).map(_ => Random.nextLong()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
        InstancePropertyValue.Float32List((1 to 10).map(_ => Random.nextFloat()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
        InstancePropertyValue.Float64List((1 to 10).map(_ => Random.nextDouble()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
        InstancePropertyValue.DateList(
          (1 to 10).toList.map(i => LocalDate.now().minusDays(i.toLong))
        )
      case PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
        InstancePropertyValue.TimestampList(
          (1 to 10).toList.map(i => ZonedDateTime.now().minusDays(i.toLong))
        )
      case PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)) =>
        InstancePropertyValue.ObjectList(
          List(
            Json.fromJsonObject(
              JsonObject.fromMap(
                Map(
                  "a" -> Json.fromString("a"),
                  "b" -> Json.fromInt(1),
                  "c" -> Json.fromBoolean(true)
                )
              )
            ),
            Json.fromJsonObject(
              JsonObject.fromMap(
                Map(
                  "a" -> Json.fromString("b"),
                  "b" -> Json.fromInt(1),
                  "c" -> Json.fromBoolean(false),
                  "d" -> Json.fromDoubleOrString(1.56)
                )
              )
            )
          )
        )
      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  private def nonListContainerPropToInstanceProperty(
      propName: String,
      containerPropType: PropertyType
  ): InstancePropertyValue =
    containerPropType match {
      case PropertyType.TextProperty(None | Some(false), _) =>
        InstancePropertyValue.String(s"${propName}Value")
      case PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, _) =>
        InstancePropertyValue.Boolean(false)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
        InstancePropertyValue.Int32(Random.nextInt(10000))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
        InstancePropertyValue.Int64(Random.nextLong())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float32, None | Some(false)) =>
        InstancePropertyValue.Float32(Random.nextFloat())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
        InstancePropertyValue.Float64(Random.nextDouble())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
        InstancePropertyValue.Date(LocalDate.now().minusDays(Random.nextInt(30).toLong))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
        InstancePropertyValue.Timestamp(ZonedDateTime.now().minusDays(Random.nextInt(30).toLong))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
        InstancePropertyValue.Object(
          Json.fromJsonObject(
            JsonObject.fromMap(
              Map(
                "a" -> Json.fromString("a"),
                "b" -> Json.fromInt(1),
                "c" -> Json.fromBoolean(true)
              )
            )
          )
        )
      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }
  // scalastyle:on cyclomatic.complexity

  private def toInstanceData(
      containerRef: ContainerReference,
      instancePropertyValues: Map[String, InstancePropertyValue]
  ) =
    EdgeOrNodeData(
      source = containerRef,
      properties = Some(instancePropertyValues)
    )
  // scalastyle:off cyclomatic.complexity
  private def propertyDefaultValueForPropertyType(
      p: PropertyType,
      withDefault: Boolean
  ): Option[PropertyDefaultValue] =
    if (withDefault && !p.isList) {
      p match {
        case TextProperty(_, _) => Some(PropertyDefaultValue.String("defaultTextValue"))
        case PrimitiveProperty(PrimitivePropType.Boolean, _) =>
          Some(PropertyDefaultValue.Boolean(false))
        case PrimitiveProperty(PrimitivePropType.Float32, _) =>
          Some(PropertyDefaultValue.Float32(1.2f))
        case PrimitiveProperty(PrimitivePropType.Float64, _) =>
          Some(PropertyDefaultValue.Float64(1.21))
        case PrimitiveProperty(PrimitivePropType.Int32, _) => Some(PropertyDefaultValue.Int32(1))
        case PrimitiveProperty(PrimitivePropType.Int64, _) => Some(PropertyDefaultValue.Int64(12L))
        case PrimitiveProperty(PrimitivePropType.Timestamp, _) =>
          Some(
            PropertyDefaultValue.String(
              LocalDateTime
                .now()
                .atZone(ZoneId.of("UTC"))
                .format(InstancePropertyValue.Timestamp.formatter)
            )
          )
        case PrimitiveProperty(PrimitivePropType.Date, _) =>
          Some(
            PropertyDefaultValue.String(
              LocalDate.now().format(InstancePropertyValue.Date.formatter)
            )
          )
        case PrimitiveProperty(PrimitivePropType.Json, _) =>
          Some(
            PropertyDefaultValue.Object(
              Json.fromJsonObject(
                JsonObject.fromMap(
                  Map(
                    "a" -> Json.fromString("a"),
                    "b" -> Json.fromInt(1)
                  )
                )
              )
            )
          )
        case DirectNodeRelationProperty(_) => None
      }
    } else {
      None
    }
  // scalastyle:on cyclomatic.complexity

}
