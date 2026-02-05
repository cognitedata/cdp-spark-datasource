package cognite.spark.v1.fdm.RelationUtils

import cognite.spark.v1.CdfSparkException
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.{
  ConnectionDefinition,
  ViewCorePropertyDefinition,
  ViewPropertyDefinition
}
import com.cognite.sdk.scala.v1.fdm.common.sources.SourceReference
import org.apache.spark.sql.types.StructType

import scala.util.Try

object Validators {
  def validateSourceSchema(
      source: Option[SourceReference],
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition]): Either[CdfSparkException, Boolean] =
    source match {
      case Some(_) =>
        validateRowFieldsWithPropertyDefinitions(schema, propertyDefMap)
      case None =>
        Right(true)
    }

  private def validateRowFieldsWithPropertyDefinitions(
      schema: StructType,
      propertyDefMap: Map[String, ViewPropertyDefinition]): Either[CdfSparkException, Boolean] = {

    val (_, propsMissingInSchema) = propertyDefMap.partition {
      case (propName, _) => Try(schema.fieldIndex(propName)).isSuccess
    }
    val (_, nonNullablePropsMissingInSchema) =
      propsMissingInSchema.partition {
        case (_, corePropDef: ViewCorePropertyDefinition) => corePropDef.nullable.getOrElse(true)
        case (_, _: ConnectionDefinition) => true
      }

    if (nonNullablePropsMissingInSchema.nonEmpty) {
      val propsAsStr = nonNullablePropsMissingInSchema.keys.mkString(", ")
      Left(new CdfSparkException(s"Could not find required properties: [$propsAsStr]"))
    } else {
      Right(true)
    }
  }
}
