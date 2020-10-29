package cognite.spark.v1

import cats.data.NonEmptyList
import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

private[spark] object SparkSchemaHelperRuntime {
  private def simplifyTypeName(name: String) =
    name match {
      case "java.time.Instant" => "Timestamp"
      case "java.lang.Integer" => "Int"
      case "java.lang.Long" => "Long"
      case "java.lang.Double" => "Double"
      case "java.lang.String" => "String"
      case x => x
    }

  def rowIdentifier(row: Row): String = {
    val columns = row.schema.fieldNames
    if (columns.contains("externalId")) {
      s"with externalId='${row.getAs[Any]("externalId")}'"
    } else if (columns.contains("id")) {
      s"with id='${row.getAs[Any]("id")}'"
    } else if (columns.contains("name")) {
      s"with name='${row.getAs[Any]("name")}'"
    } else {
      row.toString
    }
  }

  private def valueToString(value: Any) =
    if (value == null) {
      "NULL"
    } else {
      s"'$value' of type ${simplifyTypeName(value.getClass.getName)}"
    }

  // convert Map[Any, Any] to Map[String, String] by checking every key and value.
  def checkMetadataMap(mapAny: Map[Any, Any], row: Row): Map[String, String] = {
    val map = mapAny.asInstanceOf[Map[Any, Any]]
    val badKeys = map.keys
      .filter(!_.isInstanceOf[String])
      .map(k =>
        s"Map with string keys was expected, but ${valueToString(k)} was found (on row ${rowIdentifier(row)}).")

    val badValues = map
      .filter { case (k, v) => !v.isInstanceOf[String] && v != null }
      .map {
        case (k, v) =>
          s"Map with string values was expected, but ${valueToString(v)} was found (under key '$k' on row ${rowIdentifier(row)})"
      }

    (badKeys ++ badValues).headOption match {
      case Some(error) => throw new CdfSparkIllegalArgumentException(error)
      case None => filterMetadata(map.asInstanceOf[Map[String, String]])
    }
  }

  def badRowError(row: Row, name: String, typeName: String, rowType: String): Throwable =
    Try(row.getAs[Any](name)) match {
      case Failure(error) =>
        new CdfSparkIllegalArgumentException(
          s"Required column '$name' is missing on row [${row.schema.fieldNames.mkString(", ")}].")
      case Success(value) =>
        val hint =
          if (rowType == "cognite.spark.v1.AssetsIngestSchema" && name == "parentExternalId" && value == null) {
            " To mark the node as root, please use an empty string ('')."
          } else {
            ""
          }

        // this function is invoked only in case of an error -> we have some type issues
        val valueString = valueToString(value)
        new CdfSparkIllegalArgumentException(s"Column '$name' was expected to have type ${simplifyTypeName(
          typeName)}, but $valueString was found (on row ${rowIdentifier(row)}).$hint")
    }

  case class PathSegment(description: String, key: String, expectedType: String)

  def fromRowError(row: Row, typeName: String, path: NonEmptyList[PathSegment], value: Any): Throwable = {
    val reversePath = path//.reverse // Convert from bottom-up to top-down

    val traceback =
      reversePath.map {
        case PathSegment(description, key, expectedType) =>
          s"$description $key ($expectedType)"
      }
      .toList
      .mkString(" in ")

    val valueStr =
      if (value == null) { "NULL" }
      else { s"$value (${value.getClass.getName})" }

    val message = (
      s"""Row cannot be converted into a $typeName:
         |The $traceback was expected to have a value of type ${reversePath.head.expectedType}, but found $valueStr""".stripMargin
    )

    new CdfSparkIllegalArgumentException(message)
  }

  // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
  // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
  // Since null keys don't make sense to CDP either, remove them as well.
  def filterMetadata(metadata: Map[String, String]): Map[String, String] =
    metadata
      .filter { case (k, v) => k != null && v != null }
}
