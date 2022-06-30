package cognite.spark.v1

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
    val mapStringString = mapAny.collect {
      case (k: String, v: String) =>
        (k, v)
      case (k: String, null) => // scalastyle:ignore null
        (k, null: String) // scalastyle:ignore null
    }
    val maybeError = mapAny.view
      .map {
        case (k: String, _) if mapStringString.contains(k) =>
          None // No problem here.
        case (k: String, v) =>
          Some(
            s"Map with string values was expected, but ${valueToString(v)} was found (under key '$k' on row ${rowIdentifier(row)})")
        case (k, _) =>
          Some(
            s"Map with string keys was expected, but ${valueToString(k)} was found (on row ${rowIdentifier(row)}).")
      }
      .find(_.isDefined)
      .flatten

    maybeError match {
      case Some(error) => throw new CdfSparkIllegalArgumentException(error)
      case None => filterMetadata(mapStringString)
    }
  }

  def badRowError(row: Row, name: String, typeName: String, rowType: String): Throwable =
    Try(row.getAs[Any](name)) match {
      case Failure(_) =>
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

  // null values aren't allowed according to our schema, and also not allowed by CDP, but they can
  // still end up here. Filter them out to avoid null pointer exceptions from Circe encoding.
  // Since null keys don't make sense to CDP either, remove them as well.
  def filterMetadata(metadata: Map[String, String]): Map[String, String] =
    metadata
      .filter { case (k, v) => k != null && v != null }
}
