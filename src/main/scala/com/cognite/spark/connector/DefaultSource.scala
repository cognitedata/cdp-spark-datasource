package com.cognite.spark.connector

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister {

  override def shortName(): String = "cognite"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val apiKey = parameters.getOrElse("apiKey", sys.error("ApiKey must be specified."))
    val project = parameters.getOrElse("project", sys.error("Project must be specified"))
    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))
    val batchSize = try {
      parameters.get("batchSize").map(_.toInt)
    } catch {
      case _: NumberFormatException => None
    }
    val limit = try {
      parameters.get("limit").map(_.toInt)
    } catch {
      case _: NumberFormatException => None
    }
    resourceType match {
      case "timeseries" =>
        val tagId = parameters.getOrElse("tagId", sys.error("tagId must be specified"))
        val aggregates = parameters.get("aggregates")
        val granularity = parameters.get("granularity")
        new TimeSeriesRelation(apiKey, project, tagId, schema, limit, batchSize, aggregates, granularity)(sqlContext)
      case "tables" =>
        val database = parameters.getOrElse("database", sys.error("Database must be specified"))
        val tableName = parameters.getOrElse("table", sys.error("Table must be specified"))
        val inferSchema = parameters.get("inferSchema") match {
          case Some("true") => true
          case Some("false") => false
          case Some(_) => sys.error("inferSchema must be 'true' or 'false'")
          case None => false
        }
        val inferSchemaLimit = try {
          Some(parameters.get("inferSchemaLimit").get.toInt)
        } catch {
          case _: NumberFormatException => sys.error("inferSchemaLimit must be an integer")
          case _: NoSuchElementException => None
        }
        new RawTableRelation(apiKey, project, database, tableName, Option(schema), limit,
          inferSchema, inferSchemaLimit, batchSize)(sqlContext)
      case "assets" =>
        val assetsPath = parameters.get("assetsPath")
        if (assetsPath.isDefined && !AssetsTableRelation.isValidAssetsPath(assetsPath.get)) {
          sys.error("Invalid assets path: " + assetsPath.get)
        }
        new AssetsTableRelation(apiKey, project, assetsPath, limit, batchSize)(sqlContext)
      case _ => sys.error("Unknown resource type: " + resourceType)
    }
  }
}
