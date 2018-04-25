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
        val tagId = parameters.getOrElse("path", sys.error("tagId must be specified (as a parameter to load())"))
        new TimeSeriesRelation(apiKey, project, tagId, schema, limit, batchSize)(sqlContext)
      case "tables" =>
        val database = parameters.getOrElse("database", sys.error("Database must be specified"))
        val tableName = parameters.getOrElse("path", sys.error("table name must be specified (as a parameter to load())"))
        new RawTableRelation(apiKey, project, database, tableName, Option(schema), limit, batchSize)(sqlContext)
      case "assets" =>
        val assetsPath = parameters.get("path")
        if (assetsPath.isDefined && !AssetsTableRelation.isValidAssetsPath(assetsPath.get)) {
          sys.error("Invalid assets path: " + assetsPath.get)
        }
        new AssetsTableRelation(apiKey, project, assetsPath, limit, batchSize)(sqlContext)
      case _ => sys.error("Unknown resource type: " + resourceType)
    }
  }
}
