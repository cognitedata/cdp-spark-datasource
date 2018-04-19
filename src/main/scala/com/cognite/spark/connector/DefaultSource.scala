package com.cognite.spark.connector

import java.lang.NumberFormatException

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
    val path = parameters.getOrElse("path", sys.error("path (tagId or table name) must be specified (as a parameter to load())"))
    resourceType match {
      case "timeseries" =>
        new FullScanRelations(apiKey = apiKey,
          project = project,
          path = path,
          suppliedSchema = schema,
          batchSize = parameters.getOrElse("batchSize", "1000").toInt,
          start = parameters.get("start").map(v => v.toLong).orElse(None),
          stop = parameters.get("stop").map(v => v.toLong).orElse(None)
        )(sqlContext)
      case "tables" =>
        val database = parameters.getOrElse("database", sys.error("Database must be specified"))
        val batchSize = try {
          parameters.get("batchSize").map(_.toInt)
        } catch {
          case _: NumberFormatException => None
        }
        val limit = try {
          parameters.get("limit").map(_.toLong)
        } catch {
          case _: NumberFormatException => None
        }
        new RawTableRelation(apiKey, project, database, path,
          Option(schema),
          limit,
          batchSize)(sqlContext)
    }
  }
}
