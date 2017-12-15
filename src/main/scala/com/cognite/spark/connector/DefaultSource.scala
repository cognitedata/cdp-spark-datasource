package com.cognite.spark.connector

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister {

  override def shortName(): String = "timeseries"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val apiKey = parameters.getOrElse("apiKey", sys.error("ApiKey must be specified."))
    val project = parameters.getOrElse("project", sys.error("Project must be specified"))
    val path = parameters.getOrElse("path", sys.error("TagId must be specified (as a parameter to load())"))
    val batchSize = parameters.getOrElse("batchsize", "1000").toInt
    new FullScanRelations(apiKey = apiKey,
      project = project,
      path = path,
      suppliedSchema = schema,
      batchSize = batchSize,
      start = parameters.get("start").map(v => v.toLong).orElse(None),
      stop = parameters.get("stop").map(v => v.toLong).orElse(None)
    )(sqlContext)
  }
}
