package com.cognite.spark.datasource

import java.util.concurrent.Executors

import cats.effect.{IO, Timer}
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

import scala.concurrent.ExecutionContext

class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister
  with CdpConnector {

  override def shortName(): String = "cognite"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null) // scalastyle:off null
  }

  private def toBoolean(parameters: Map[String, String], parameterName: String): Boolean = {
    parameters.get(parameterName) match {
      case Some(string) =>
        if (string.equalsIgnoreCase("true")) {
          true
        } else if (string.equalsIgnoreCase("false")) {
          false
        } else {
          sys.error("$parameterName must be 'true' or 'false'")
        }
      case None => false
    }
  }

  private def toPositiveInt(parameters: Map[String, String], parameterName: String): Option[Int] = {
    parameters.get(parameterName).map { intString =>
      val intValue = intString.toInt
      if (intValue <= 0) {
        sys.error(s"$parameterName must be greater than 0")
      }
      intValue
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val maxRetries = toPositiveInt(parameters, "maxRetries")
    val apiKey = parameters.getOrElse("apiKey", sys.error("ApiKey must be specified."))
    val project = getProject(apiKey, maxRetries.getOrElse(Constants.DefaultMaxRetries))
    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))
    val batchSize = toPositiveInt(parameters, "batchSize")
    val limit = toPositiveInt(parameters, "limit")
    val metricsPrefix = parameters.get("metricsPrefix") match {
      case Some(prefix) => s"$prefix."
      case None => ""
    }
    val collectMetrics = toBoolean(parameters, "collectMetrics")
    resourceType match {
      case "datapoints" =>
        new DataPointsRelation(apiKey, project, Option(schema), limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "timeseries" =>
        new TimeSeriesRelation(apiKey, project, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "raw" =>
        val database = parameters.getOrElse("database", sys.error("Database must be specified"))
        val tableName = parameters.getOrElse("table", sys.error("Table must be specified"))

        val inferSchema = toBoolean(parameters, "inferSchema")
        val inferSchemaLimit = try {
          Some(parameters("inferSchemaLimit").toInt)
        } catch {
          case _: NumberFormatException => sys.error("inferSchemaLimit must be an integer")
          case _: NoSuchElementException => None
        }
        val collectSchemaInferenceMetrics = toBoolean(parameters, "collectSchemaInferenceMetrics")

        new RawTableRelation(apiKey, project, database, tableName, Option(schema), limit,
          inferSchema, inferSchemaLimit, batchSize, maxRetries,
          metricsPrefix, collectMetrics, collectSchemaInferenceMetrics)(sqlContext)
      case "assets" =>
        val assetsPath = parameters.get("assetsPath")
        if (assetsPath.isDefined && !AssetsRelation.isValidAssetsPath(assetsPath.get)) {
          sys.error("Invalid assets path: " + assetsPath.get)
        }
        new AssetsRelation(apiKey, project, assetsPath, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "events" =>
        new EventsRelation(apiKey, project, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "files" =>
        new FilesRelation(apiKey, project, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "3dmodels" =>
        new ThreeDModelsRelation(apiKey, project, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "3dmodelrevisions" =>
        val modelId = parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        new ThreeDModelRevisionsRelation(apiKey, project, modelId, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "3dmodelrevisionmappings" =>
        val modelId = parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        val revisionId = parameters.getOrElse("revisionId", sys.error("Revision id must be specified")).toLong
        new ThreeDModelRevisionMappingsRelation(apiKey, project, modelId, revisionId, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case "3dmodelrevisionnodes" =>
        val modelId = parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        val revisionId = parameters.getOrElse("revisionId", sys.error("Revision id must be specified")).toLong
        new ThreeDModelRevisionNodesRelation(apiKey, project, modelId, revisionId, limit, batchSize, maxRetries, metricsPrefix, collectMetrics)(sqlContext)
      case _ => sys.error("Unknown resource type: " + resourceType)
    }
  }
  // scalastyle:on cyclomatic.complexity method.length
}
