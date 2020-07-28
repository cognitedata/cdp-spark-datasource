package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.{ApiKeyAuth, Auth, BearerTokenAuth}
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteInternalId, GenericClient}
import com.softwaremill.sttp.SttpBackend
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

final case class RelationConfig(
    auth: Auth,
    projectName: String,
    batchSize: Option[Int],
    limitPerPartition: Option[Int],
    partitions: Int,
    maxRetries: Int,
    collectMetrics: Boolean,
    metricsPrefix: String,
    baseUrl: String,
    onConflict: OnConflict.Value,
    applicationId: String,
    parallelismPerPartition: Int,
    ignoreUnknownIds: Boolean,
    deleteMissingAssets: Boolean,
    subtrees: AssetSubtreeOption.AssetSubtreeOption,
    legacyNameSource: LegacyNameSource.Value
)

object OnConflict extends Enumeration {
  type Mode = Value
  val Abort, Update, Upsert, Delete = Value

  def withNameOpt(s: String): Option[Value] = values.find(_.toString.toLowerCase == s.toLowerCase())
}

object LegacyNameSource extends Enumeration {
  type LegacyNameSource = Value
  val None, Name, ExternalId = Value

  def fromSparkOption(configValue: Option[String]): LegacyNameSource =
    configValue.map(_.toLowerCase).getOrElse("false") match {
      case "false" => LegacyNameSource.None
      case "true" | "name" => LegacyNameSource.Name
      case "externalid" => LegacyNameSource.ExternalId
      case invalid =>
        throw new CdfSparkIllegalArgumentException(s"Invalid value for useLegacyName: $invalid")
    }
}

object AssetSubtreeOption extends Enumeration {
  type AssetSubtreeOption = Value
  val Ingest, Ignore, Error = Value

  def withNameOpt(s: String): Option[Value] = values.find(_.toString.toLowerCase == s.toLowerCase)
}

class DefaultSource
    extends RelationProvider
    with CreatableRelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {
  import DefaultSource._

  override def shortName(): String = "cognite"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, null) // scalastyle:off null

  private def createSequenceRows(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) = {
    val sequenceId =
      parameters
        .get("id")
        .map(id => CogniteInternalId(id.toInt))
        .orElse(
          parameters.get("externalId").map(CogniteExternalId)
        )
        .getOrElse(
          sys.error("id or externalId option must be specified.")
        )

    new SequenceRowsRelation(config, sequenceId)(sqlContext)
  }

  // scalastyle:off cyclomatic.complexity method.length
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))
    val config = parseRelationConfig(parameters, sqlContext)
    resourceType match {
      case "datapoints" =>
        new NumericDataPointsRelationV1(config)(sqlContext)
      case "stringdatapoints" =>
        new StringDataPointsRelationV1(config)(sqlContext)
      case "timeseries" =>
        new TimeSeriesRelation(config)(sqlContext)
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

        new RawTableRelation(
          config,
          database,
          tableName,
          Option(schema),
          inferSchema,
          inferSchemaLimit,
          collectSchemaInferenceMetrics)(sqlContext)
      case "sequencerows" =>
        createSequenceRows(parameters, config, sqlContext)
      case "assets" =>
        new AssetsRelation(config)(sqlContext)
      case "events" =>
        new EventsRelation(config)(sqlContext)
      case "files" =>
        new FilesRelation(config)(sqlContext)
      case "3dmodels" =>
        new ThreeDModelsRelation(config)(sqlContext)
      case "3dmodelrevisions" =>
        val modelId =
          parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        new ThreeDModelRevisionsRelation(config, modelId)(sqlContext)
      case "3dmodelrevisionmappings" =>
        val modelId =
          parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        val revisionId =
          parameters.getOrElse("revisionId", sys.error("Revision id must be specified")).toLong
        new ThreeDModelRevisionMappingsRelation(config, modelId, revisionId)(sqlContext)
      case "3dmodelrevisionnodes" =>
        val modelId =
          parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        val revisionId =
          parameters.getOrElse("revisionId", sys.error("Revision id must be specified")).toLong
        new ThreeDModelRevisionNodesRelation(config, modelId, revisionId)(sqlContext)
      case "sequences" =>
        new SequencesRelation(config)(sqlContext)
      case _ => sys.error("Unknown resource type: " + resourceType)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val config = parseRelationConfig(parameters, sqlContext)
    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))
    if (resourceType == "assethierarchy") {
      val relation = new AssetHierarchyBuilder(config)(sqlContext)
      relation.build(data).unsafeRunSync()
      relation
    } else {
      val relation = resourceType match {
        case "events" =>
          new EventsRelation(config)(sqlContext)
        case "timeseries" =>
          new TimeSeriesRelation(config)(sqlContext)
        case "assets" =>
          new AssetsRelation(config)(sqlContext)
        case "datapoints" =>
          new NumericDataPointsRelationV1(config)(sqlContext)
        case "stringdatapoints" =>
          new StringDataPointsRelationV1(config)(sqlContext)
        case "files" =>
          new FilesRelation(config)(sqlContext)
        case "sequences" =>
          new SequencesRelation(config)(sqlContext)
        case "sequencerows" =>
          createSequenceRows(parameters, config, sqlContext)
        case _ => sys.error(s"Resource type $resourceType does not support save()")
      }
      val batchSize = relation match {
        case _: NumericDataPointsRelationV1 => Constants.CreateDataPointsLimit
        case _: StringDataPointsRelationV1 => Constants.CreateDataPointsLimit
        case _: SequenceRowsRelation => Constants.DefaultSequenceRowsBatchSize
        case _ => Constants.DefaultBatchSize
      }
      data.foreachPartition((rows: Iterator[Row]) => {
        import CdpConnector._

        val batches = rows.grouped(batchSize).toVector
        config.onConflict match {
          case OnConflict.Abort =>
            batches.parTraverse(relation.insert).unsafeRunSync()
          case OnConflict.Upsert =>
            batches.parTraverse(relation.upsert).unsafeRunSync()
          case OnConflict.Update =>
            batches.parTraverse(relation.update).unsafeRunSync()
          case OnConflict.Delete =>
            batches.parTraverse(relation.delete).unsafeRunSync()
        }

        ()
      })
      relation
    }
  }
}

object DefaultSource {
  private def toBoolean(
      parameters: Map[String, String],
      parameterName: String,
      defaultValue: Boolean = false): Boolean =
    parameters.get(parameterName) match {
      case Some(string) =>
        if (string.equalsIgnoreCase("true")) {
          true
        } else if (string.equalsIgnoreCase("false")) {
          false
        } else {
          sys.error("$parameterName must be 'true' or 'false'")
        }
      case None => defaultValue
    }

  private def toPositiveInt(parameters: Map[String, String], parameterName: String): Option[Int] =
    parameters.get(parameterName).map { intString =>
      val intValue = intString.toInt
      if (intValue <= 0) {
        sys.error(s"$parameterName must be greater than 0")
      }
      intValue
    }

  private def parseSaveMode(parameters: Map[String, String]) = {
    val onConflictName = parameters.getOrElse("onconflict", "ABORT")
    OnConflict
      .withNameOpt(onConflictName.toUpperCase())
      .getOrElse(throw new CdfSparkIllegalArgumentException(
        s"$onConflictName is not a valid onConflict option. Please choose one of the following options instead: ${OnConflict.values
          .mkString(", ")}"))
  }

  def parseRelationConfig(parameters: Map[String, String], sqlContext: SQLContext): RelationConfig = { // scalastyle:off
    val maxRetries = toPositiveInt(parameters, "maxRetries")
      .getOrElse(Constants.DefaultMaxRetries)
    val baseUrl = parameters.getOrElse("baseUrl", Constants.DefaultBaseUrl)
    val bearerToken = parameters
      .get("bearerToken")
      .map(bearerToken => BearerTokenAuth(bearerToken))
    val apiKey = parameters
      .get("apiKey")
      .map(apiKey => ApiKeyAuth(apiKey))
    val auth = apiKey
      .orElse(bearerToken)
      .getOrElse(sys.error("Either apiKey or bearerToken is required."))
    val projectName = parameters
      .getOrElse("project", DefaultSource.getProjectFromAuth(auth, maxRetries, baseUrl))
    val batchSize = toPositiveInt(parameters, "batchSize")
    val limitPerPartition = toPositiveInt(parameters, "limitPerPartition")
    val partitions = toPositiveInt(parameters, "partitions")
      .getOrElse(Constants.DefaultPartitions)
    val metricsPrefix = parameters.get("metricsPrefix") match {
      case Some(prefix) => s"$prefix"
      case None => ""
    }
    val collectMetrics = toBoolean(parameters, "collectMetrics")
    val saveMode = parseSaveMode(parameters)
    val parallelismPerPartition = {
      toPositiveInt(parameters, "parallelismPerPartition").getOrElse(
        Constants.DefaultParallelismPerPartition)
    }
    // keep compatibility with ignoreDisconnectedAssets
    val subtreesOption =
      (parameters.get("ignoreDisconnectedAssets"), parameters.get("subtrees")) match {
        case (None, None) => AssetSubtreeOption.Ingest
        case (None, Some(x)) =>
          AssetSubtreeOption
            .withNameOpt(x)
            .getOrElse(
              throw new CdfSparkIllegalArgumentException(
                s"`$x` is an invalid value for option subtree. You can use ingest, ignore or value."))
        case (Some(_), None) =>
          if (toBoolean(parameters, "ignoreDisconnectedAssets")) {
            AssetSubtreeOption.Ignore
          } else {
            // error was the previous default. If somebody was explicit about ignoreDisconnectedAssets=false, then error
            AssetSubtreeOption.Error
          }
        case (Some(ignoreDisconnectedAssets), Some(subtree)) =>
          throw new CdfSparkIllegalArgumentException(
            s"Can not specify both ignoreDisconnectedAssets=$ignoreDisconnectedAssets and subtree=$subtree")
      }
    RelationConfig(
      auth,
      projectName,
      batchSize,
      limitPerPartition,
      partitions,
      maxRetries,
      collectMetrics,
      metricsPrefix,
      baseUrl,
      saveMode,
      Option(sqlContext).map(_.sparkContext.applicationId).getOrElse("CDF"),
      parallelismPerPartition,
      ignoreUnknownIds = toBoolean(parameters, "ignoreUnknownIds", defaultValue = true),
      deleteMissingAssets = toBoolean(parameters, "deleteMissingAssets"),
      subtrees = subtreesOption,
      legacyNameSource = LegacyNameSource.fromSparkOption(parameters.get("useLegacyName"))
    )
  }

  def getProjectFromAuth(auth: Auth, maxRetries: Int, baseUrl: String): String = {
    implicit val backend: SttpBackend[IO, Nothing] = CdpConnector.retryingSttpBackend(maxRetries)
    val getProject = for {
      client <- GenericClient.forAuth[IO](Constants.SparkDatasourceVersion, auth, baseUrl)
    } yield client.projectName
    getProject.unsafeRunSync()
  }
}
