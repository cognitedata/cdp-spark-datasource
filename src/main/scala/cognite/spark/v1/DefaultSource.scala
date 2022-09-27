package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.{ApiKeyAuth, BearerTokenAuth, OAuth2, TicketAuth}
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteInternalId, GenericClient}
import fs2.Stream
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import sttp.client3.SttpBackend
import sttp.model.Uri

final case class RelationConfig(
    auth: CdfSparkAuth,
    clientTag: Option[String],
    applicationName: Option[String],
    projectName: String,
    batchSize: Option[Int],
    limitPerPartition: Option[Int],
    partitions: Int,
    maxRetries: Int,
    maxRetryDelaySeconds: Int,
    collectMetrics: Boolean,
    collectTestMetrics: Boolean,
    metricsPrefix: String,
    baseUrl: String,
    onConflict: OnConflictOption,
    applicationId: String,
    parallelismPerPartition: Int,
    ignoreUnknownIds: Boolean,
    deleteMissingAssets: Boolean,
    subtrees: AssetSubtreeOption,
    ignoreNullFields: Boolean,
    rawEnsureParent: Boolean
) {

  /** Desired number of Spark partitions ~= partitions / parallelismPerPartition */
  def sparkPartitions: Int = Math.max(1, partitions / parallelismPerPartition)
}

sealed trait OnConflictOption extends Serializable
object OnConflictOption {
  object Abort extends OnConflictOption
  object Update extends OnConflictOption
  object Upsert extends OnConflictOption
  object Delete extends OnConflictOption
  val fromString: Map[String, OnConflictOption] = Map(
    "abort" -> Abort,
    "update" -> Update,
    "upsert" -> Upsert,
    "delete" -> Delete
  )

  def withNameOpt(s: String): Option[OnConflictOption] =
    fromString.get(s.toLowerCase)
}

sealed trait AssetSubtreeOption extends Serializable
object AssetSubtreeOption {
  object Ingest extends AssetSubtreeOption
  object Ignore extends AssetSubtreeOption
  object Error extends AssetSubtreeOption
  val fromString: Map[String, AssetSubtreeOption] = Map(
    "ingest" -> Ingest,
    "ignore" -> Ignore,
    "error" -> Error
  )
  def withNameOpt(s: String): Option[AssetSubtreeOption] =
    fromString.get(s.toLowerCase)
}

class DefaultSource
    extends RelationProvider
    with CreatableRelationProvider
    with SchemaRelationProvider
    with DataSourceRegister
    with Serializable {
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
        .map(id => CogniteInternalId(id.toLong))
        .orElse(
          parameters.get("externalId").map(CogniteExternalId(_))
        )
        .getOrElse(
          throw new CdfSparkIllegalArgumentException("id or externalId option must be specified.")
        )

    new SequenceRowsRelation(config, sequenceId)(sqlContext)
  }

  private def createDataModelInstances(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) = {
    val spaceExternalId =
      parameters.getOrElse("spaceExternalId", sys.error("spaceExternalId must be specified"))
    val modelExternalId =
      parameters.getOrElse("modelExternalId", sys.error("modelExternalId must be specified"))
    val instanceSpaceExternalId =
      parameters.get("instanceSpaceExternalId")
    new DataModelInstanceRelation(
      config,
      spaceExternalId,
      modelExternalId,
      instanceSpaceExternalId
    )(sqlContext)
  }

  // scalastyle:off cyclomatic.complexity method.length
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {

    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))
    val assetSubtreeIds = parameters.get("assetSubtreeIds").map(_.split(",").toList)
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
        new AssetsRelation(config, assetSubtreeIds)(sqlContext)
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
      case "labels" =>
        new LabelsRelation(config)(sqlContext)
      case "relationships" =>
        new RelationshipsRelation(config)(sqlContext)
      case "datasets" =>
        new DataSetsRelation(config)(sqlContext)
      case "datamodelinstances" =>
        createDataModelInstances(parameters, config, sqlContext)
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
      config.onConflict match {
        case OnConflictOption.Delete =>
          relation.delete(data)
        case _ =>
          relation.buildFromDf(data)
      }
      relation
    } else if (resourceType == "datapoints" || resourceType == "stringdatapoints") {
      val relation = resourceType match {
        case "datapoints" =>
          new NumericDataPointsRelationV1(config)(sqlContext)
        case "stringdatapoints" =>
          new StringDataPointsRelationV1(config)(sqlContext)
      }
      if (config.onConflict == OnConflictOption.Delete) {
        // Datapoints support 100_000 per request when inserting, but only 10_000 when deleting
        val batchSize = config.batchSize.getOrElse(Constants.DefaultDataPointsLimit)
        data.foreachPartition((rows: Iterator[Row]) => {
          import CdpConnector.ioRuntime
          val batches = rows.grouped(batchSize).toVector
          batches.parTraverse_(relation.delete).unsafeRunSync()
        })
      } else {
        // datapoints need special handling of dataframes and batches
        relation.insert(data, overwrite = true)
      }
      relation
    } else {
      val relation = resourceType match {
        case "events" =>
          new EventsRelation(config)(sqlContext)
        case "timeseries" =>
          new TimeSeriesRelation(config)(sqlContext)
        case "assets" =>
          new AssetsRelation(config)(sqlContext)
        case "files" =>
          new FilesRelation(config)(sqlContext)
        case "sequences" =>
          new SequencesRelation(config)(sqlContext)
        case "labels" =>
          new LabelsRelation(config)(sqlContext)
        case "sequencerows" =>
          createSequenceRows(parameters, config, sqlContext)
        case "relationships" =>
          new RelationshipsRelation(config)(sqlContext)
        case "datasets" =>
          new DataSetsRelation(config)(sqlContext)
        case "datamodelinstances" =>
          createDataModelInstances(parameters, config, sqlContext)
        case _ => sys.error(s"Resource type $resourceType does not support save()")
      }
      val batchSizeDefault = relation match {
        case _: SequenceRowsRelation => Constants.DefaultSequenceRowsBatchSize
        case _ => Constants.DefaultBatchSize
      }
      val batchSize = config.batchSize.getOrElse(batchSizeDefault)
      val originalNumberOfPartitions = data.rdd.getNumPartitions
      val idealNumberOfPartitions = config.sparkPartitions

      // If we have very many partitions, it's quite likely that they are significantly uneven.
      // And we will have to limit parallelism on each partition to low number, so the operation could
      // take unnecessarily long time. Rather than risking this, we'll just repartition data in such case.
      // If the number of partitions is reasonable, we avoid the data shuffling
      val (dataRepartitioned, numberOfPartitions) =
        if (originalNumberOfPartitions > 50 && originalNumberOfPartitions > idealNumberOfPartitions) {
          (data.repartition(idealNumberOfPartitions), idealNumberOfPartitions)
        } else {
          (data, originalNumberOfPartitions)
        }
      dataRepartitioned.foreachPartition((rows: Iterator[Row]) => {
        import CdpConnector.ioRuntime

        val maxParallelism = Math.max(1, config.partitions / numberOfPartitions)
        val batches = Stream.fromIterator[IO](rows, chunkSize = batchSize).chunks

        val operation = config.onConflict match {
          case OnConflictOption.Abort =>
            relation.insert(_)
          case OnConflictOption.Upsert =>
            relation.upsert(_)
          case OnConflictOption.Update =>
            relation.update(_)
          case OnConflictOption.Delete =>
            relation.delete(_)
        }

        batches
          .parEvalMapUnordered(maxParallelism) { chunk =>
            operation(chunk.toVector)
          }
          .compile
          .drain
          .unsafeRunSync()
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
          sys.error(s"$parameterName must be 'true' or 'false'")
        }
      case None => defaultValue
    }

  private def toPositiveInt(parameters: Map[String, String], parameterName: String): Option[Int] =
    parameters.get(parameterName).map { intString =>
      val intValue = intString.toInt
      if (intValue < 0) {
        sys.error(s"$parameterName must be greater than or equal to 0")
      }
      intValue
    }

  private def parseSaveMode(parameters: Map[String, String]) = {
    val onConflictName = parameters.getOrElse("onconflict", "ABORT")
    val validOptions = OnConflictOption.fromString.values.mkString(", ")
    OnConflictOption
      .withNameOpt(onConflictName.toUpperCase())
      .getOrElse(throw new CdfSparkIllegalArgumentException(
        s"`$onConflictName` not a valid subtrees option. Valid options are: $validOptions"))
  }

  private[v1] def parseAuth(parameters: Map[String, String])(
      implicit backend: SttpBackend[IO, Any]): Option[CdfSparkAuth] = {
    val authTicket = parameters.get("authTicket").map(ticket => TicketAuth(ticket))
    val bearerToken = parameters.get("bearerToken").map(bearerToken => BearerTokenAuth(bearerToken))
    val apiKey = parameters.get("apiKey").map(apiKey => ApiKeyAuth(apiKey))
    val scopes: List[String] = parameters.get("scopes") match {
      case None => List.empty
      case Some(scopesStr) => scopesStr.split(" ").toList
    }
    val audience = parameters.get("audience")
    val clientCredentials = for {
      tokenUri <- parameters.get("tokenUri").map { tokenUriString =>
        Uri
          .parse(tokenUriString)
          .getOrElse(throw new CdfSparkIllegalArgumentException("Invalid URI in tokenUri parameter"))
      }
      clientId <- parameters.get("clientId")
      clientSecret <- parameters.get("clientSecret")
      project <- parameters.get("project")
      clientCredentials = OAuth2.ClientCredentials(
        tokenUri,
        clientId,
        clientSecret,
        scopes,
        project,
        audience)
    } yield CdfSparkAuth.OAuth2ClientCredentials(clientCredentials)

    val session = for {
      sessionId <- parameters.get("sessionId").map(_.toLong)
      sessionKey <- parameters.get("sessionKey")
      cdfProjectName <- parameters.get("project")
      session = OAuth2.Session(
        parameters.getOrElse("baseUrl", Constants.DefaultBaseUrl),
        sessionId,
        sessionKey,
        cdfProjectName)
    } yield CdfSparkAuth.OAuth2Sessions(session)

    authTicket
      .orElse(apiKey)
      .orElse(bearerToken)
      .map(CdfSparkAuth.Static)
      .orElse(session)
      .orElse(clientCredentials)
  }

  def parseRelationConfig(parameters: Map[String, String], sqlContext: SQLContext): RelationConfig = { // scalastyle:off
    val maxRetries = toPositiveInt(parameters, "maxRetries")
      .getOrElse(Constants.DefaultMaxRetries)
    val maxRetryDelaySeconds = toPositiveInt(parameters, "maxRetryDelay")
      .getOrElse(Constants.DefaultMaxRetryDelaySeconds)
    val baseUrl = parameters.getOrElse("baseUrl", Constants.DefaultBaseUrl)
    val clientTag = parameters.get("clientTag")
    val applicationName = parameters.get("applicationName")

    implicit val backend: SttpBackend[IO, Any] =
      CdpConnector.retryingSttpBackend(maxRetries, maxRetryDelaySeconds)

    val auth = parseAuth(parameters) match {
      case Some(x) => x
      case None =>
        sys.error(
          s"Either apiKey, authTicket, clientCredentials, session or bearerToken is required. Only these options were provided: ${parameters.keys
            .mkString(", ")}")
    }
    val projectName = parameters
      .getOrElse("project", DefaultSource.getProjectFromAuth(auth, baseUrl))
    val batchSize = toPositiveInt(parameters, "batchSize")
    val limitPerPartition = toPositiveInt(parameters, "limitPerPartition")
    val partitions = toPositiveInt(parameters, "partitions")
      .getOrElse(Constants.DefaultPartitions)
    val metricsPrefix = parameters.get("metricsPrefix") match {
      case Some(prefix) => s"$prefix"
      case None => ""
    }
    val collectMetrics = toBoolean(parameters, "collectMetrics")
    val collectTestMetrics = toBoolean(parameters, "collectTestMetrics")

    val saveMode = parseSaveMode(parameters)
    val parallelismPerPartition = {
      toPositiveInt(parameters, "parallelismPerPartition").getOrElse(
        Constants.DefaultParallelismPerPartition)
    }
    // keep compatibility with ignoreDisconnectedAssets
    val subtreesOption =
      (parameters.get("ignoreDisconnectedAssets"), parameters.get("subtrees")) match {
        case (None, None) => AssetSubtreeOption.Ingest
        case (None, Some(subtreeParameter)) =>
          val validOptions = AssetSubtreeOption.fromString.values.mkString(", ")
          AssetSubtreeOption
            .withNameOpt(subtreeParameter)
            .getOrElse(
              throw new CdfSparkIllegalArgumentException(
                s"`$subtreeParameter` not a valid subtrees option. Valid options are: $validOptions"))
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
      clientTag,
      applicationName,
      projectName,
      batchSize,
      limitPerPartition,
      partitions,
      maxRetries,
      maxRetryDelaySeconds,
      collectMetrics,
      collectTestMetrics,
      metricsPrefix,
      baseUrl,
      saveMode,
      Option(sqlContext).map(_.sparkContext.applicationId).getOrElse("CDF"),
      parallelismPerPartition,
      ignoreUnknownIds = toBoolean(parameters, "ignoreUnknownIds", defaultValue = true),
      deleteMissingAssets = toBoolean(parameters, "deleteMissingAssets"),
      subtrees = subtreesOption,
      ignoreNullFields = toBoolean(parameters, "ignoreNullFields", defaultValue = true),
      rawEnsureParent = toBoolean(parameters, "rawEnsureParent", defaultValue = true)
    )
  }

  def getProjectFromAuth(auth: CdfSparkAuth, baseUrl: String)(
      implicit backend: SttpBackend[IO, Any]): String = {
    import CdpConnector.ioRuntime
    val getProject = for {
      authProvider <- auth.provider
      client <- GenericClient
        .forAuthProvider[IO](Constants.SparkDatasourceVersion, authProvider, baseUrl)
    } yield client.projectName
    getProject.unsafeRunSync()
  }
}
