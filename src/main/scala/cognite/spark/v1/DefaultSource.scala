package cognite.spark.v1

import cats.{Apply, Functor}
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.fdm.{FlexibleDataModelBaseRelation, FlexibleDataModelRelationFactory}
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory.{
  ConnectionConfig,
  DataModelConnectionConfig,
  DataModelViewConfig,
  ViewCorePropertyConfig
}
import com.cognite.sdk.scala.common.{BearerTokenAuth, OAuth2, TicketAuth}
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteId, CogniteInternalId, InstanceId}
import fs2.Stream
import io.circe.Decoder
import io.circe.parser.parse
import natchez.{Kernel, Trace}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.log4s.getLogger
import org.typelevel.ci.CIString
import sttp.model.Uri

import scala.reflect.classTag

class DefaultSource
    extends RelationProvider
    with CreatableRelationProvider
    with SchemaRelationProvider
    with DataSourceRegister
    with Serializable {
  import DefaultSource._

  override def shortName(): String = "cognite"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, null)

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

  private def createFlexibleDataModelRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext): FlexibleDataModelBaseRelation = {
    val corePropertySyncRelation = extractCorePropertySyncRelation(parameters, config, sqlContext)
    val corePropertyRelation = extractCorePropertyRelation(parameters, config, sqlContext)
    val datamodelBasedSync = extractDataModelBasedConnectionRelationSync(parameters, config, sqlContext)
    val dataModelBasedConnectionRelation =
      extractDataModelBasedConnectionRelation(parameters, config, sqlContext)
    val dataModelBasedCorePropertyRelation =
      extractDataModelBasedCorePropertyRelation(parameters, config, sqlContext)
    val connectionRelation = extractConnectionRelation(parameters, config, sqlContext)

    corePropertySyncRelation
      .orElse(corePropertyRelation)
      .orElse(datamodelBasedSync)
      .orElse(dataModelBasedConnectionRelation)
      .orElse(dataModelBasedCorePropertyRelation)
      .orElse(connectionRelation)
      .getOrElse(throw new CdfSparkException(
        s"""
             |Invalid combination of arguments!
             |
             | Expecting 'instanceType' and 'cursor' with optional arguments ('viewSpace', 'viewExternalId', 'viewVersion',
             | 'instanceSpace') for CorePropertySyncRelation,
             | or expecting 'instanceType' with optional arguments ('viewSpace', 'viewExternalId', 'viewVersion',
             | 'instanceSpace') for CorePropertyRelation,
             | or expecting ('edgeTypeSpace', 'edgeTypeExternalId') with optional 'instanceSpace' for ConnectionRelation,
             | or expecting ('modelSpace', 'modelExternalId', 'modelVersion', 'viewExternalId') with optional
             | 'instanceSpace' and optional 'cursor' for data model based CorePropertyRelation,
             | or expecting ('modelSpace', 'modelExternalId', 'modelVersion', viewExternalId', 'connectionPropertyName')
             | with optional 'instanceSpace' for data model based  ConnectionRelation,
             |""".stripMargin
      ))
  }

  /**
    * Create a spark relation for reading.
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))

    val config = parseRelationConfig(parameters, sqlContext)

    logger.info(
      s"createRelation(reading): " +
        s"resourceType=${resourceType}, " +
        s"config.parallelismPerPartition=${config.parallelismPerPartition}, " +
        s"config.sparkPartitions=${config.sparkPartitions}")

    resourceType match {
      case NumericDataPointsRelation.name =>
        new NumericDataPointsRelationV1(config)(sqlContext)
      case StringDataPointsRelation.name =>
        new StringDataPointsRelationV1(config)(sqlContext)
      case TimeSeriesRelation.name =>
        new TimeSeriesRelation(config)(sqlContext)
      case RawTableRelation.name =>
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
      case SequenceRowsRelation.name =>
        createSequenceRows(parameters, config, sqlContext)
      case AssetsRelation.name =>
        val subtreeIds = parameters.get("assetSubtreeIds").map(parseCogniteIds)
        new AssetsRelation(config, subtreeIds)(sqlContext)
      case EventsRelation.name =>
        new EventsRelation(config)(sqlContext)
      case FilesRelation.name =>
        new FilesRelation(config)(sqlContext)
      case FileContentRelation.name =>
        val externalId: Option[String] = parameters.get("externalId")
        val instanceId: Option[InstanceId] = for {
          instanceExternalId <- parameters.get("instanceExternalId")
          instanceSpace <- parameters.get("instanceSpace")
        } yield InstanceId(instanceSpace, instanceExternalId)
        val inferSchema = toBoolean(parameters, "inferSchema", defaultValue = true)
        (externalId, instanceId) match {
          case (Some(externalId), None) =>
            new FileContentRelation(config, Left(externalId), inferSchema)(sqlContext)
          case (None, Some(instanceId)) =>
            new FileContentRelation(config, Right(instanceId), inferSchema)(sqlContext)
          case (None, None) =>
            throw new CdfSparkIllegalArgumentException(
              "Either 'externalId' or both 'instanceSpace' and 'instanceExternalId' must be specified for filecontent.")
          case (Some(_), Some(_)) =>
            throw new CdfSparkIllegalArgumentException(
              "Both 'externalId' and ('instanceSpace', 'instanceExternalId') were specified for filecontent. Please provide only one identifier.")
        }
      case ThreeDModelsRelation.name =>
        new ThreeDModelsRelation(config)(sqlContext)
      case ThreeDModelRevisionsRelation.name =>
        val modelId =
          parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        new ThreeDModelRevisionsRelation(config, modelId)(sqlContext)
      case ThreeDModelRevisionMappingsRelation.name =>
        val modelId =
          parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        val revisionId =
          parameters.getOrElse("revisionId", sys.error("Revision id must be specified")).toLong
        new ThreeDModelRevisionMappingsRelation(config, modelId, revisionId)(sqlContext)
      case ThreeDModelRevisionNodesRelation.name =>
        val modelId =
          parameters.getOrElse("modelId", sys.error("Model id must be specified")).toLong
        val revisionId =
          parameters.getOrElse("revisionId", sys.error("Revision id must be specified")).toLong
        new ThreeDModelRevisionNodesRelation(config, modelId, revisionId)(sqlContext)
      case SequenceRelation.name =>
        new SequencesRelation(config)(sqlContext)
      case LabelsRelation.name =>
        new LabelsRelation(config)(sqlContext)
      case RelationshipsRelation.name =>
        new RelationshipsRelation(config)(sqlContext)
      case DataSetsRelation.name =>
        new DataSetsRelation(config)(sqlContext)
      case FlexibleDataModelBaseRelation.name =>
        createFlexibleDataModelRelation(parameters, config, sqlContext)
      case _ => sys.error("Unknown resource type: " + resourceType)
    }
  }

  /**
    * Create a spark relation for writing.
    */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val config = parseRelationConfig(parameters, sqlContext)
    val resourceType = parameters.getOrElse("type", sys.error("Resource type must be specified"))

    logger.info(
      s"createRelation(writing): " +
        s"resourceType=${resourceType}, " +
        s"data.rdd.getNumPartitions=${data.rdd.getNumPartitions}, " +
        s"config.parallelismPerPartition=${config.parallelismPerPartition}, " +
        s"config.sparkPartitions=${config.sparkPartitions}")

    val relation: CdfRelation = resourceType match {
      case AssetHierarchyBuilder.name =>
        new AssetHierarchyBuilder(config)(sqlContext)
      case NumericDataPointsRelation.name =>
        new NumericDataPointsRelationV1(config)(sqlContext)
      case StringDataPointsRelation.name =>
        new StringDataPointsRelationV1(config)(sqlContext)
      case EventsRelation.name =>
        new EventsRelation(config)(sqlContext)
      case TimeSeriesRelation.name =>
        new TimeSeriesRelation(config)(sqlContext)
      case AssetsRelation.name =>
        new AssetsRelation(config)(sqlContext)
      case FilesRelation.name =>
        new FilesRelation(config)(sqlContext)
      case SequenceRelation.name =>
        new SequencesRelation(config)(sqlContext)
      case LabelsRelation.name =>
        new LabelsRelation(config)(sqlContext)
      case SequenceRowsRelation.name =>
        createSequenceRows(parameters, config, sqlContext)
      case RelationshipsRelation.name =>
        new RelationshipsRelation(config)(sqlContext)
      case DataSetsRelation.name =>
        new DataSetsRelation(config)(sqlContext)
      case FlexibleDataModelBaseRelation.name =>
        createFlexibleDataModelRelation(parameters, config, sqlContext)
      case _ => sys.error(s"Resource type $resourceType does not support save()")
    }

    relation match {
      case relation: DataPointsRelationV1[_] =>
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
      case relation: AssetHierarchyBuilder =>
        config.onConflict match {
          case OnConflictOption.Delete =>
            relation.delete(data)
          case _ =>
            relation.buildFromDf(data)
        }
        relation
      case relation: CdfRelation with WritableRelation =>
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
        val dataRepartitioned =
          if (originalNumberOfPartitions > 50 && originalNumberOfPartitions > idealNumberOfPartitions) {
            logger.info(
              s"createRelation(writing): " +
                s"repartition ${resourceType} from ${originalNumberOfPartitions} to " +
                s"${idealNumberOfPartitions} partitions")
            data.repartition(idealNumberOfPartitions)
          } else {
            data
          }

        dataRepartitioned.foreachPartition((rows: Iterator[Row]) => {
          import CdpConnector.ioRuntime

          val maxParallelism = config.parallelismPerPartition
          val batches = Stream.fromIterator[IO](rows, chunkSize = batchSize).chunks

          val operation: Seq[Row] => IO[Unit] = config.onConflict match {
            case OnConflictOption.Abort =>
              relation.insert
            case OnConflictOption.Upsert =>
              relation.upsert
            case OnConflictOption.Update =>
              relation.update
            case OnConflictOption.Delete =>
              relation.delete
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
      case _ =>
        sys.error(s"Resource type $resourceType does not support save()")
    }
  }
}

object DefaultSource {
  @transient private val logger = getLogger

  val sparkFormatString: String = classTag[DefaultSource].runtimeClass.getCanonicalName

  val TRACING_PARAMETER_PREFIX: String = "com.cognite.tracing.parameter."

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

  private[v1] def parseAuth(parameters: Map[String, String]): Option[CdfSparkAuth] = {
    val authTicket = parameters.get("authTicket").map(ticket => TicketAuth(ticket))
    val bearerToken = parameters.get("bearerToken").map(bearerToken => BearerTokenAuth(bearerToken))
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
      .orElse(bearerToken)
      .map(CdfSparkAuth.Static)
      .orElse(session)
      .orElse(clientCredentials)
  }

  def extractTracingHeadersKernel(parameters: Map[String, String]): Kernel =
    new Kernel(
      parameters.toSeq
        .filter(_._1.startsWith(TRACING_PARAMETER_PREFIX))
        .map(kv => (CIString(kv._1.substring(TRACING_PARAMETER_PREFIX.length)), kv._2))
        .toMap)

  def saveTracingHeaders(knl: Kernel): Seq[(String, String)] =
    knl.toHeaders.toList.map(kv => (TRACING_PARAMETER_PREFIX + kv._1, kv._2))

  def saveTracingHeaders[F[_]: Functor: Trace](): F[Seq[(String, String)]] =
    Trace[F].kernel.map(saveTracingHeaders(_))

  def logParameterOrDefault[A](name: String, value: Option[A], default: => A): A = value match {
    case None => logger.info(s"using default value for ${name}: ${default}"); default
    case Some(value) => logger.info(s"using supplied value for ${name}: $value"); value
  }

  def parseRelationConfig(parameters: Map[String, String], sqlContext: SQLContext): RelationConfig = {
    val maxRetries = toPositiveInt(parameters, "maxRetries")
      .getOrElse(Constants.DefaultMaxRetries)
    val initialRetryDelayMillis = toPositiveInt(parameters, "initialRetryDelayMs")
      .getOrElse(Constants.DefaultInitialRetryDelay.toMillis.toInt)
    val maxRetryDelaySeconds = toPositiveInt(parameters, "maxRetryDelay")
      .getOrElse(Constants.DefaultMaxRetryDelaySeconds)
    val baseUrl = parameters.getOrElse("baseUrl", Constants.DefaultBaseUrl)
    val clientTag = parameters.get("clientTag")
    val applicationName = parameters.get("applicationName")

    val auth = parseAuth(parameters) match {
      case Some(x) => x
      case None =>
        sys.error(
          s"Either authTicket, clientCredentials, session or bearerToken is required. Only these options were provided: ${parameters.keys
            .mkString(", ")}")
    }
    val projectName =
      parameters.getOrElse(
        "project",
        throw new CdfSparkIllegalArgumentException(s"`project` must be specified"))
    val batchSize = toPositiveInt(parameters, "batchSize")
    val limitPerPartition = toPositiveInt(parameters, "limitPerPartition")
    val partitions = logParameterOrDefault(
      "partitions",
      toPositiveInt(parameters, "partitions"),
      Constants.DefaultPartitions)
    val metricsPrefix = parameters.get("metricsPrefix") match {
      case Some(prefix) => s"$prefix"
      case None => ""
    }
    val metricsTrackAttempts = toBoolean(parameters, "metricsTrackAttempts")
    val collectMetrics = toBoolean(parameters, "collectMetrics")
    val collectTestMetrics = toBoolean(parameters, "collectTestMetrics")

    val enableSinglePartitionDeleteAssetHierarchy =
      toBoolean(parameters, "enableSinglePartitionDeleteHierarchy", defaultValue = false)

    val saveMode = parseSaveMode(parameters)
    val parallelismPerPartition = logParameterOrDefault(
      "parallelismPerPartition",
      toPositiveInt(parameters, "parallelismPerPartition"),
      Constants.DefaultParallelismPerPartition)
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
      auth = auth,
      clientTag = clientTag,
      applicationName = applicationName,
      projectName = projectName,
      batchSize = batchSize,
      limitPerPartition = limitPerPartition,
      partitions = partitions,
      maxRetries = maxRetries,
      initialRetryDelayMillis = initialRetryDelayMillis,
      maxRetryDelaySeconds = maxRetryDelaySeconds,
      collectMetrics = collectMetrics,
      collectTestMetrics = collectTestMetrics,
      metricsPrefix = metricsPrefix,
      metricsTrackAttempts = metricsTrackAttempts,
      baseUrl = baseUrl,
      onConflict = saveMode,
      applicationId = Option(sqlContext).map(_.sparkContext.applicationId).getOrElse("CDF"),
      parallelismPerPartition = parallelismPerPartition,
      ignoreUnknownIds = toBoolean(parameters, "ignoreUnknownIds", defaultValue = true),
      deleteMissingAssets = toBoolean(parameters, "deleteMissingAssets"),
      subtrees = subtreesOption,
      ignoreNullFields = toBoolean(parameters, "ignoreNullFields", defaultValue = true),
      rawEnsureParent = toBoolean(parameters, "rawEnsureParent", defaultValue = true),
      enableSinglePartitionDeleteAssetHierarchy = enableSinglePartitionDeleteAssetHierarchy,
      tracingParent = extractTracingHeadersKernel(parameters),
      useSharedThrottle = toBoolean(parameters, "useSharedThrottle", defaultValue = false),
      serverSideFilterNullValuesOnNonSchemaRawQueries =
        toBoolean(parameters, "filterNullFieldsOnNonSchemaRawQueries", defaultValue = false),
      maxOutstandingRawInsertRequests = toPositiveInt(parameters, "maxOutstandingRawInsertRequests")
    )
  }

  private[v1] def parseCogniteIds(jsonIds: String): List[CogniteId] = {

    implicit val singleIdDecoder: Decoder[CogniteId] =
      List[Decoder[CogniteId]](
        Decoder.decodeLong
          .validate(_.value.isNumber, "strict int decoder") // to distinguish 123 from "123"
          .map(internalId => CogniteInternalId(internalId)),
        Decoder.decodeString.map(externalId => CogniteExternalId(externalId))
      ).reduceLeft(_.or(_))
    implicit val multipleIdsDecoder: Decoder[List[CogniteId]] =
      List(
        singleIdDecoder.map(List(_)),
        Decoder.decodeArray[CogniteId].map(_.toList)
      ).reduceLeft(_.or(_))

    parse(jsonIds)
      .flatMap(_.as[List[CogniteId]])
      .getOrElse(List(CogniteExternalId(jsonIds)))

  }

  private def extractDataModelBasedCorePropertyRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) = {
    val instanceSpace = parameters.get("instanceSpace")
    Apply[Option]
      .map4(
        parameters.get("modelSpace"),
        parameters.get("modelExternalId"),
        parameters.get("modelVersion"),
        parameters.get("viewExternalId")
      )(DataModelViewConfig(_, _, _, _, instanceSpace))
      .map(FlexibleDataModelRelationFactory.dataModelRelation(config, sqlContext, _))
  }

  private def extractDataModelBasedConnectionRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) = {
    val instanceSpace = parameters.get("instanceSpace")
    Apply[Option]
      .map5(
        parameters.get("modelSpace"),
        parameters.get("modelExternalId"),
        parameters.get("modelVersion"),
        parameters.get("viewExternalId"),
        parameters.get("connectionPropertyName")
      )(DataModelConnectionConfig(_, _, _, _, _, instanceSpace))
      .map(FlexibleDataModelRelationFactory.dataModelRelation(config, sqlContext, _))
  }

  private def extractDataModelBasedConnectionRelationSync(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext): Option[FlexibleDataModelBaseRelation] = {
    val instanceSpace = parameters.get("instanceSpace")
    Apply[Option]
      .map5(
        parameters.get("modelSpace"),
        parameters.get("modelExternalId"),
        parameters.get("modelVersion"),
        parameters.get("viewExternalId"),
        parameters.get("cursor")
      )(Tuple5(_, _, _, _, _))
      .map(t =>
        FlexibleDataModelRelationFactory.dataModelRelationSync(
          t._5,
          parameters.get("cursorName"),
          parameters.get("jobId"),
          parameters.get("syncCursorSaveCallbackUrl"),
          config,
          sqlContext,
          DataModelViewConfig(t._1, t._2, t._3, t._4, instanceSpace)
      ))
  }

  private def extractConnectionRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) = {
    val instanceSpace = parameters.get("instanceSpace")
    Apply[Option]
      .map2(
        parameters.get("edgeTypeSpace"),
        parameters.get("edgeTypeExternalId")
      )(ConnectionConfig(_, _, instanceSpace))
      .map(FlexibleDataModelRelationFactory.connectionRelation(config, sqlContext, _))
  }

  private def extractCorePropertySyncRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) =
    Apply[Option]
      .map2(
        parameters.get("instanceType"),
        parameters.get("cursor")
      )(Tuple2(_, _))
      .map { usageAndCursor =>
        val usage = usageAndCursor._1 match {
          case t if t.equalsIgnoreCase("edge") => Usage.Edge
          case t if t.equalsIgnoreCase("node") => Usage.Node
        }
        val viewReference = Apply[Option]
          .map3(
            parameters.get("viewSpace"),
            parameters.get("viewExternalId"),
            parameters.get("viewVersion")
          )(ViewReference.apply)

        val cursorName = parameters.get("cursorName")
        val jobId = parameters.get("jobId")
        val syncCursorSaveCallbackUrl = parameters.get("syncCursorSaveCallbackUrl")

        FlexibleDataModelRelationFactory.corePropertySyncRelation(
          usageAndCursor._2,
          config = config,
          sqlContext = sqlContext,
          cursorName = cursorName,
          jobId = jobId,
          syncCursorSaveCallbackUrl = syncCursorSaveCallbackUrl,
          viewCorePropConfig = ViewCorePropertyConfig(
            intendedUsage = usage,
            viewReference = viewReference,
            instanceSpace = parameters.get("instanceSpace"))
        )
      }

  private def extractCorePropertyRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext) =
    parameters
      .get("instanceType")
      .collect {
        // converting to `Usage` because when supporting data models, a view could have 'usedFor' set to 'All'
        case t if t.equalsIgnoreCase("edge") => Usage.Edge
        case t if t.equalsIgnoreCase("node") => Usage.Node
      }
      .map { usage =>
        FlexibleDataModelRelationFactory.corePropertyRelation(
          config = config,
          sqlContext = sqlContext,
          ViewCorePropertyConfig(
            intendedUsage = usage,
            viewReference = Apply[Option]
              .map3(
                parameters.get("viewSpace"),
                parameters.get("viewExternalId"),
                parameters.get("viewVersion")
              )(ViewReference.apply),
            instanceSpace = parameters.get("instanceSpace")
          )
        )
      }
}
