package cognite.spark.v1

import cats.Apply
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.FlexibleDataModelRelationFactory.{
  ConnectionConfig,
  DataModelConnectionConfig,
  DataModelViewConfig,
  ViewCorePropertyConfig
}
import cognite.spark.v1.wdl.WellDataLayerRelation
import com.cognite.sdk.scala.common.{BearerTokenAuth, OAuth2, TicketAuth}
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import com.cognite.sdk.scala.v1.{CogniteExternalId, CogniteId, CogniteInternalId}
import fs2.Stream
import io.circe.Decoder
import io.circe.parser.parse
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import sttp.client3.SttpBackend
import sttp.model.Uri

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

  private def createWellDataLayer(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext
  ): WellDataLayerRelation = {
    val model =
      parameters.getOrElse("wdlDataType", throw new CdfSparkException("wdlDataType must be specified"))
    new WellDataLayerRelation(config, model)(sqlContext)
  }

  private def createFlexibleDataModelRelation(
      parameters: Map[String, String],
      config: RelationConfig,
      sqlContext: SQLContext): FlexibleDataModelBaseRelation = {
    val corePropertyRelation = extractCorePropertyRelation(parameters, config, sqlContext)
    val dataModelBasedConnectionRelation =
      extractDataModelBasedConnectionRelation(parameters, config, sqlContext)
    val dataModelBasedCorePropertyRelation =
      extractDataModelBasedCorePropertyRelation(parameters, config, sqlContext)
    val connectionRelation = extractConnectionRelation(parameters, config, sqlContext)

    corePropertyRelation
      .orElse(dataModelBasedConnectionRelation)
      .orElse(dataModelBasedCorePropertyRelation)
      .orElse(connectionRelation)
      .getOrElse(
        throw new CdfSparkException(
          s"""
             |Invalid combination of arguments!
             |
             | Expecting 'instanceType' with optional arguments ('viewSpace', 'viewExternalId', 'viewVersion',
             | 'instanceSpace') for CorePropertyRelation,
             | or expecting ('edgeTypeSpace', 'edgeTypeExternalId') with optional 'instanceSpace' for ConnectionRelation,
             | or expecting ('modelSpace', 'modelExternalId', 'modelVersion', 'viewExternalId') with optional
             | 'instanceSpace' for data model based CorePropertyRelation,
             | or expecting ('modelSpace', 'modelExternalId', 'modelVersion', viewExternalId', 'connectionPropertyName')
             | with optional 'instanceSpace' for data model based  ConnectionRelation,
             |""".stripMargin
        ))
  }

  /**
    * Create a spark relation for reading.
    */
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
        val subtreeIds = parameters.get("assetSubtreeIds").map(parseCogniteIds)
        new AssetsRelation(config, subtreeIds)(sqlContext)
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
      case FlexibleDataModelRelationFactory.ResourceType =>
        createFlexibleDataModelRelation(parameters, config, sqlContext)
      case "welldatalayer" =>
        createWellDataLayer(parameters, config, sqlContext)
      case _ => sys.error("Unknown resource type: " + resourceType)
    }
  }

  case class MultiSpec(types: Map[String, String])
  object MultiSpec {
    def unapply(input: String): Option[MultiSpec] =
      input
        .split(',')
        .toVector
        .traverse { part =>
          part.split(':') match {
            case Array(name, tp) => Some(name -> tp)
            case _ => None
          }
        }
        .map(_.toMap)
        .map(MultiSpec(_))
  }

  private def createSingleWritableRelation(
      resourceType: String,
      config: RelationConfig,
      parameters: Map[String, String],
      sqlContext: SQLContext) = resourceType match {
    case "events" =>
      Some(new EventsRelation(config)(sqlContext))
    case "timeseries" =>
      Some(new TimeSeriesRelation(config)(sqlContext))
    case "assets" =>
      Some(new AssetsRelation(config)(sqlContext))
    case "files" =>
      Some(new FilesRelation(config)(sqlContext))
    case "sequences" =>
      Some(new SequencesRelation(config)(sqlContext))
    case "labels" =>
      Some(new LabelsRelation(config)(sqlContext))
    case "sequencerows" =>
      Some(createSequenceRows(parameters, config, sqlContext))
    case "relationships" =>
      Some(new RelationshipsRelation(config)(sqlContext))
    case "datasets" =>
      Some(new DataSetsRelation(config)(sqlContext))
    case "datamodelinstances" =>
      Some(createDataModelInstances(parameters, config, sqlContext))
    case FlexibleDataModelRelationFactory.ResourceType =>
      Some(createFlexibleDataModelRelation(parameters, config, sqlContext))
    case "welldatalayer" =>
      Some(createWellDataLayer(parameters, config, sqlContext))
    case _ => None
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
        case MultiSpec(MultiSpec(types)) =>
          new MultiRelation(
            config,
            types.map {
              case (name, relationType) =>
                val generalParams = parameters.filterKeys(k => !k.contains(':'))
                val prefix = name + ':'
                val specificParams = parameters.filterKeys(k => k.startsWith(prefix)).map {
                  case (k, v) => k.stripPrefix(prefix) -> v
                }
                val newParams = generalParams ++ specificParams + ("type" -> relationType)
                val newConfig = parseRelationConfig(newParams, sqlContext)
                name -> createSingleWritableRelation(
                  relationType,
                  newConfig,
                  newParams,
                  sqlContext
                ).getOrElse(sys.error(s"Resource type $resourceType does not support multiple outputs"))
            }
          )(sqlContext)
        case _ =>
          createSingleWritableRelation(resourceType, config, parameters, sqlContext).getOrElse(
            sys.error(s"Resource type $resourceType does not support save()"))
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

  def parseRelationConfig(parameters: Map[String, String], sqlContext: SQLContext): RelationConfig = { // scalastyle:off
    val maxRetries = toPositiveInt(parameters, "maxRetries")
      .getOrElse(Constants.DefaultMaxRetries)
    val maxRetryDelaySeconds = toPositiveInt(parameters, "maxRetryDelay")
      .getOrElse(Constants.DefaultMaxRetryDelaySeconds)
    val baseUrl = parameters.getOrElse("baseUrl", Constants.DefaultBaseUrl)
    val clientTag = parameters.get("clientTag")
    val applicationName = parameters.get("applicationName")

    //This backend is used only for auth, so we should not retry as much as maxRetries config
    implicit val authBackend: SttpBackend[IO, Any] =
      CdpConnector.retryingSttpBackend(5, maxRetryDelaySeconds)

    val auth = parseAuth(parameters) match {
      case Some(x) => x
      case None =>
        sys.error(
          s"Either apiKey, authTicket, clientCredentials, session or bearerToken is required. Only these options were provided: ${parameters.keys
            .mkString(", ")}")
    }
    val projectName =
      parameters.getOrElse(
        "project",
        throw new CdfSparkIllegalArgumentException(s"`project` must be specified"))
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
      auth = auth,
      clientTag = clientTag,
      applicationName = applicationName,
      projectName = projectName,
      batchSize = batchSize,
      limitPerPartition = limitPerPartition,
      partitions = partitions,
      maxRetries = maxRetries,
      maxRetryDelaySeconds = maxRetryDelaySeconds,
      collectMetrics = collectMetrics,
      collectTestMetrics = collectTestMetrics,
      metricsPrefix = metricsPrefix,
      baseUrl = baseUrl,
      onConflict = saveMode,
      applicationId = Option(sqlContext).map(_.sparkContext.applicationId).getOrElse("CDF"),
      parallelismPerPartition = parallelismPerPartition,
      ignoreUnknownIds = toBoolean(parameters, "ignoreUnknownIds", defaultValue = true),
      deleteMissingAssets = toBoolean(parameters, "deleteMissingAssets"),
      subtrees = subtreesOption,
      ignoreNullFields = toBoolean(parameters, "ignoreNullFields", defaultValue = true),
      rawEnsureParent = toBoolean(parameters, "rawEnsureParent", defaultValue = true)
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
