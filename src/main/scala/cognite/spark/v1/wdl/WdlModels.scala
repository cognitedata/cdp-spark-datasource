package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import io.circe.{Json, JsonObject}

case class WdlModel(
    shortName: String,
    ingest: Option[WdlIngestDefinition],
    retrieve: WdlRetrieveDefinition
)

case class WdlIngestDefinition(
    schemaName: String,
    url: String
)

case class WdlRetrieveDefinition(
    schemaName: String,
    url: String,
    isGet: Boolean = false,
    transformBody: JsonObject => JsonObject = it => it
)

object WdlModels {
  private val models = Seq(
    WdlModel(
      "sources",
      Some(WdlIngestDefinition("Source", "sources")),
      WdlRetrieveDefinition("Source", "sources", isGet = true)
    ),
    WdlModel(
      "wellsources",
      Some(WdlIngestDefinition("WellSource", "wells")),
      WdlRetrieveDefinition("WellSource", "wellsources/list")
    ),
    WdlModel(
      "wells",
      None,
      WdlRetrieveDefinition(
        "Well",
        "wells/list",
        transformBody = jsonObject => jsonObject.add("includeWellbores", Json.fromBoolean(false))
      )
    ),
    WdlModel(
      "wellboresources",
      Some(WdlIngestDefinition("WellboreSource", "wellbores")),
      WdlRetrieveDefinition("WellboreSource", "wellboresources/list")
    ),
    WdlModel(
      "wellbores",
      None,
      WdlRetrieveDefinition("Wellbore", "wellbores/list")
    ),
    WdlModel(
      "depthmeasurements",
      Some(WdlIngestDefinition("DepthMeasurementIngestion", "measurements/depth")),
      WdlRetrieveDefinition("DepthMeasurement", "measurements/depth/list")
    ),
    WdlModel(
      "timemeasurements",
      Some(WdlIngestDefinition("TimeMeasurementIngestion", "measurements/time")),
      WdlRetrieveDefinition("TimeMeasurement", "measurements/time/list")
    ),
    WdlModel(
      "rigoperations",
      Some(WdlIngestDefinition("RigOperationIngestion", "rigoperations")),
      WdlRetrieveDefinition("RigOperation", "rigoperations/list")
    ),
    WdlModel(
      "holesections",
      Some(WdlIngestDefinition("HoleSectionGroupIngestion", "holesections")),
      WdlRetrieveDefinition("HoleSectionGroup", "holesections/list")
    ),
    WdlModel(
      "welltops",
      Some(WdlIngestDefinition("WellTopGroupIngestion", "welltops")),
      WdlRetrieveDefinition("WellTopGroup", "welltops/list")
    ),
    WdlModel(
      "nptevents",
      Some(WdlIngestDefinition("NptIngestion", "npt")),
      WdlRetrieveDefinition("Npt", "npt/list")
    ),
    WdlModel(
      "ndsevents",
      Some(WdlIngestDefinition("NdsIngestion", "nds")),
      WdlRetrieveDefinition("Nds", "nds/list")
    ),
    WdlModel(
      "casings",
      Some(WdlIngestDefinition("CasingSchematicIngestion", "casings")),
      WdlRetrieveDefinition("CasingSchematic", "casings/list")
    ),
    WdlModel(
      "trajectories",
      Some(WdlIngestDefinition("TrajectoryIngestion", "trajectories")),
      WdlRetrieveDefinition("Trajectory", "trajectories/list")
    )
  )

  private val retrievalSchemaNames: Seq[String] = models.map(_.retrieve.schemaName)
  private val schemaNamesForIngestion: Seq[String] = models.flatMap(_.ingest).map(_.schemaName)
  private val shortNames: Seq[String] = models.map(_.shortName)

  def fromRetrievalSchemaName(schemaName: String): WdlModel =
    models
      .find(p => p.retrieve.schemaName == schemaName)
      .getOrElse(
        throw new CdfSparkException(
          s"Invalid schema name: `$schemaName`. The valid options are: [${retrievalSchemaNames.mkString(", ")}]")
      )

  def fromIngestionSchemaName(schemaName: String): WdlModel =
    models
      .find(p => p.ingest.map(_.schemaName).contains(schemaName))
      .getOrElse(
        throw new CdfSparkException(
          s"Invalid schema name: `$schemaName`. The valid options are: [${schemaNamesForIngestion.mkString(", ")}]")
      )

  def fromShortName(shortName: String): WdlModel =
    models
      .find(p => p.shortName.toLowerCase() == shortName.toLowerCase())
      .getOrElse(
        throw new CdfSparkException(
          s"Invalid well data layer data type: `$shortName`. The valid options are: [${shortNames.mkString(", ")}]")
      )
}
