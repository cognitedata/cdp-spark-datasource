package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.{CdfRelation, CdfSparkException, RelationConfig, WritableRelation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class WellDataLayerRelation(
    config: RelationConfig,
    model: String
)(override val sqlContext: SQLContext)
    extends CdfRelation(config, "well-data-layer")
    with WritableRelation
    with TableScan
    with Serializable {

  import cognite.spark.v1.CdpConnector._

  override def schema: StructType =
    cachedSchema.getOrElse(
      throw new CdfSparkException("Failed to decode well-data-layer schema into StructType"))

  private lazy val cachedSchema: Option[StructType] = getSchema

  private def getSchema: Option[StructType] = {
    val schemaAsString = client.wdl.getSchema(model).unsafeRunSync()
    DataType.fromJson(schemaAsString) match {
      case s @ StructType(_) => Some(s)
      case _ => None
    }
  }

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val jsonObjects = rows.map(row => {
      val rowJson = RowToJson.toJsonObject(row, schema)
      if (rowJson.isEmpty) {
        throw new CdfSparkException("Upsert invalid empty row!")
      }
      rowJson
    })

    client.wdl.setItems(getWriteUrlPart(model), jsonObjects)
  }

  private val modelTypeToWriteUrlPart = Map(
    "Well" -> "wells",
    "WellSource" -> "wells",
    "Wellbore" -> "wellbores",
    "WellboreSource" -> "wellbores",
    "DepthMeasurement" -> "measurements/depth",
    "TimeMeasurement" -> "measurements/time",
    "RigOperation" -> "rigoperations",
    "HoleSectionGroup" -> "holesections",
    "WellTopGroup" -> "welltops",
    "Npt" -> "npt",
    "Nds" -> "npt",
    "CasingSchematic" -> "casings",
    "Trajectory" -> "trajectories",
    "Source" -> "sources",
  )

  private def getWriteUrlPart(modelType: String): String = {
    val modelKey = modelType.replace("Ingestion", "")
    modelTypeToWriteUrlPart.getOrElse(
      modelKey,
      throw new CdfSparkException(s"Unknown model type: $modelType"))
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] =
    IO.unit

  override def buildScan(): RDD[Row] =
    new WellDataLayerRDD(
      sparkContext = sqlContext.sparkContext,
      schema = schema,
      model = model,
      config = config
    )
}
