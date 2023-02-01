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

  override def schema: StructType = {
    val schemaAsString = client.wdl.getSchema(model).unsafeRunSync()
    DataType.fromJson(schemaAsString) match {
      case s @ StructType(_) => s
      case _ => throw new CdfSparkException("Failed to decode well-data-layer schema into StructType")
    }
  }

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val jsonObjects = rows.map(row => {
      val rowJson = RowToJson.toJsonObject(row, schema)
      if (rowJson.isEmpty) {
        sys.error("Upsert invalid empty row!")
      }
      rowJson
    })

    client.wdl.setItems(getWriteUrlPart(model), jsonObjects).unsafeRunSync()

    IO.unit
  }

  private def getWriteUrlPart(modelType: String): String =
    modelType.replace("Ingestion", "") match {
      case "Well" => "wells"
      case "WellSource" => "wells"
      case "Wellbore" => "wellbores"
      case "Npt" => "npt"
      case "Nds" => "npt"
      case "CasingSchematic" => "casings"
      case "Trajectory" => "trajectories"
      case "Source" => "sources"
      case _ => sys.error(s"Unknown model type: $modelType")
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
