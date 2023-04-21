package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.{CdfRelation, CdfSparkException, RelationConfig, WritableRelation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import natchez.Trace

class WellDataLayerRelation(
    config: RelationConfig,
    model: String
)(override val sqlContext: SQLContext)(implicit val trace: Trace[IO])
    extends CdfRelation(config, "welldatalayer")
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
      case s: StructType => Some(s)
      case _ => None
    }
  }

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val jsonObjects = rows.map(row => {
      val rowJson = RowToJson.toJsonObject(row, schema, None)
      if (rowJson.isEmpty) {
        throw new CdfSparkException("Upsert invalid empty row!")
      }
      rowJson
    })

    val url = WdlModels.fromIngestionSchemaName(model).ingest.getOrElse(sys.error("Unreachable")).url

    client.wdl.setItems(url, jsonObjects).flatTap(_ => incMetrics(itemsCreated, jsonObjects.size))
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] = IO.unit

  override def buildScan(): RDD[Row] =
    new WellDataLayerRDD(
      sparkContext = sqlContext.sparkContext,
      schema = schema,
      model = WdlModels.fromRetrievalSchemaName(model),
      config = config
    )
}
