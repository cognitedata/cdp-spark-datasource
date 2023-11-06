package cognite.spark.v1.wdl

import cats.implicits._
import cognite.spark.v1.{CdfRelation, CdfSparkException, RelationConfig, TracedIO, WritableRelation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SQLContext}

@deprecated("wdl support is deprecated", since = "0")
class WellDataLayerRelation(
    config: RelationConfig,
    model: String
)(override val sqlContext: SQLContext)
    extends CdfRelation(config, "welldatalayer")
    with WritableRelation
    with TableScan
    with Serializable {

  import cognite.spark.v1.CdpConnector._

  override def schema: StructType =
    cachedSchema.getOrElse(
      throw new CdfSparkException("Failed to decode well-data-layer schema into StructType"))

  private lazy val cachedSchema: Option[StructType] =
    config.trace("getSchema")(getSchema).unsafeRunSync()

  private def getSchema: TracedIO[Option[StructType]] =
    client.wdl
      .getSchema(model)
      .map(schemaAsString =>
        DataType.fromJson(schemaAsString) match {
          case s: StructType => Some(s)
          case _ => None
      })

  override def insert(rows: Seq[Row]): TracedIO[Unit] = upsert(rows)

  override def upsert(rows: Seq[Row]): TracedIO[Unit] = {
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

  override def update(rows: Seq[Row]): TracedIO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  override def delete(rows: Seq[Row]): TracedIO[Unit] = TracedIO.unit

  override def buildScan(): RDD[Row] =
    new WellDataLayerRDD(
      sparkContext = sqlContext.sparkContext,
      schema = schema,
      model = WdlModels.fromRetrievalSchemaName(model),
      config = config
    )
}
