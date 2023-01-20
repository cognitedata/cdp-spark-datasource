package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.{CdfSparkException, RelationConfig, WritableRelation}
import com.cognite.sdk.scala.common.Items
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class WellDataLayerRelation(
    config: RelationConfig,
    model: String,
)(override val sqlContext: SQLContext)
    extends BaseRelation
    with WritableRelation
    with TableScan
    with Serializable {

  @transient private lazy val client = WdlClient.fromConfig(config)

  override def schema: StructType = client.getSchema(model)

  override def insert(rows: Seq[Row]): IO[Unit] = upsert(rows)

  override def upsert(rows: Seq[Row]): IO[Unit] = {
    val jsons = rows.map(row => {
      val jsonObj = RowConversion.convertToJson(row, schema)
      if (jsonObj == null) {
        sys.error("This json object is null!")
      }
      jsonObj
    })

    val items = Items(jsons)
    client.setItems(model, items)

    IO.unit
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update is not supported for WDL. Use upsert instead.")

  override def delete(rows: Seq[Row]): IO[Unit] =
    IO.unit

  override def buildScan(): RDD[Row] =
    new WdlRDD(
      sparkContext = sqlContext.sparkContext,
      schema = schema,
      model = model,
      config = config,
    )
}
