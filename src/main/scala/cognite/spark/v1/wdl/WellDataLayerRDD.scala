package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfPartition, CdfSparkException, RelationConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

class WellDataLayerRDD(
    @transient override val sparkContext: SparkContext,
    val schema: StructType,
    val model: String,
    val config: RelationConfig
) extends RDD[Row](sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val client = WellDataLayerClient.fromConfig(config)
    val response = client.getItems(model)

    val rows = response.items.map(jsonObject =>
      JsonObjectToRow.toRow(jsonObject, schema) match {
        case Some(row) => row
        case None => throw new CdfSparkException("Got null as top level row.")
    })

    rows.iterator
  }

  override protected def getPartitions: Array[Partition] = Array(
    CdfPartition(0)
  )
}
