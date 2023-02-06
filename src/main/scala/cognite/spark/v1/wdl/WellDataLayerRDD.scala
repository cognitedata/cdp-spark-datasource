package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.{CdfPartition, CdfSparkException, CdpConnector, RelationConfig}
import com.cognite.sdk.scala.common.ItemsWithCursor
import com.cognite.sdk.scala.v1.GenericClient
import io.circe.JsonObject
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
  import CdpConnector._

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  // scalastyle:off cyclomatic.complexity
  private def getReadUrlPart(modelType: String): String =
    modelType.replace("Ingestion", "") match {
      case "Well" => "wells/list"
      case "DepthMeasurement" => "measurements/depth/list"
      case "TimeMeasurement" => "measurements/time/list"
      case "RigOperation" => "rigoperations/list"
      case "HoleSectionGroup" => "holesections/list"
      case "WellTopGroup" => "welltops/list"
      case "Npt" => "npt/list"
      case "Nds" => "npt/list"
      case "CasingSchematic" => "casings/list"
      case "Trajectory" => "trajectories/list"
      case _ => sys.error(s"Unknown model type: $modelType")
    }
  // scalastyle:on cyclomatic.complexity

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val response: IO[ItemsWithCursor[JsonObject]] = if (model == "Source") {
      client.wdl.listItemsWithGet("sources")
    } else {
      client.wdl.listItemsWithPost(getReadUrlPart(model))
    }
    val responseUnwrapped = response.unsafeRunSync()

    val rows = responseUnwrapped.items.map(jsonObject =>
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
