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

import scala.collection.AbstractIterator

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

  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    if (model == "Source") {
      val response: IO[ItemsWithCursor[JsonObject]] = client.wdl.listItemsWithGet("sources")
      val responseUnwrapped = response.unsafeRunSync()

      val rows = responseUnwrapped.items.map(jsonObject =>
        JsonObjectToRow.toRow(jsonObject, schema) match {
          case Some(row) => row
          case None => throw new CdfSparkException("Got null as top level row.")
      })

      rows.iterator
    } else {
      makeComputeIterator()
    }

  private def makeComputeIterator(): Iterator[Row] = new AbstractIterator[Row] {
    private[this] var nextComputed: Option[(Row, ItemsWithCursor[JsonObject])] =
      computeNext(ItemsWithCursor[JsonObject](Seq.empty), isFirstQuery = true) // scalafix:ok

    def hasNext: Boolean = nextComputed match {
      case Some(_) => true
      case None => false
    }

    def next(): Row = nextComputed match {
      case Some((head, tail)) =>
        nextComputed = computeNext(tail, isFirstQuery = false)
        head
      case None => throw new NoSuchElementException()
    }
  }

  private def processItems(itemsWithCursor: ItemsWithCursor[JsonObject]) = {
    val (headItem, tailItems) = itemsWithCursor.items.splitAt(1)
    if (headItem.nonEmpty) {
      val jsonObject = headItem.head
      val headRow = JsonObjectToRow.toRow(jsonObject, schema) match {
        case Some(row) => row
        case None => throw new CdfSparkException("Got null as top level row.")
      }
      Some((headRow, ItemsWithCursor(tailItems, nextCursor = itemsWithCursor.nextCursor)))
    } else {
      None
    }
  }

  private def computeNext(
      itemsWithCursor: ItemsWithCursor[JsonObject],
      isFirstQuery: Boolean): Option[(Row, ItemsWithCursor[JsonObject])] =
    if (itemsWithCursor.items.nonEmpty) {
      processItems(itemsWithCursor)
    } else if (isFirstQuery || itemsWithCursor.nextCursor.nonEmpty) {
      val response: IO[ItemsWithCursor[JsonObject]] =
        client.wdl.listItemsWithPost(
          getReadUrlPart(model),
          cursor = itemsWithCursor.nextCursor,
          limit = config.batchSize)
      val responseUnwrapped = response.unsafeRunSync()

      processItems(responseUnwrapped) match {
        case Some(value) => Some(value)
        case None =>
          if (responseUnwrapped.nextCursor.nonEmpty) {
            throw new CdfSparkException("Got empty items and non-empty nextCursor from WDL.")
          } else {
            None
          }
      }
    } else {
      None
    }

  override protected def getPartitions: Array[Partition] = Array(
    CdfPartition(0)
  )
}
