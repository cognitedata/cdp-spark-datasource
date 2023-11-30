package cognite.spark.v1.wdl

import cognite.spark.v1.{CdfPartition, CdfSparkException, CdpConnector, RelationConfig, TracedIO}
import com.cognite.sdk.scala.common.ItemsWithCursor
import com.cognite.sdk.scala.v1.GenericClient
import io.circe.JsonObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.AbstractIterator

@deprecated("wdl support is deprecated", since = "0")
class WellDataLayerRDD(
    @transient override val sparkContext: SparkContext,
    val schema: StructType,
    val model: WdlModel,
    val config: RelationConfig
) extends RDD[Row](sparkContext, Nil) {
  import CdpConnector._

  @transient lazy val client: GenericClient[TracedIO] = CdpConnector.clientFromConfig(config)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    if (model.retrieve.isGet) {
      val response = config
        .trace("compute")(
          client.wdl
            .listItemsWithGet(model.retrieve.url)
        )
        .unsafeRunSync()

      val rows = response.items.map(jsonObject =>
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

    override def hasNext: Boolean = nextComputed match {
      case Some(_) => true
      case None => false
    }

    override def next(): Row = nextComputed match {
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
      val response = config
        .trace("wdl.computeNext")(
          client.wdl
            .listItemsWithPost(
              model.retrieve.url,
              cursor = itemsWithCursor.nextCursor,
              limit = config.batchSize,
              transformBody = jb => model.retrieve.transformBody(jb)
            )
        )
        .unsafeRunSync()

      processItems(response) match {
        case Some(value) => Some(value)
        case None =>
          if (response.nextCursor.nonEmpty) {
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
