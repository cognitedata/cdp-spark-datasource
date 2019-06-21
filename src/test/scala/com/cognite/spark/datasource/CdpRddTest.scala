package com.cognite.spark.datasource

import cats.effect.IO
import com.softwaremill.sttp._
import io.circe.Decoder
import io.circe.syntax._
import io.circe.generic.auto._
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import io.circe.parser.decode

case class Number(number: Int)

class CdpRddTest extends FlatSpec with Matchers with SparkTest {
  val defaultConfig = getDefaultConfig(ApiKeyAuth(System.getenv("TEST_API_KEY_WRITE")))
  class TestRdd(config: RelationConfig, nextCursorIterator: Iterator[(Option[String], Option[Int])])
    extends CdpRdd[Number](spark.sparkContext, (n: Number) => Row(n.number),
      uri"http://localhost/api",
      config,
      Seq[PushdownFilter](),
      nextCursorIterator)

  case class NumberedItems(nextCursor: Iterator[String]) extends Iterator[String] {
    var hasNext = true

    def next: String = {
      val cursor = if (nextCursor.hasNext) {
        Some(nextCursor.next)
      } else {
        hasNext = false
        None
      }

      Data[ItemsWithCursor[Number]](
        ItemsWithCursor[Number](Seq.empty, cursor)).asJson.toString
    }
  }

  private def makeCursorIterator(config: RelationConfig, nItems: Int): Iterator[(Option[String], Option[Int])] = {
    val iterator = Iterator((Option.empty[String], config.batchSize)) ++
      0.until(nItems - 2).map(n => (Some(n.toString), config.batchSize))
    if (nItems == 1) {
      Iterator((Option.empty[String],
        (config.batchSize, config.limit) match {
          case (Some(batchSize), Some(limit)) => Some(scala.math.min(batchSize, limit))
          case (None, Some(limit)) => Some(limit)
          case (Some(batchSize), None) => Some(batchSize)
          case (None, None) => None
        }))
    } else if (nItems > 1) {
      iterator ++ Iterator((Some((nItems - 2).toString), None))
    } else {
      iterator
    }
  }

  "CdpRdd" should "fetch the correct number of cursors, and set None as the cursor for the first partition" in {
    val config = defaultConfig.copy(batchSize = Some(2), limit = Some(10))
    val cursors = makeCursorIterator(config, 5)
    val rdd: TestRdd = new TestRdd(config, cursors) {
      val expectedCursors: Iterator[Option[String]] =
        Iterator(Option.empty[String]) ++ Stream.from(0).map(n => Some(n.toString)).toIterator
      val numberedItems = NumberedItems(Stream.from(0).map(_.toString).toIterator)
      override def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
        url.paramsMap.get("limit") should be(Some("2"))
        url.paramsMap.get("cursor") should be(expectedCursors.next)
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rdd.partitions should have length 5
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should
      contain only (Seq(None) ++ (0 to 3).map(x => Some(x.toString)): _*)
  }

  it should "use the batch size argument to create the correct number of partitions" in {
    val config = defaultConfig.copy(batchSize = Some(5), limit = Some(10))
    val cursors = makeCursorIterator(config, 2)
    val rddInfiniteCursors: TestRdd = new TestRdd(config, cursors) {
      val numberedItems = NumberedItems(Stream.from(0).map(_.toString).toIterator)
      override def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
        url.paramsMap.get("limit") should be(Some("5"))
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rddInfiniteCursors.partitions should have length 2
    rddInfiniteCursors.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should
      contain only(None, Some("0"))

    val config1 = defaultConfig.copy(batchSize = Some(5), limit = Some(1))
    val cursors1 = makeCursorIterator(config, 1)
    val rddLimit1: TestRdd = new TestRdd(config1, cursors1) {
      val numberedItems = NumberedItems(Stream.from(0).map(_.toString).toIterator)
      override def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rddLimit1.partitions should have length 1
    rddLimit1.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should contain only None

    val cursorsFiveCursors = makeCursorIterator(config, 2)
    val rddFiveCursors: TestRdd = new TestRdd(config, cursorsFiveCursors) {
      val numberedItems = NumberedItems((1 to 5).map(_.toString).toIterator)
      override def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rddFiveCursors.partitions should have length 2
    rddFiveCursors.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should contain only (Some("0"), None)
  }

  it should "not require a limit to be set" in {
    val config = defaultConfig.copy(batchSize = Some(300), limit = None)
    val cursors = makeCursorIterator(config, 6)
    val rdd: TestRdd = new TestRdd(config, cursors) {
      val numberedItems = NumberedItems((1 to 5).map(_.toString).toIterator)
      override def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rdd.partitions should have length 6
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should
      contain only (Seq(None) ++ (0 to 4).map(x => Some(x.toString)): _*)
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].size) should contain only(Some(300), None)
  }

  it should "not set a cursor if the batch size is larger than the limit, and set the partition size equal to the limit" in {
    val config = defaultConfig.copy(batchSize = Some(300), limit = Some(5))
    val cursors = makeCursorIterator(config, 1)
    val rdd: TestRdd = new TestRdd(config, cursors) {
      val numberedItems = NumberedItems((1 to 5).map(_.toString).toIterator)
      override def getJson[A: Decoder](config: RelationConfig, url: Uri): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rdd.partitions should have length 1
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should contain only None
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].size) should contain only Some(5)
  }
}
