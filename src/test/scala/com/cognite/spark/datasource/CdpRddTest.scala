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
  class TestRdd(batchSize: Int, limit: Option[Int]) extends CdpRdd[Number](spark.sparkContext, (n: Number) => Row(n.number),
    uri"http://localhost/api", uri"http://localhost/api",
    "apikey", "project", batchSize, limit) {
  }

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

  "CdpRdd" should "fetch the correct number of cursors, and set None as the cursor for the first partition" in {
    val rdd = new TestRdd(2, Some(10)) {
      val expectedCursors: Iterator[Option[String]] =
        Iterator(Option.empty[String]) ++ Stream.from(0).map(n => Some(n.toString)).toIterator
      val numberedItems = NumberedItems(Stream.from(0).map(_.toString).toIterator)
      override def getJson[A: Decoder](apiKey: String, url: Uri, maxRetries: StatusCode): IO[A] = {
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
    val rddInfiniteCursors = new TestRdd(5, Some(10)) {
      val numberedItems = NumberedItems(Stream.from(0).map(_.toString).toIterator)
      override def getJson[A: Decoder](apiKey: String, url: Uri, maxRetries: StatusCode): IO[A] = {
        url.paramsMap.get("limit") should be(Some("5"))
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rddInfiniteCursors.partitions should have length 2
    rddInfiniteCursors.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should
      contain only(None, Some("0"))

    val rddLimit1 = new TestRdd(5, Some(1)) {
      val numberedItems = NumberedItems(Stream.from(0).map(_.toString).toIterator)
      override def getJson[A: Decoder](apiKey: String, url: Uri, maxRetries: StatusCode): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rddLimit1.partitions should have length 1
    rddLimit1.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should contain only None

    val rddFiveCursors = new TestRdd(5, Some(10)) {
      val numberedItems = NumberedItems((1 to 5).map(_.toString).toIterator)
      override def getJson[A: Decoder](apiKey: String, url: Uri, maxRetries: StatusCode): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rddFiveCursors.partitions should have length 2
    rddFiveCursors.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should contain only (Some("1"), None)
  }

  it should "not require a limit to be set" in {
    val rdd = new TestRdd(300, None) {
      val numberedItems = NumberedItems((1 to 5).map(_.toString).toIterator)
      override def getJson[A: Decoder](apiKey: String, url: Uri, maxRetries: StatusCode): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rdd.partitions should have length 6
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should
      contain only (Seq(None) ++ (1 to 5).map(x => Some(x.toString)): _*)
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].size) should contain only(Some(300), None)
  }

  it should "not set a cursor if the batch size is larger than the limit, and set the partition size equal to the limit" in {
    val rdd = new TestRdd(300, Some(5)) {
      val numberedItems = NumberedItems((1 to 5).map(_.toString).toIterator)
      override def getJson[A: Decoder](apiKey: String, url: Uri, maxRetries: StatusCode): IO[A] = {
        IO.pure(decode(numberedItems.next).right.get)
      }
    }

    rdd.partitions should have length 1
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].cursor) should contain only None
    rdd.partitions.map(p => p.asInstanceOf[CdpRddPartition].size) should contain only Some(5)
  }
}
