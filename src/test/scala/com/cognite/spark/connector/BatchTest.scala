package com.cognite.spark.connector

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest._

@RunWith(classOf[JUnitRunner])
class BatchTest extends FlatSpec with Matchers {
  "Batch size " should "be limit if limit is smaller than batchSize" in {
    Batch.withCursor(1000, limit=Some(10)) { (thisBatchSize, _: Option[Unit]) =>
      thisBatchSize should be (10)
      (Seq.empty, None)
    }.toList
  }
  it should "be batchSize if limit is larger than batchSize" in {
    Batch.withCursor(100, limit=Some(1000)) { (thisBatchSize, _: Option[Unit]) =>
      thisBatchSize should be (100)
      (Seq.empty, None)
    }.toList
  }

  "Batch process" should "process multiple batches if batchSize < limit" in {
    var numCalls = 0
    val batch = Batch.withCursor(100, limit=Some(1000)) { (thisBatchSize, cursor: Option[Int]) =>
      val curs = cursor.getOrElse(0)
      val upper = thisBatchSize + curs
      val chunk = curs to upper
      numCalls += 1
      (chunk, Some(upper + 1))
    }.toList
    numCalls should be (1000/100)
    batch should be (0 to 1000)
  }

  it should "handle a limit which is not divisible by batchSize" in {
    var numCalls = 0
    val batch = Batch.withCursor(100, limit=Some(199)) { (thisBatchSize, cursor: Option[Int]) =>
      val curs = cursor.getOrElse(0)
      val upper = thisBatchSize + curs
      val chunk = curs to upper
      numCalls += 1
      (chunk, Some(upper + 1))
    }.toList
    numCalls should be (2)
    batch should be (0 to 199)
  }

  it should "stop on empty chunk" in {
    var numCalls = 0
    Batch.withCursor(100, limit=Some(1000)) { (_, _: Option[Unit]) =>
      numCalls += 1
      (Seq.empty, Some())
    }.toList

    numCalls should be (1)
  }
}
