package com.cognite.spark.datasource

import org.scalactic.Equality
import org.scalatest.{FlatSpec, Matchers}

class DataPointsRddTest extends FlatSpec with Matchers {
  implicit val dataPointsRddPartitionEquality1 =
    new Equality[DataPointsRddPartition] {
      override def areEqual(a: DataPointsRddPartition, b: Any): Boolean = {
        b match {
          case p: DataPointsRddPartition =>
            a.startTime == p.startTime && a.endTime == p.endTime && a.index == p.index
          case _ => false
        }
      }

    }
  "DataPointsRdd" should "split time intervals into some number of approximately equally sized windows" in {
    val parts1 = DataPointsRdd.intervalPartitions(1550241236999L, 1550244237001L, 86400000L, 1)
    parts1 should contain theSameElementsInOrderAs Array(DataPointsRddPartition(1550241236999L, 1550244237001L, 0))
    val parts2 = DataPointsRdd.intervalPartitions(0L, 10000, 1000, 10)
    parts2 should contain theSameElementsInOrderAs 0.until(10000).by(1000).zipWithIndex.map {
      case (start, index) => DataPointsRddPartition(start, start + 1000, index)
    }
    val parts3 = DataPointsRdd.intervalPartitions(0, 2500, 1000, 3)
    parts3 should contain theSameElementsInOrderAs Array(DataPointsRddPartition(0, 1000, 0), DataPointsRddPartition(1000, 2500, 1))
  }
}
