package org.apache.spark.datasource

import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class MetricsSourceTest extends FlatSpec with Matchers with ParallelTestExecution {

  behavior.of("A metric source")

  it should "detect attempt parameters from the name" in {
    val name = ":sid:1:sat:22:pid:333:tat:4444:my.name:goes.here"

    name match {
      case MetricsSource.metricNameRegex(stageId, stageAttempt, partitionId, taskAttempt, name) => {
        assert(stageId === "1")
        assert(stageAttempt === "22")
        assert(partitionId === "333")
        assert(taskAttempt === "4444")
        assert(name === "my.name:goes.here")
      }
      case _ => {
        fail("Regex did not capture the expected fields")
      }
    }
  }

  it should "detect accept empty parameters from the name" in {
    val name = ":sid::sat::pid::tat::my.name:goes.here"

    name match {
      case MetricsSource.metricNameRegex(stageId, stageAttempt, partitionId, taskAttempt, name) => {
        assert(stageId.isEmpty)
        assert(stageAttempt.isEmpty)
        assert(partitionId.isEmpty)
        assert(taskAttempt.isEmpty)
        assert(name === "my.name:goes.here")
      }
      case _ => {
        fail("Regex did not capture the expected fields")
      }
    }
  }

  it should "work with old name format" in {
    val name = "my.name:goes.here"

    name match {
      case MetricsSource.metricNameRegex(stageId, stageAttempt, partitionId, taskAttempt, name) => {
        assert(stageId === null)
        assert(stageAttempt === null)
        assert(partitionId === null)
        assert(taskAttempt === null)
        assert(name === "my.name:goes.here")
      }
      case _ => {
        fail("Regex did not capture the expected fields")
      }
    }
  }

  it should "not get confused with names that resemble attempt tracking" in {
    val name = ":sid:1:my:name:goes:here:"

    name match {
      case MetricsSource.metricNameRegex(stageId, stageAttempt, partitionId, taskAttempt, name) => {
        assert(stageId === null)
        assert(stageAttempt === null)
        assert(partitionId === null)
        assert(taskAttempt === null)
        assert(name === ":sid:1:my:name:goes:here:")
      }
      case _ => {
        fail("Regex did not capture the expected fields")
      }
    }
  }
}
