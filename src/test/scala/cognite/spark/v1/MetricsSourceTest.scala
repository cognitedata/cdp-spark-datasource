package org.apache.spark.datasource

import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class MetricsSourceTest extends FlatSpec with Matchers with ParallelTestExecution {

  behavior.of("A metric source")

  it should "detect attempt parameters from the name" in {
    val name = ":sid:1:sat:22:pid:333:tat:4444:my.name:goes.here"

    val parsed = AttemptTrackingMetricName.parse(name);

    assert(parsed.stageId === Some("1"))
    assert(parsed.stageAttempt === Some("22"))
    assert(parsed.partitionId === Some("333"))
    assert(parsed.taskAttempt === Some("4444"))
    assert(parsed.name === "my.name:goes.here")
  }

  it should "detect accept empty parameters from the name" in {
    val name = ":sid::sat::pid::tat::my.name:goes.here"

    val parsed = AttemptTrackingMetricName.parse(name);

    assert(parsed.stageId.isEmpty)
    assert(parsed.stageAttempt.isEmpty)
    assert(parsed.partitionId.isEmpty)
    assert(parsed.taskAttempt.isEmpty)
    assert(parsed.name === "my.name:goes.here")
  }

  it should "work with old name format" in {
    val name = "my.name:goes.here"

    val parsed = AttemptTrackingMetricName.parse(name);

    assert(parsed.stageId.isEmpty)
    assert(parsed.stageAttempt.isEmpty)
    assert(parsed.partitionId.isEmpty)
    assert(parsed.taskAttempt.isEmpty)
    assert(parsed.name === "my.name:goes.here")
  }

  it should "not get confused with names that resemble attempt tracking" in {
    val name = ":sid:1:my:name:goes:here:"

    val parsed = AttemptTrackingMetricName.parse(name);

    assert(parsed.stageId.isEmpty)
    assert(parsed.stageAttempt.isEmpty)
    assert(parsed.partitionId.isEmpty)
    assert(parsed.taskAttempt.isEmpty)
    assert(parsed.name === ":sid:1:my:name:goes:here:")
  }
}
