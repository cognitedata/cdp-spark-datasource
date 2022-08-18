package cognite.spark.performancebench

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.Row

class EventPerformance extends PerformanceSuite {
  import spark.implicits._
  private def readEvents() =
    read()
      .option("type", "events")
      .load()

  private val eventsTestSchema =
    Seq("type", "subtype", "externalId", "description", "startTime", "endTime")
  val externalIdPrefix = s"cdf-spark-perf-test"
  private val eventsTestData = (0 until 1000000)
    .map(
      id =>
        (
          "cdf-spark-perf-test",
          "test",
          s"$externalIdPrefix$id",
          s"This is a test row ($id)",
          java.sql.Timestamp.from(Instant.now().minus(Math.min(id.toLong, 100), ChronoUnit.HOURS)),
          java.sql.Timestamp.from(Instant.now().plus(Math.min(id.toLong, 100), ChronoUnit.HOURS))))
    .toVector

  private def writeEvents(): Unit =
    write(spark.sparkContext.parallelize(eventsTestData, 300).toDF(eventsTestSchema: _*))
      .option("type", "events")
      .option("onconflict", "upsert")
      .save()

  private def prepareForDelete(): Array[Row] =
    readEvents()
      .where(s"externalId LIKE '$externalIdPrefix%'")
      .select($"id")
      .collect()

  private def deleteEvents(rows: Array[Row]): Unit =
    write(spark.sparkContext.parallelize(rows.map(r => r.getAs[Long](0))).toDF("id"))
      .option("type", "events")
      .option("onconflict", "delete")
      .save()

  registerTest("writeEvents", writeEvents)
  registerTest("readEvents", () => {
    readEvents().collect()
    ()
  })
  registerTest("upsertEvents", writeEvents)
  registerTest("deleteEvents", prepareForDelete, deleteEvents)
}
