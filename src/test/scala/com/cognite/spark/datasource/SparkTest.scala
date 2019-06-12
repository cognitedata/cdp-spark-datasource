package com.cognite.spark.datasource

import cats.effect.IO
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.sql.SparkSession
import org.scalatest.Tag

import scala.concurrent.TimeoutException

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")
object GreenfieldTest extends Tag("GreenfieldTest")

trait SparkTest extends CdpConnector {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    // https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random * 1000).toLong.toString)
    .getOrCreate()

  def getThreeDModelIdAndRevisionId(auth: Auth): (String, String) = {
    val config = getDefaultConfig(auth)

    val modelUrl = uri"https://api.cognitedata.com/api/0.6/projects/${config.project}/3d/models"
    val models = getJson[Data[Items[ModelItem]]](config, modelUrl).unsafeRunSync()
    val modelId = models.data.items.head.id.toString

    val revisionsUrl =
      uri"https://api.cognitedata.com/api/0.6/projects/${config.project}/3d/models/$modelId/revisions"
    val revisions =
      getJson[Data[Items[ModelRevisionItem]]](config, revisionsUrl).unsafeRunSync()
    val revisionId = revisions.data.items.head.id.toString

    (modelId, revisionId)
  }

  def retryWhile[A](action: => A, shouldRetry: A => Boolean): A =
    retryWithBackoff(
      IO {
        val actionValue = action
        if (shouldRetry(actionValue)) {
          throw new TimeoutException("Retry")
        }
        actionValue
      },
      Constants.DefaultInitialRetryDelay,
      Constants.DefaultMaxRetries
    ).unsafeRunSync()

  def getDefaultConfig(auth: Auth): RelationConfig = {
    val project = getProject(auth, Constants.DefaultMaxRetries, Constants.DefaultBaseUrl)
    RelationConfig(
      auth,
      project,
      Some(Constants.DefaultBatchSize),
      None,
      Constants.DefaultPartitions,
      Constants.DefaultMaxRetries,
      false,
      "",
      Constants.DefaultBaseUrl,
      OnConflict.ABORT,
      spark.sparkContext.applicationId
    )
  }
}
